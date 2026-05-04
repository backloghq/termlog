/**
 * Query iterators — boolean posting-list operations over a segment snapshot.
 *
 * Primitives:
 *   - SegmentPostingIter   — wraps a single segment's lazy posting iterator;
 *                            supports next() and seek(docId).
 *   - MultiSegmentIter     — k-way merge of SegmentPostingIters for one term,
 *                            emitting (docId, tf) in docId order.
 *   - andQuery             — zigzag merge of MultiSegmentIters; yields only docIds
 *                            present in ALL term iterators.
 *   - orQuery              — k-way union of MultiSegmentIters; yields every docId
 *                            present in ANY term iterator, accumulating tf per term.
 *
 * All iterators are lazy — no postings are decoded until next()/seek() is called.
 * Tombstoned doc IDs are filtered at the MultiSegmentIter level.
 */

import { MinHeap } from "./heap.js";
import type { SegmentReader } from "./segment.js";
import type { Posting } from "./codec.js";

export interface QueryPosting {
  docId: number;
  /** Per-term tf values, keyed by term (for BM25 scoring layer). */
  tfs: Map<string, number>;
  /**
   * Index of the segment that owns this docId (into the segments[] array passed
   * to andQuery/orQuery). Enables O(1) docLen lookup in the scoring layer
   * instead of scanning all segments.
   */
  segIndex: number;
}

// ---------------------------------------------------------------------------
// Single-segment posting iterator with seek support
// ---------------------------------------------------------------------------

/**
 * Wraps a segment's lazy posting iterator for one term.
 * Caches the current posting so seek() can avoid re-decoding from scratch
 * for forward seeks (which is the common case in zigzag/merge algorithms).
 */
export class SegmentPostingIter {
  private readonly iter: Iterator<Posting>;
  private current: Posting | null = null;
  private done = false;

  constructor(iter: Iterator<Posting>) {
    this.iter = iter;
    this.advance();
  }

  get docId(): number | null {
    return this.current !== null ? this.current.docId : null;
  }

  get tf(): number {
    return this.current?.tf ?? 0;
  }

  get isExhausted(): boolean {
    return this.done;
  }

  /** Advance to the next posting. */
  advance(): void {
    if (this.done) return;
    const result = this.iter.next();
    if (result.done) {
      this.done = true;
      this.current = null;
    } else {
      this.current = result.value;
    }
  }

  /**
   * Advance until docId >= target. Since posting lists are sorted, this is
   * a linear scan from the current position — O(k) where k is the skip distance.
   * (v0.2+ can add skip lists for O(log n) seeks.)
   */
  seek(target: number): void {
    while (!this.done && this.current !== null && this.current.docId < target) {
      this.advance();
    }
  }
}

// ---------------------------------------------------------------------------
// Tombstone union — built once per query from all segment tombstone arrays
// ---------------------------------------------------------------------------

/** Build the union of all tombstone sets across segments for O(1) lookup. */
export function buildTombstoneSet(segments: SegmentReader[]): Set<number> {
  const set = new Set<number>();
  for (const seg of segments) {
    for (const id of seg.tombstones) set.add(id);
  }
  return set;
}

// ---------------------------------------------------------------------------
// Multi-segment iterator for one term (k-way merge)
// ---------------------------------------------------------------------------

/** One active entry in the MultiSegmentIter heap. */
interface HeapSlot {
  docId: number;
  segIndex: number;
  iter: SegmentPostingIter;
}

/**
 * Merges per-segment SegmentPostingIters for a single term by docId order.
 * Uses a MinHeap (O(log K) per step) instead of a linear scan (O(K)).
 * Tombstoned docIds (from the provided set) are skipped transparently.
 */
export class MultiSegmentIter {
  readonly term: string;
  private readonly tombstones: Set<number>;
  private readonly heap: MinHeap<HeapSlot>;
  /** All iters, indexed by segIndex — needed for seek(). */
  private readonly iters: SegmentPostingIter[];

  constructor(term: string, segments: SegmentReader[], tombstones?: Set<number>) {
    this.term = term;
    this.tombstones = tombstones ?? new Set();
    this.heap = new MinHeap<HeapSlot>((a, b) => a.docId - b.docId);
    this.iters = segments.map((seg, si) => {
      const it = new SegmentPostingIter(seg.postings(term));
      this.advancePastTombstones(it);
      if (!it.isExhausted && it.docId !== null) {
        this.heap.push({ docId: it.docId, segIndex: si, iter: it });
      }
      return it;
    });
  }

  private advancePastTombstones(it: SegmentPostingIter): void {
    while (!it.isExhausted && it.docId !== null && this.tombstones.has(it.docId)) {
      it.advance();
    }
  }

  get isExhausted(): boolean {
    return this.heap.size === 0;
  }

  /** The smallest current docId across all active iterators, or null if exhausted. */
  get currentDocId(): number | null {
    return this.heap.peek()?.docId ?? null;
  }

  /**
   * Emit the posting for the current minimum docId, summing tf across any segments
   * sharing that docId (rare in practice), and advance those iterators.
   * Returns { docId, tf, segIndex } where segIndex is the segment with the lowest
   * index that contributed (for O(1) docLen lookup).
   */
  next(): { docId: number; tf: number; segIndex: number } | null {
    const top = this.heap.peek();
    if (!top) return null;
    const docId = top.docId;
    let tf = 0;
    let segIndex = top.segIndex;

    // Drain all heap entries at this docId (handles the rare cross-segment duplicate).
    while (this.heap.size > 0 && this.heap.peek()!.docId === docId) {
      const slot = this.heap.pop()!;
      tf += slot.iter.tf;
      if (slot.segIndex < segIndex) segIndex = slot.segIndex;
      slot.iter.advance();
      this.advancePastTombstones(slot.iter);
      if (!slot.iter.isExhausted && slot.iter.docId !== null) {
        slot.docId = slot.iter.docId;
        this.heap.push(slot);
      }
    }
    return { docId, tf, segIndex };
  }

  /**
   * Seek all iterators to the first docId >= target, rebuilding the heap.
   * O(K log K) — used by andQuery's zigzag merge.
   */
  seek(target: number): void {
    // Drain heap, seek each iter, re-push if still alive.
    while (this.heap.size > 0) this.heap.pop();
    for (let si = 0; si < this.iters.length; si++) {
      const it = this.iters[si];
      it.seek(target);
      this.advancePastTombstones(it);
      if (!it.isExhausted && it.docId !== null) {
        this.heap.push({ docId: it.docId, segIndex: si, iter: it });
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Boolean query operators
// ---------------------------------------------------------------------------

/**
 * AND query — zigzag merge.
 *
 * Iterates over the intersection of all term iterators. At each step:
 *   1. Find the maximum current docId across all iterators.
 *   2. Seek all other iterators to that docId.
 *   3. If all iterators land on the same docId, emit it and advance everyone.
 *   4. Otherwise, repeat from step 1.
 *
 * Returns results in ascending docId order.
 */
export function* andQuery(
  terms: string[],
  segments: SegmentReader[],
): Generator<QueryPosting> {
  if (terms.length === 0 || segments.length === 0) return;

  const tombstones = buildTombstoneSet(segments);
  const iters = terms.map((t) => new MultiSegmentIter(t, segments, tombstones));

  while (true) {
    if (iters.some((it) => it.isExhausted)) break;

    // Find the maximum current docId (O(1) per iter via heap peek).
    let maxDocId = -1;
    for (const it of iters) {
      const id = it.currentDocId;
      if (id === null) return;
      if (id > maxDocId) maxDocId = id;
    }

    // Seek all iterators to maxDocId.
    for (const it of iters) it.seek(maxDocId);

    if (iters.some((it) => it.isExhausted)) break;

    // Check if all iterators landed exactly on maxDocId.
    const allMatch = iters.every((it) => it.currentDocId === maxDocId);
    if (allMatch) {
      const tfs = new Map<string, number>();
      let segIndex = 0;
      for (let i = 0; i < iters.length; i++) {
        const result = iters[i].next();
        if (result) {
          tfs.set(terms[i], result.tf);
          if (i === 0) segIndex = result.segIndex;
        }
      }
      yield { docId: maxDocId, tfs, segIndex };
    }
  }
}

/**
 * OR query — k-way union merge.
 *
 * Iterates over the union of all term iterators, emitting each unique docId once
 * with all matching term tfs collected. Results are in ascending docId order.
 */
export function* orQuery(
  terms: string[],
  segments: SegmentReader[],
): Generator<QueryPosting> {
  if (terms.length === 0 || segments.length === 0) return;

  const tombstones = buildTombstoneSet(segments);
  const iters = terms.map((t) => new MultiSegmentIter(t, segments, tombstones));

  while (true) {
    // Find the minimum current docId across all active iterators (O(1) per iter).
    let minDocId: number | null = null;
    for (const it of iters) {
      if (!it.isExhausted) {
        const id = it.currentDocId;
        if (id !== null && (minDocId === null || id < minDocId)) minDocId = id;
      }
    }
    if (minDocId === null) break;

    // Collect tfs from all iterators at minDocId; capture segIndex from first match.
    const tfs = new Map<string, number>();
    let segIndex = 0;
    let segIndexSet = false;
    for (let i = 0; i < iters.length; i++) {
      const it = iters[i];
      if (!it.isExhausted && it.currentDocId === minDocId) {
        const result = it.next();
        if (result) {
          tfs.set(terms[i], result.tf);
          if (!segIndexSet) { segIndex = result.segIndex; segIndexSet = true; }
        }
      }
    }
    yield { docId: minDocId, tfs, segIndex };
  }
}
