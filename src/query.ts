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
 */

import type { SegmentReader } from "./segment.js";
import type { Posting } from "./codec.js";

export interface QueryPosting {
  docId: number;
  /** Per-term tf values, keyed by term (for BM25 scoring layer). */
  tfs: Map<string, number>;
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
// Multi-segment iterator for one term (k-way merge)
// ---------------------------------------------------------------------------

/**
 * Merges per-segment SegmentPostingIters for a single term by docId order.
 * Uses a simple linear scan over active iterators (k is small for v0.1;
 * a binary heap can be added in v0.2 for large segment counts).
 */
export class MultiSegmentIter {
  readonly term: string;
  private readonly iters: SegmentPostingIter[];

  constructor(term: string, segments: SegmentReader[]) {
    this.term = term;
    this.iters = segments.map((seg) => new SegmentPostingIter(seg.postings(term)));
  }

  get isExhausted(): boolean {
    return this.iters.every((it) => it.isExhausted);
  }

  /** The smallest current docId across all active iterators, or null if exhausted. */
  get currentDocId(): number | null {
    let min: number | null = null;
    for (const it of this.iters) {
      if (!it.isExhausted && it.docId !== null) {
        if (min === null || it.docId < min) min = it.docId;
      }
    }
    return min;
  }

  /**
   * Collect the tf for the current minimum docId (summing across segments that
   * share the same docId, though in practice each docId lives in one segment).
   * Advances all iterators that were at that docId.
   */
  next(): { docId: number; tf: number } | null {
    const docId = this.currentDocId;
    if (docId === null) return null;
    let tf = 0;
    for (const it of this.iters) {
      if (!it.isExhausted && it.docId === docId) {
        tf += it.tf;
        it.advance();
      }
    }
    return { docId, tf };
  }

  /** Advance all iterators until their current docId >= target. */
  seek(target: number): void {
    for (const it of this.iters) {
      it.seek(target);
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

  const iters = terms.map((t) => new MultiSegmentIter(t, segments));

  while (true) {
    // Check exhaustion.
    if (iters.some((it) => it.isExhausted)) break;

    // Find the maximum current docId.
    let maxDocId = -1;
    for (const it of iters) {
      const id = it.currentDocId;
      if (id === null) { return; }
      if (id > maxDocId) maxDocId = id;
    }

    // Seek all iterators to maxDocId.
    for (const it of iters) it.seek(maxDocId);

    // If any iterator is now exhausted or past maxDocId, loop.
    if (iters.some((it) => it.isExhausted)) break;

    // Check if all iterators are exactly at maxDocId.
    const allMatch = iters.every((it) => it.currentDocId === maxDocId);
    if (allMatch) {
      const tfs = new Map<string, number>();
      for (let i = 0; i < iters.length; i++) {
        const result = iters[i].next();
        if (result) tfs.set(terms[i], result.tf);
      }
      yield { docId: maxDocId, tfs };
    }
    // If not all match, the seek in the next iteration will advance the laggards.
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

  const iters = terms.map((t) => new MultiSegmentIter(t, segments));

  while (true) {
    // Find the minimum current docId across all active iterators.
    let minDocId: number | null = null;
    for (const it of iters) {
      if (!it.isExhausted) {
        const id = it.currentDocId;
        if (id !== null && (minDocId === null || id < minDocId)) minDocId = id;
      }
    }
    if (minDocId === null) break; // all exhausted

    // Collect tfs from all iterators that are at minDocId.
    const tfs = new Map<string, number>();
    for (let i = 0; i < iters.length; i++) {
      const it = iters[i];
      if (!it.isExhausted && it.currentDocId === minDocId) {
        const result = it.next();
        if (result) tfs.set(terms[i], result.tf);
      }
    }
    yield { docId: minDocId, tfs };
  }
}
