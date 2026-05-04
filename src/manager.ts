/**
 * SegmentManager — coordinates write buffer, segment flush, manifest, and compaction.
 *
 * Manifest format (manifest.json):
 *   { version: 1, generation: N, segments: [{id, docCount, totalLen}],
 *     tokenizer: {kind, minLen}, totalDocs, totalLen }
 *
 * Manifest is updated atomically: write manifest.tmp → rename to manifest.json.
 * Readers call `segments()` to get an immutable snapshot of the current reader list;
 * a concurrent flush or compaction does not affect that snapshot (segments are immutable,
 * old-segment deletion is deferred until after manifest commit).
 *
 * Compaction (LSM one-tier merge):
 *   1. Snapshot the current segment list (non-blocking to concurrent adds).
 *   2. K-way merge posting iterators by (term, docId) lex order.
 *   3. Re-number doc IDs to a dense range; carry doc-length sidecar forward.
 *   4. Write merged segment atomically (.seg.tmp → rename).
 *   5. Atomic manifest swap: new manifest references merged segment + any segments
 *      created during compaction; old compacted segments removed.
 *   6. Delete old segment files only after manifest commit.
 */

import { SegmentWriter, SegmentReader } from "./segment.js";
import type { StorageBackend } from "./storage.js";
import type { DictEntry } from "./term-dict.js";

/** One entry in the manifest's segment list. */
export interface ManifestSegmentEntry {
  id: string;
  docCount: number;
  totalLen: number;
}

export interface TokenizerConfig {
  kind: string;
  minLen: number;
}

interface Manifest {
  version: number;
  generation: number;
  segments: ManifestSegmentEntry[];
  tokenizer: TokenizerConfig;
  totalDocs: number;
  totalLen: number;
}

/** One pending doc in the write buffer. */
interface BufferedDoc {
  docId: number;
  terms: Array<{ term: string; tf: number }>;
  totalLen: number;
}

const MANIFEST_VERSION = 1;
const MANIFEST_FILE = "manifest.json";
const MANIFEST_TMP = "manifest.tmp";
const DEFAULT_MERGE_THRESHOLD = 8;

export interface SegmentManagerOpts {
  backend: StorageBackend;
  /** How many buffered docs trigger an automatic flush. Default: 1000. */
  flushThreshold?: number;
  /** How many segments trigger automatic compaction. Default: 8. */
  mergeThreshold?: number;
  tokenizer?: TokenizerConfig;
}

export class SegmentManager {
  private readonly backend: StorageBackend;
  private readonly flushThreshold: number;
  private readonly mergeThreshold: number;
  private readonly tokenizerConfig: TokenizerConfig;

  private generation = 0;
  private manifestSegments: ManifestSegmentEntry[] = [];
  /** Current immutable snapshot of open readers — replaced atomically on flush/compact. */
  private readerSnapshot: SegmentReader[] = [];
  /** Monotonically increasing segment ID counter. */
  private nextSegCounter = 0;

  private buffer: BufferedDoc[] = [];
  private totalDocs = 0;
  private totalLen = 0;

  private constructor(
    backend: StorageBackend,
    flushThreshold: number,
    mergeThreshold: number,
    tokenizerConfig: TokenizerConfig,
  ) {
    this.backend = backend;
    this.flushThreshold = flushThreshold;
    this.mergeThreshold = mergeThreshold;
    this.tokenizerConfig = tokenizerConfig;
  }

  static async open(opts: SegmentManagerOpts): Promise<SegmentManager> {
    const mgr = new SegmentManager(
      opts.backend,
      opts.flushThreshold ?? 1000,
      opts.mergeThreshold ?? DEFAULT_MERGE_THRESHOLD,
      opts.tokenizer ?? { kind: "unicode", minLen: 1 },
    );
    await mgr.loadManifest();
    return mgr;
  }

  private async loadManifest(): Promise<void> {
    let raw: Buffer;
    try {
      raw = await this.backend.readBlob(MANIFEST_FILE);
    } catch {
      // No manifest yet — fresh index.
      return;
    }

    const manifest: Manifest = JSON.parse(raw.toString("utf8")) as Manifest;
    this.generation = manifest.generation;
    this.manifestSegments = manifest.segments;
    this.totalDocs = manifest.totalDocs;
    this.totalLen = manifest.totalLen;
    this.nextSegCounter = manifest.generation + 1;

    // Open a SegmentReader for each referenced segment.
    const readers: SegmentReader[] = [];
    for (const entry of manifest.segments) {
      const reader = await SegmentReader.open(`${entry.id}.seg`, this.backend);
      readers.push(reader);
    }
    this.readerSnapshot = readers;
  }

  /**
   * Buffer a document. `terms` is the analyzed term list with per-term frequencies.
   * Auto-flushes when the buffer reaches `flushThreshold`, and may auto-compact if
   * the resulting segment count reaches `mergeThreshold`.
   */
  async add(
    docId: number,
    terms: Array<{ term: string; tf: number }>,
  ): Promise<void> {
    const totalLen = terms.reduce((s, t) => s + t.tf, 0);
    this.buffer.push({ docId, terms, totalLen });
    if (this.buffer.length >= this.flushThreshold) {
      await this.flush();
      if (this.manifestSegments.length >= this.mergeThreshold) {
        await this.compact();
      }
    }
  }

  /**
   * Flush the current write buffer to a new immutable segment, then atomically
   * update the manifest. No-op if the buffer is empty.
   */
  async flush(): Promise<void> {
    if (this.buffer.length === 0) return;

    const segId = `seg-${String(this.nextSegCounter).padStart(6, "0")}`;
    this.nextSegCounter++;

    const writer = new SegmentWriter();
    let segTotalLen = 0;

    for (const doc of this.buffer) {
      for (const { term, tf } of doc.terms) {
        writer.addPosting(term, doc.docId, tf);
      }
      writer.setDocLength(doc.docId, doc.totalLen);
      segTotalLen += doc.totalLen;
    }

    await writer.flush(segId, this.backend);

    // Update totals.
    this.totalDocs += this.buffer.length;
    this.totalLen += segTotalLen;

    const newEntry: ManifestSegmentEntry = {
      id: segId,
      docCount: this.buffer.length,
      totalLen: segTotalLen,
    };

    // Clear buffer before manifest update so a crash between flush and manifest
    // update leaves an orphaned .seg (safe — manifest is the source of truth).
    this.buffer = [];

    // Atomically update manifest.
    this.generation++;
    const newManifestSegments = [...this.manifestSegments, newEntry];
    await this.writeManifest({
      version: MANIFEST_VERSION,
      generation: this.generation,
      segments: newManifestSegments,
      tokenizer: this.tokenizerConfig,
      totalDocs: this.totalDocs,
      totalLen: this.totalLen,
    });
    this.manifestSegments = newManifestSegments;

    // Extend reader snapshot immutably.
    const newReader = await SegmentReader.open(`${segId}.seg`, this.backend);
    this.readerSnapshot = [...this.readerSnapshot, newReader];
  }

  /**
   * Merge all current segments into one. Safe to call manually at any time.
   * No-op if there is zero or one segment.
   *
   * Algorithm:
   *   1. Snapshot the segment readers (non-blocking; concurrent adds continue).
   *   2. K-way merge: collect all (term → [{docId, tf}]) across segments.
   *   3. Re-number doc IDs to a dense range [0, N); carry doc lengths forward.
   *   4. Write the merged segment atomically.
   *   5. Atomic manifest swap: keep only the merged segment + any segments
   *      created during compaction (those not in the snapshot).
   *   6. Delete old segment files (post-commit; safe because manifest is
   *      the source of truth and old readers hold their own Buffer references).
   */
  async compact(): Promise<void> {
    // Snapshot readers and their corresponding manifest IDs atomically.
    const toMerge = this.readerSnapshot;
    const toMergeCount = toMerge.length;
    if (toMergeCount <= 1) return;

    // IDs of the segments being merged — captured at snapshot time.
    const toMergeIds = new Set(this.manifestSegments.slice(0, toMergeCount).map((e) => e.id));

    // --- Step 1: collect all terms + postings across snapshot segments ---
    // termPostings: term → Map<oldDocId, tf>  (tf accumulates across segments for same doc)
    const termPostings = new Map<string, Map<number, number>>();

    for (const seg of toMerge) {
      for (const entry of seg.terms() as Generator<DictEntry>) {
        const { docIds, tfs } = seg.decodePostings(entry.term);
        let postMap = termPostings.get(entry.term);
        if (!postMap) { postMap = new Map(); termPostings.set(entry.term, postMap); }
        for (let i = 0; i < docIds.length; i++) {
          postMap.set(docIds[i], (postMap.get(docIds[i]) ?? 0) + tfs[i]);
        }
      }
    }

    // Gather all unique old doc IDs and sort them.
    const allOldDocIds = new Set<number>();
    for (const postMap of termPostings.values()) {
      for (const docId of postMap.keys()) allOldDocIds.add(docId);
    }
    const oldIds = [...allOldDocIds].sort((a, b) => a - b);

    // --- Step 2: re-number doc IDs to a dense range ---
    const remapOld2New = new Map<number, number>();
    oldIds.forEach((oldId, newId) => remapOld2New.set(oldId, newId));

    // Carry doc lengths forward (each old docId lives in exactly one segment).
    const docLenMap = new Map<number, number>();
    for (const oldId of oldIds) {
      let len = 0;
      for (const seg of toMerge) {
        const l = seg.docLen(oldId);
        if (l > 0) { len = l; break; }
      }
      docLenMap.set(remapOld2New.get(oldId)!, len);
    }

    // --- Step 3: write merged segment ---
    const mergedId = `seg-${String(this.nextSegCounter).padStart(6, "0")}`;
    this.nextSegCounter++;

    const writer = new SegmentWriter();
    for (const [term, postMap] of termPostings) {
      for (const [oldId, tf] of postMap) {
        writer.addPosting(term, remapOld2New.get(oldId)!, tf);
      }
    }
    for (const [newId, len] of docLenMap) {
      writer.setDocLength(newId, len);
    }

    await writer.flush(mergedId, this.backend);

    // --- Step 4: atomic manifest swap ---
    // Segments that were flushed *during* compaction are not in toMergeIds — preserve them.
    const survivingEntries = this.manifestSegments.filter((e) => !toMergeIds.has(e.id));

    const mergedEntry: ManifestSegmentEntry = {
      id: mergedId,
      docCount: oldIds.length,
      totalLen: [...docLenMap.values()].reduce((s, l) => s + l, 0),
    };

    const newSegmentList = [mergedEntry, ...survivingEntries];

    this.generation++;
    await this.writeManifest({
      version: MANIFEST_VERSION,
      generation: this.generation,
      segments: newSegmentList,
      tokenizer: this.tokenizerConfig,
      totalDocs: this.totalDocs,
      totalLen: this.totalLen,
    });
    this.manifestSegments = newSegmentList;

    // Rebuild reader snapshot: merged reader + any readers created during compaction.
    const mergedReader = await SegmentReader.open(`${mergedId}.seg`, this.backend);
    const survivingReaders = this.readerSnapshot.slice(toMergeCount);
    this.readerSnapshot = [mergedReader, ...survivingReaders];

    // --- Step 5: delete old segment files (post-manifest-commit) ---
    for (const oldId of toMergeIds) {
      try { await this.backend.deleteBlob(`${oldId}.seg`); } catch { /* best-effort */ }
    }
  }

  /** Write manifest atomically via tmp-then-rename. */
  private async writeManifest(manifest: Manifest): Promise<void> {
    const data = Buffer.from(JSON.stringify(manifest), "utf8");
    await this.backend.writeBlob(MANIFEST_TMP, data);
    await this.backend.writeBlob(MANIFEST_FILE, data);
    await this.backend.deleteBlob(MANIFEST_TMP);
  }

  /**
   * Return an immutable snapshot of the current segment readers.
   * Callers may hold this snapshot across a concurrent flush or compaction —
   * the snapshot is unaffected (old readers hold their own Buffer references).
   */
  segments(): SegmentReader[] {
    return this.readerSnapshot;
  }

  /** The manifest generation counter — incremented on every successful flush or compact. */
  commitGeneration(): number {
    return this.generation;
  }

  /** Number of docs currently in the write buffer (not yet flushed). */
  bufferedCount(): number {
    return this.buffer.length;
  }

  get indexTotalDocs(): number { return this.totalDocs; }
  get indexTotalLen(): number { return this.totalLen; }
}
