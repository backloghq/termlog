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

import { open as fsOpen, readFile as fsReadFile, unlink as fsUnlink } from "node:fs/promises";
import { join as pathJoin } from "node:path";
import { SegmentWriter, SegmentReader } from "./segment.js";
import { MinHeap } from "./heap.js";
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
const LOCK_FILE = ".lock";
const DEFAULT_MERGE_THRESHOLD = 8;

/** Thrown when manifest.json exists but cannot be parsed (scenario 5). */
export class ManifestCorruptionError extends Error {
  constructor(detail: string) {
    super(`Manifest corruption: ${detail}`);
    this.name = "ManifestCorruptionError";
  }
}

/** Thrown when another process holds the write lock on this index directory. */
export class IndexLockedError extends Error {
  constructor(public readonly pid: number) {
    super(`Index is locked by process ${pid}. If stale, delete the .lock file.`);
    this.name = "IndexLockedError";
  }
}

export interface SegmentManagerOpts {
  backend: StorageBackend;
  /** Directory on disk — required to enable the advisory .lock file (local FS only). */
  dir?: string;
  /** How many buffered docs trigger an automatic flush. Default: 1000. */
  flushThreshold?: number;
  /** How many segments trigger automatic compaction. Default: 8. */
  mergeThreshold?: number;
  tokenizer?: TokenizerConfig;
  /**
   * Called after the segment file is written but BEFORE the manifest is committed.
   * Use this to persist any side-data (e.g. docIds mapping) that must be consistent
   * with the manifest. A crash after this callback but before manifest commit leaves
   * the side-data ahead of the manifest — safe, because the mapping only grows.
   */
  onBeforeManifest?: () => Promise<void>;
}

export class SegmentManager {
  private readonly backend: StorageBackend;
  private readonly flushThreshold: number;
  private readonly mergeThreshold: number;
  private readonly tokenizerConfig: TokenizerConfig;
  private readonly onBeforeManifest: (() => Promise<void>) | undefined;
  /** Absolute path to the .lock file, or null for non-local-FS backends. */
  private lockPath: string | null = null;

  private generation = 0;
  private manifestSegments: ManifestSegmentEntry[] = [];
  /** Current immutable snapshot of open readers — replaced atomically on flush/compact. */
  private readerSnapshot: SegmentReader[] = [];
  /** Monotonically increasing segment ID counter. */
  private nextSegCounter = 0;

  private buffer: BufferedDoc[] = [];
  private totalDocs = 0;
  private totalLen = 0;
  /** True when an existing manifest.json was loaded (not a fresh index). */
  private _manifestLoaded = false;
  /** Tokenizer kind recorded in the manifest (null for fresh indexes). */
  private _persistedTokenizerKind: string | null = null;
  /** Tokenizer minLen recorded in the manifest (null for fresh indexes). */
  private _persistedTokenizerMinLen: number | null = null;
  /** Tombstone docIds accumulated since last flush — written into the next segment. */
  private pendingTombstones = new Set<number>();

  /** Serialize all state-mutating operations through a promise chain. Reads are lock-free. */
  private _lock: Promise<void> = Promise.resolve();
  private serialize<R>(fn: () => Promise<R>): Promise<R> {
    const prev = this._lock;
    let resolve!: () => void;
    this._lock = new Promise<void>((r) => { resolve = r; });
    return prev.then(fn).finally(() => resolve());
  }

  private constructor(
    backend: StorageBackend,
    flushThreshold: number,
    mergeThreshold: number,
    tokenizerConfig: TokenizerConfig,
    onBeforeManifest?: () => Promise<void>,
  ) {
    this.backend = backend;
    this.flushThreshold = flushThreshold;
    this.mergeThreshold = mergeThreshold;
    this.tokenizerConfig = tokenizerConfig;
    this.onBeforeManifest = onBeforeManifest;
  }

  static async open(opts: SegmentManagerOpts): Promise<SegmentManager> {
    const mgr = new SegmentManager(
      opts.backend,
      opts.flushThreshold ?? 1000,
      opts.mergeThreshold ?? DEFAULT_MERGE_THRESHOLD,
      opts.tokenizer ?? { kind: "unicode", minLen: 1 },
      opts.onBeforeManifest,
    );
    // Acquire advisory lock for local FS backends to prevent multi-process corruption.
    if (opts.dir && opts.backend.isLocalFs?.()) {
      mgr.lockPath = pathJoin(opts.dir, LOCK_FILE);
      await mgr.acquireLock();
    }
    await mgr.loadManifest();
    await mgr.recoverOrphans();
    return mgr;
  }

  /** Release the advisory lock and flush any pending buffered writes. */
  async close(): Promise<void> {
    await this.flush();
    await this.releaseLock();
  }

  private async acquireLock(): Promise<void> {
    const lp = this.lockPath!;
    // Try exclusive create (O_EXCL) — fails if lock file already exists.
    try {
      const fh = await fsOpen(lp, "wx");
      await fh.writeFile(String(process.pid), "utf-8");
      await fh.close();
      return;
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
    }
    // Lock file exists — check if the holder is still alive.
    let content: string;
    try {
      content = await fsReadFile(lp, "utf-8");
    } catch {
      // File disappeared (race) — retry.
      return this.acquireLock();
    }
    const pid = parseInt(content, 10);
    if (!isNaN(pid)) {
      try { process.kill(pid, 0); } catch (e) {
        if ((e as NodeJS.ErrnoException).code === "ESRCH") {
          // Stale lock — remove and retry.
          try { await fsUnlink(lp); } catch { /* already gone */ }
          return this.acquireLock();
        }
      }
      throw new IndexLockedError(pid);
    }
    // Unreadable/corrupt lock file — treat as stale.
    try { await fsUnlink(lp); } catch { /* already gone */ }
    return this.acquireLock();
  }

  private async releaseLock(): Promise<void> {
    if (!this.lockPath) return;
    try { await fsUnlink(this.lockPath); } catch { /* best-effort */ }
    this.lockPath = null;
  }

  private async loadManifest(): Promise<void> {
    // Scenario 3: stale *.tmp from interrupted FsBackend rename — delete any
    // manifest-related temp files. manifest.json is either absent or fully
    // written (FsBackend.writeBlob is atomic: writes to <path>.tmp then renames).
    try { await this.backend.deleteBlob(MANIFEST_TMP); } catch { /* not present */ }
    try { await this.backend.deleteBlob(`${MANIFEST_FILE}.tmp`); } catch { /* not present */ }

    let raw: Buffer;
    try {
      raw = await this.backend.readBlob(MANIFEST_FILE);
    } catch (err) {
      // ENOENT = fresh index (no manifest yet). Any other error (EACCES, EIO, S3 5xx)
      // must surface — silently treating it as a fresh index would wipe data.
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
      throw err;
    }

    // Scenario 5: manifest.json exists but is corrupt JSON.
    let manifest: Manifest;
    try {
      manifest = JSON.parse(raw.toString("utf8")) as Manifest;
    } catch (err) {
      throw new ManifestCorruptionError(String(err));
    }

    this._manifestLoaded = true;
    this._persistedTokenizerKind = manifest.tokenizer.kind;
    this._persistedTokenizerMinLen = manifest.tokenizer.minLen;
    this.generation = manifest.generation;
    this.manifestSegments = manifest.segments;
    this.totalDocs = manifest.totalDocs;
    this.totalLen = manifest.totalLen;
    this.nextSegCounter = manifest.generation + 1;

    // Open a SegmentReader for each referenced segment.
    // Scenario 4: if any segment is corrupt, SegmentReader.open throws
    // SegmentCorruptionError — propagate to caller.
    const readers: SegmentReader[] = [];
    for (const entry of manifest.segments) {
      const reader = await SegmentReader.open(`${entry.id}.seg`, this.backend);
      readers.push(reader);
    }
    this.readerSnapshot = readers;
  }

  /**
   * Scenarios 1 + 2: delete orphaned temp files and unreferenced segment files.
   * Called after loadManifest so we know which segment IDs are referenced.
   */
  private async recoverOrphans(): Promise<void> {
    const referencedIds = new Set(this.manifestSegments.map((e) => e.id));

    // Use prefix-scoped list calls so S3 backends don't pay full-bucket-list cost.
    // Segments are always named "seg-*.seg" or "seg-*.seg.*.tmp".
    let segBlobs: string[] = [];
    try { segBlobs = await this.backend.listBlobs("seg-"); } catch { /* skip */ }

    // Manifest temp files.
    let manifestBlobs: string[] = [];
    try { manifestBlobs = await this.backend.listBlobs("manifest"); } catch { /* skip */ }

    for (const blob of [...segBlobs, ...manifestBlobs]) {
      if (blob.endsWith(".tmp")) {
        try { await this.backend.deleteBlob(blob); } catch { /* best-effort */ }
        continue;
      }
      if (blob.endsWith(".seg")) {
        const id = blob.slice(0, -4); // strip ".seg"
        if (!referencedIds.has(id)) {
          try { await this.backend.deleteBlob(blob); } catch { /* best-effort */ }
        }
      }
    }
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
    return this.serialize(async () => {
      const totalLen = terms.reduce((s, t) => s + t.tf, 0);
      this.buffer.push({ docId, terms, totalLen });
      if (this.buffer.length >= this.flushThreshold) {
        await this.flushLocked();
        if (this.manifestSegments.length >= this.mergeThreshold) {
          await this.compactLocked();
        }
      }
    });
  }

  /**
   * Mark a document as removed. If it's still in the write buffer, drops it immediately.
   * Otherwise, queues a tombstone that will be written into the next segment on flush.
   * Idempotent — removing a doc not in the index is a no-op.
   */
  async remove(docId: number): Promise<void> {
    return this.serialize(() => {
      // Drop from write buffer if not yet flushed.
      const before = this.buffer.length;
      this.buffer = this.buffer.filter((d) => d.docId !== docId);
      if (this.buffer.length < before) return Promise.resolve();
      // Otherwise record a tombstone for flushed segments.
      this.pendingTombstones.add(docId);
      return Promise.resolve();
    });
  }

  /**
   * Flush the current write buffer to a new immutable segment, then atomically
   * update the manifest. No-op if the buffer is empty AND there are no pending tombstones.
   */
  async flush(): Promise<void> {
    return this.serialize(() => this.flushLocked());
  }

  private async flushLocked(): Promise<void> {
    if (this.buffer.length === 0 && this.pendingTombstones.size === 0) return;

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

    if (this.pendingTombstones.size > 0) {
      writer.setTombstones([...this.pendingTombstones]);
      this.pendingTombstones = new Set();
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

    // Persist any side-data (e.g. docId mapping) before the manifest commit so
    // the two are always consistent: if the process crashes here, the mapping is
    // ahead of the manifest (harmless — extra entries); never behind it.
    if (this.onBeforeManifest) await this.onBeforeManifest();

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
    return this.serialize(() => this.compactLocked());
  }

  private async compactLocked(): Promise<void> {
    // Snapshot readers and their corresponding manifest IDs atomically.
    const toMerge = this.readerSnapshot;
    const toMergeCount = toMerge.length;
    if (toMergeCount <= 1) return;

    // IDs of the segments being merged — captured at snapshot time.
    const toMergeIds = new Set(this.manifestSegments.slice(0, toMergeCount).map((e) => e.id));

    // Build the union tombstone set across all merged segments.
    const tombstoneUnion = new Set<number>();
    for (const seg of toMerge) {
      for (const id of seg.tombstones) tombstoneUnion.add(id);
    }

    // --- Step 1: build docId renumber map (O(unique surviving docs)) ---
    // First pass: collect surviving docIds from all posting lists (skip tombstoned).
    const survivingOldDocIds = new Set<number>();
    for (const seg of toMerge) {
      for (const entry of seg.terms()) {
        const { docIds } = seg.decodePostings(entry.term);
        for (const id of docIds) {
          if (!tombstoneUnion.has(id)) survivingOldDocIds.add(id);
        }
      }
    }
    const oldIds = [...survivingOldDocIds].sort((a, b) => a - b);

    // Re-number to dense range.
    const remapOld2New = new Map<number, number>();
    oldIds.forEach((oldId, newId) => remapOld2New.set(oldId, newId));

    // Carry doc lengths forward.
    const docLenMap = new Map<number, number>();
    for (const oldId of oldIds) {
      for (const seg of toMerge) {
        const l = seg.docLen(oldId);
        if (l > 0) { docLenMap.set(remapOld2New.get(oldId)!, l); break; }
      }
    }

    // --- Step 2: streaming k-way merge of term iterators into SegmentWriter ---
    // Uses a min-heap keyed by (term, segIndex) — O(K) heap entries at any time,
    // O(largest single posting list) for the per-term accumulator.
    const mergedId = `seg-${String(this.nextSegCounter).padStart(6, "0")}`;
    this.nextSegCounter++;

    const writer = new SegmentWriter();

    interface HeapEntry {
      segIndex: number;
      termIter: Generator<DictEntry, void, unknown>;
      currentTerm: string;
    }

    const heap = new MinHeap<HeapEntry>((a, b) => {
      if (a.currentTerm < b.currentTerm) return -1;
      if (a.currentTerm > b.currentTerm) return 1;
      return a.segIndex - b.segIndex;
    });

    for (let si = 0; si < toMerge.length; si++) {
      const termIter = toMerge[si].terms();
      const first = termIter.next();
      if (!first.done) {
        heap.push({ segIndex: si, termIter, currentTerm: first.value.term });
      }
    }

    let prevTerm: string | null = null;
    // Accumulator for the current term's postings: Map<newDocId, tf>
    let termAccum = new Map<number, number>();

    const flushTerm = () => {
      if (prevTerm === null || termAccum.size === 0) return;
      for (const [newId, tf] of termAccum) {
        writer.addPosting(prevTerm, newId, tf);
      }
    };

    while (heap.size > 0) {
      const top = heap.peek()!;

      if (top.currentTerm !== prevTerm) {
        flushTerm();
        termAccum = new Map();
        prevTerm = top.currentTerm;
      }

      // Drain all heap entries at this term.
      while (heap.size > 0 && heap.peek()!.currentTerm === prevTerm) {
        const entry = heap.pop()!;
        const { docIds, tfs } = toMerge[entry.segIndex].decodePostings(prevTerm);
        for (let i = 0; i < docIds.length; i++) {
          const newId = remapOld2New.get(docIds[i]);
          if (newId !== undefined) {
            termAccum.set(newId, (termAccum.get(newId) ?? 0) + tfs[i]);
          }
        }
        const next = entry.termIter.next();
        if (!next.done) {
          entry.currentTerm = next.value.term;
          heap.push(entry);
        }
      }
    }
    flushTerm();

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

    // Derive totalDocs and totalLen from the new segment list so tombstoned docs
    // are reflected immediately rather than relying on the running counter (which
    // was only ever incremented, never decremented).
    const newTotalDocs = newSegmentList.reduce((s, e) => s + e.docCount, 0);
    const newTotalLen = newSegmentList.reduce((s, e) => s + e.totalLen, 0);
    this.totalDocs = newTotalDocs;
    this.totalLen = newTotalLen;

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

  /** Write manifest atomically. FsBackend.writeBlob is itself atomic (tmp+rename). */
  private async writeManifest(manifest: Manifest): Promise<void> {
    const data = Buffer.from(JSON.stringify(manifest), "utf8");
    await this.backend.writeBlob(MANIFEST_FILE, data);
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
  /** The tokenizer kind as recorded in the persisted manifest (null for fresh index). */
  get persistedTokenizerKind(): string | null { return this._persistedTokenizerKind; }
  /** The tokenizer minLen as recorded in the persisted manifest (null for fresh index). */
  get persistedTokenizerMinLen(): number | null { return this._persistedTokenizerMinLen; }
  /** True when an existing manifest was loaded (not a fresh index). */
  get manifestLoaded(): boolean { return this._manifestLoaded; }
}
