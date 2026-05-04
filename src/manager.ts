/**
 * SegmentManager — coordinates write buffer, segment flush, manifest, and compaction.
 *
 * Manifest format (manifest.json):
 *   { version: 2, generation: N, segments: [{id, docCount, totalLen, tier}],
 *     tokenizer: {kind, minLen}, totalDocs, totalLen }
 *
 * Manifest is updated atomically: write manifest.tmp → rename to manifest.json.
 * Readers call `segments()` to get an immutable snapshot of the current reader list;
 * a concurrent flush or compaction does not affect that snapshot (segments are immutable,
 * old-segment deletion is deferred until after manifest commit).
 *
 * Compaction (LSM size-tiered):
 *   Each segment has a tier. After each flush the new segment starts at tier 0.
 *   chooseCompactionTargets() finds the lowest tier with >= fanout segments and merges them
 *   into a single segment at tier+1. This cascades: after a successful merge, if the
 *   result pushes tier+1 over the fanout, another merge is triggered automatically.
 *   Manual compact() merges everything into a single segment at maxTier+1.
 */

import { open as fsOpen, readFile as fsReadFile, unlink as fsUnlink } from "node:fs/promises";
import { join as pathJoin } from "node:path";
import { SegmentWriter, SegmentReader, SegmentCorruptionError } from "./segment.js";
import { MinHeap } from "./heap.js";
import type { StorageBackend } from "./storage.js";
import type { DictEntry } from "./term-dict.js";

/** One entry in the manifest's segment list. */
export interface ManifestSegmentEntry {
  id: string;
  docCount: number;
  totalLen: number;
  /** Compaction tier. 0 = freshly flushed; N = result of merging fanout tier-(N-1) segments. */
  tier: number;
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

const MANIFEST_VERSION = 2;
const MANIFEST_FILE = "manifest.json";
const LOCK_FILE = ".lock";
export const DEFAULT_FLUSH_THRESHOLD = 1000;
const DEFAULT_FANOUT = 4;

/** Thrown when manifest.json exists but cannot be parsed. */
export class ManifestCorruptionError extends Error {
  constructor(public readonly detail: string) {
    super(`Manifest corruption: ${detail}`);
    this.name = "ManifestCorruptionError";
  }
}

/** Thrown when manifest.json has a version other than MANIFEST_VERSION. */
export class ManifestVersionError extends Error {
  constructor(
    public readonly found: number,
    public readonly expected: number,
  ) {
    super(`Unsupported manifest version ${found} (expected ${expected})`);
    this.name = "ManifestVersionError";
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
  /**
   * How many same-tier segments trigger a tier merge (size-tiered compaction).
   * Default: 4.
   */
  fanout?: number;
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
  private readonly tieredFanout: number;
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
    tieredFanout: number,
    tokenizerConfig: TokenizerConfig,
    onBeforeManifest?: () => Promise<void>,
  ) {
    this.backend = backend;
    this.flushThreshold = flushThreshold;
    this.tieredFanout = tieredFanout;
    this.tokenizerConfig = tokenizerConfig;
    this.onBeforeManifest = onBeforeManifest;
  }

  static async open(opts: SegmentManagerOpts): Promise<SegmentManager> {
    const effectiveFanout = opts.fanout ?? DEFAULT_FANOUT;
    const mgr = new SegmentManager(
      opts.backend,
      opts.flushThreshold ?? DEFAULT_FLUSH_THRESHOLD,
      effectiveFanout,
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
    // Temp-file cleanup is handled by recoverOrphans() (called after this method).
    // manifest.json is either absent or fully written — FsBackend.writeBlob is
    // atomic (nonce-suffix tmp then rename), so no partial manifest.json exists.

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

    if (manifest.version !== MANIFEST_VERSION) {
      throw new ManifestVersionError(manifest.version, MANIFEST_VERSION);
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
    // Missing segments (ENOENT) are re-thrown as SegmentCorruptionError so callers
    // get a typed error instead of a raw OS error.
    const readers: SegmentReader[] = [];
    for (const entry of manifest.segments) {
      try {
        const reader = await SegmentReader.open(`${entry.id}.seg`, this.backend);
        readers.push(reader);
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code === "ENOENT") {
          throw new SegmentCorruptionError("footer", `segment file missing: ${entry.id}.seg`);
        }
        throw err;
      }
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
   * Auto-flushes when the buffer reaches `flushThreshold`, then cascades tiered compaction
   * as long as any tier has >= fanout segments.
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
        await this.cascadeCompactLocked();
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
      tier: 0,
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
   * The merged result is assigned tier = maxExistingTier + 1.
   */
  async compact(): Promise<void> {
    return this.serialize(async () => {
      if (this.manifestSegments.length <= 1) return;
      const maxTier = this.manifestSegments.reduce((m, e) => Math.max(m, e.tier), 0);
      const allIndices = this.manifestSegments.map((_, i) => i);
      await this.tieredCompactLocked(allIndices, maxTier + 1);
    });
  }

  /**
   * Pick the compaction target: lowest tier with >= fanout segments.
   * Returns the tier and exactly `fanout` segment indices from that tier (the first fanout
   * by position, so segments are merged in arrival order).
   * Returns null if no tier has enough segments.
   */
  private chooseCompactionTargets(): { tier: number; indices: number[] } | null {
    const byTier = new Map<number, number[]>();
    for (let i = 0; i < this.manifestSegments.length; i++) {
      const t = this.manifestSegments[i].tier;
      const arr = byTier.get(t);
      if (arr) arr.push(i);
      else byTier.set(t, [i]);
    }
    // Find the lowest tier with >= fanout segments.
    let minTier = Infinity;
    for (const [tier, indices] of byTier) {
      if (indices.length >= this.tieredFanout && tier < minTier) minTier = tier;
    }
    if (minTier === Infinity) return null;
    // Merge exactly fanout segments per step (first fanout by position).
    const allAtTier = byTier.get(minTier)!;
    return { tier: minTier, indices: allAtTier.slice(0, this.tieredFanout) };
  }

  /**
   * Run cascade compaction: repeatedly find the lowest eligible tier and merge it
   * until no tier has >= fanout segments.
   */
  private async cascadeCompactLocked(): Promise<void> {
    let target = this.chooseCompactionTargets();
    while (target !== null) {
      await this.tieredCompactLocked(target.indices, target.tier + 1);
      target = this.chooseCompactionTargets();
    }
  }

  /**
   * Merge the segments at the given indices into a single new segment with the
   * given output tier. Segments not in the merge set are preserved unchanged.
   *
   * Algorithm:
   *   1. Build union tombstone set across merged segments.
   *   2. Collect surviving docs (original docIds preserved — no renumbering).
   *   3. K-way merge posting iterators by (term, segIndex) lex order.
   *   4. Write merged segment atomically (.seg.tmp → rename).
   *   5. Atomic manifest swap: merged segment + any segments not in the merge set.
   *   6. Carry forward tombstones that target docs in unmerged segments.
   *   7. Delete old segment files post-commit.
   *
   * DocIds are NEVER renumbered. TermLog assigns globally unique numIds; the
   * segment format supports sparse uint32 docIds natively (sidecar stores
   * [(docId, length)] pairs; postings use delta-encoded VByte). Preserving
   * original docIds is what keeps TermLog.numToStr lookups correct after merge
   * and ensures tombstones (which store original numIds) always match.
   */
  private async tieredCompactLocked(indices: number[], outputTier: number): Promise<void> {
    if (indices.length <= 1) return;

    const toMergeIndices = new Set(indices);
    const toMergeEntries = indices.map((i) => this.manifestSegments[i]);
    const toMergeIds = new Set(toMergeEntries.map((e) => e.id));

    // Map segment index → position in readerSnapshot. The two arrays are parallel.
    const toMergeReaders = indices.map((i) => this.readerSnapshot[i]);

    // Build the union tombstone set across all merged segments.
    const tombstoneUnion = new Set<number>();
    for (const seg of toMergeReaders) {
      for (const id of seg.tombstones) tombstoneUnion.add(id);
    }

    // Collect surviving docs: original docId → length (no renumbering).
    // tombstoneUnion contains original numIds; localId IS the original numId
    // because TermLog allocates globally unique numIds and we never renumber.
    const survivingDocs = new Map<number, number>(); // docId → len
    for (const seg of toMergeReaders) {
      for (const [docId, len] of seg.docLenEntries()) {
        if (!tombstoneUnion.has(docId)) {
          survivingDocs.set(docId, len);
        }
      }
    }

    // --- Step 2: streaming k-way merge of term iterators into SegmentWriter ---
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

    for (let si = 0; si < toMergeReaders.length; si++) {
      const termIter = toMergeReaders[si].terms();
      const first = termIter.next();
      if (!first.done) {
        heap.push({ segIndex: si, termIter, currentTerm: first.value.term });
      }
    }

    let prevTerm: string | null = null;
    let termAccum = new Map<number, number>();

    const flushTerm = () => {
      if (prevTerm === null || termAccum.size === 0) return;
      for (const [docId, tf] of termAccum) {
        writer.addPosting(prevTerm, docId, tf);
      }
    };

    while (heap.size > 0) {
      const top = heap.peek()!;

      if (top.currentTerm !== prevTerm) {
        flushTerm();
        termAccum = new Map();
        prevTerm = top.currentTerm;
      }

      while (heap.size > 0 && heap.peek()!.currentTerm === prevTerm) {
        const entry = heap.pop()!;
        const postIter = toMergeReaders[entry.segIndex].postings(prevTerm!);
        let posting = postIter.next();
        while (!posting.done) {
          const docId = posting.value.docId;
          if (survivingDocs.has(docId)) {
            termAccum.set(docId, (termAccum.get(docId) ?? 0) + posting.value.tf);
          }
          posting = postIter.next();
        }
        const next = entry.termIter.next();
        if (!next.done) {
          entry.currentTerm = next.value.term;
          heap.push(entry);
        }
      }
    }
    flushTerm();

    for (const [docId, len] of survivingDocs) {
      writer.setDocLength(docId, len);
    }

    // Carry forward tombstones that target docs NOT in the merged segments.
    // These tombstones target docs in unmerged segments and must not be dropped.
    const mergedDocIds = new Set<number>();
    for (const seg of toMergeReaders) {
      for (const [docId] of seg.docLenEntries()) mergedDocIds.add(docId);
    }
    const unresolvedTombstones = [...tombstoneUnion].filter((id) => !mergedDocIds.has(id));
    if (unresolvedTombstones.length > 0) {
      writer.setTombstones(unresolvedTombstones);
    }

    await writer.flush(mergedId, this.backend);

    // --- Step 3: atomic manifest swap ---
    const mergedEntry: ManifestSegmentEntry = {
      id: mergedId,
      docCount: survivingDocs.size,
      totalLen: [...survivingDocs.values()].reduce((s, l) => s + l, 0),
      tier: outputTier,
    };

    // Keep segments not in the merge set, preserving their original order.
    const survivingEntries = this.manifestSegments.filter((e) => !toMergeIds.has(e.id));
    const newSegmentList = [mergedEntry, ...survivingEntries];

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

    // Rebuild reader snapshot: merged reader replaces merged segments; others kept in order.
    const mergedReader = await SegmentReader.open(`${mergedId}.seg`, this.backend);
    const survivingReaders = this.readerSnapshot.filter((_, i) => !toMergeIndices.has(i));
    this.readerSnapshot = [mergedReader, ...survivingReaders];

    // --- Step 4: delete old segment files (post-manifest-commit) ---
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
   *
   * Do NOT mutate the returned array. It is the live readerSnapshot reference;
   * mutations would corrupt internal state. Treat it as readonly.
   */
  segments(): readonly SegmentReader[] {
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
