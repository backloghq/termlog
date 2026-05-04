/**
 * TermLog — high-level facade over SegmentManager.
 *
 * Adds string↔number docId mapping, automatic tokenization, and BM25 search.
 * The SegmentManager, BM25Ranker, etc. are still exported for advanced callers.
 *
 * The docId mapping is persisted as an append-only journal (`docids.log`) with
 * periodic snapshots (`docids.snap`). The log is appended before each manifest
 * commit so the two are always consistent. On close the log is collapsed into
 * the snapshot.
 */

import { SegmentManager } from "./manager.js";
import { BM25Ranker } from "./scoring.js";
import { FsBackend } from "./storage.js";
import { DEFAULT_TOKENIZER } from "./tokenizer.js";
import type { StorageBackend } from "./storage.js";
import type { Tokenizer } from "./tokenizer.js";

const DOCIDS_SNAP = "docids.snap";
const DOCIDS_LOG  = "docids.log";

export class MappingCorruptionError extends Error {
  constructor(public readonly detail: string) {
    super(`DocId mapping corruption: ${detail}`);
    this.name = "MappingCorruptionError";
  }
}

export class TokenizerMismatchError extends Error {
  constructor(
    public readonly persisted: string,
    public readonly runtime: string,
  ) {
    super(`Tokenizer mismatch: index was built with "${persisted}" but opened with "${runtime}"`);
    this.name = "TokenizerMismatchError";
  }
}

export interface TermLogOptions {
  dir: string;
  backend?: StorageBackend;
  tokenizer?: Tokenizer;
  flushThreshold?: number;
  /** Size-tiered compaction fanout — how many same-tier segments trigger a merge. Default 4. */
  fanout?: number;
  k1?: number;
  b?: number;
}

export class TermLog {
  private readonly mgr: SegmentManager;
  private readonly tokenizer: Tokenizer;
  private readonly backend: StorageBackend;
  private readonly k1: number;
  private readonly b: number;

  /** String docId → numeric docId. */
  private readonly strToNum = new Map<string, number>();
  /** Numeric docId → string docId (for result remapping). */
  private readonly numToStr = new Map<number, string>();
  /** Monotonically increasing allocator. */
  private nextNumId = 0;
  /** Pending log lines (adds/removes) not yet flushed to docids.log. */
  private readonly pendingLog: string[] = [];
  /** Serializes add/remove — guards strToNum/numToStr/pendingLog and coordinates with mgr. */
  private _lock: Promise<void> = Promise.resolve();
  private serialize<R>(fn: () => Promise<R>): Promise<R> {
    const prev = this._lock;
    let resolve!: () => void;
    this._lock = new Promise<void>((r) => { resolve = r; });
    return prev.then(fn).finally(() => resolve());
  }

  private constructor(
    mgr: SegmentManager,
    tokenizer: Tokenizer,
    backend: StorageBackend,
    k1: number,
    b: number,
  ) {
    this.mgr = mgr;
    this.tokenizer = tokenizer;
    this.backend = backend;
    this.k1 = k1;
    this.b = b;
  }

  static async open(opts: TermLogOptions): Promise<TermLog> {
    const backend = opts.backend ?? new FsBackend(opts.dir);
    const tokenizer = opts.tokenizer ?? DEFAULT_TOKENIZER;

    // Indirection box so the onBeforeManifest callback can reference `tl` before
    // it is constructed — the box is mutated once after construction.
    const box = { tl: null as TermLog | null };
    const mgr = await SegmentManager.open({
      backend,
      dir: opts.dir,
      flushThreshold: opts.flushThreshold,
      fanout: opts.fanout,
      tokenizer: { kind: tokenizer.kind, minLen: tokenizer.minLen ?? 1 },
      onBeforeManifest: () => box.tl!.saveDocIds(),
    });

    // When reopening an existing index, validate that the persisted tokenizer config
    // matches the runtime tokenizer. Fresh indexes have no manifest to check against.
    const persistedKind = mgr.persistedTokenizerKind;
    const persistedMinLen = mgr.persistedTokenizerMinLen;
    const runtimeMinLen = tokenizer.minLen ?? 1;
    if (persistedKind !== null && persistedKind !== tokenizer.kind) {
      await mgr.close();
      throw new TokenizerMismatchError(persistedKind, tokenizer.kind);
    }
    if (persistedMinLen !== null && persistedMinLen !== runtimeMinLen) {
      await mgr.close();
      throw new TokenizerMismatchError(
        `${persistedKind}(minLen=${persistedMinLen})`,
        `${tokenizer.kind}(minLen=${runtimeMinLen})`,
      );
    }

    const tl = new TermLog(mgr, tokenizer, backend, opts.k1 ?? 1.2, opts.b ?? 0.75);
    box.tl = tl;
    await tl.loadDocIds();
    return tl;
  }

  private async loadDocIds(): Promise<void> {
    let snapRaw: Buffer | null = null;
    try {
      snapRaw = await this.backend.readBlob(DOCIDS_SNAP);
    } catch {
      // No snap — fresh index.
    }

    if (snapRaw !== null) {
      let parsed: { nextNumId: number; entries: [string, number][] };
      try {
        parsed = JSON.parse(snapRaw.toString("utf8")) as typeof parsed;
      } catch (err) {
        throw new MappingCorruptionError(String(err));
      }
      this.nextNumId = parsed.nextNumId;
      for (const [str, num] of parsed.entries) {
        this.strToNum.set(str, num);
        this.numToStr.set(num, str);
      }
    }

    // Replay log (only exists when there are unflushed deltas since last snapshot).
    let logRaw: Buffer | null = null;
    try {
      logRaw = await this.backend.readBlob(DOCIDS_LOG);
    } catch {
      // No log — nothing to replay.
    }

    if (logRaw !== null) {
      const lines = logRaw.toString("utf8").split("\n").filter(Boolean);
      for (const line of lines) {
        let entry: { op: string; str: string; num: number };
        try {
          entry = JSON.parse(line) as typeof entry;
        } catch (err) {
          throw new MappingCorruptionError(`malformed log line: ${String(err)}`);
        }
        if (entry.op === "add") {
          this.strToNum.set(entry.str, entry.num);
          this.numToStr.set(entry.num, entry.str);
          if (entry.num >= this.nextNumId) this.nextNumId = entry.num + 1;
        } else if (entry.op === "rm") {
          this.strToNum.delete(entry.str);
          this.numToStr.delete(entry.num);
        }
      }
    }
  }

  /** Append pending deltas to docids.log (called by onBeforeManifest — O(delta) not O(N)). */
  private async saveDocIds(): Promise<void> {
    if (this.pendingLog.length === 0) return;
    const chunk = Buffer.from(this.pendingLog.join("\n") + "\n", "utf8");
    if (this.backend.appendBlob) {
      // True append — O(delta), crash-safe via backend fsync.
      await this.backend.appendBlob(DOCIDS_LOG, chunk);
    } else {
      // Fallback for backends without appendBlob (e.g. S3 snapshot mode):
      // read-concat-write. Still O(delta) per call but not atomic at the OS level.
      let existing = "";
      try {
        existing = (await this.backend.readBlob(DOCIDS_LOG)).toString("utf8");
      } catch {
        // No log yet — start fresh.
      }
      await this.backend.writeBlob(DOCIDS_LOG, Buffer.from(existing + chunk.toString("utf8"), "utf8"));
    }
    this.pendingLog.length = 0;
  }

  /** Write a full snapshot and delete the log (called on close/compaction). */
  private async snapshotDocIds(): Promise<void> {
    // Flush any pending log entries first so they're captured in the snapshot.
    await this.saveDocIds();
    const payload = {
      nextNumId: this.nextNumId,
      entries: [...this.strToNum.entries()],
    };
    await this.backend.writeBlob(DOCIDS_SNAP, Buffer.from(JSON.stringify(payload), "utf8"));
    // Delete the log — snapshot is now authoritative.
    try {
      await this.backend.deleteBlob(DOCIDS_LOG);
    } catch {
      // Ignore — log may not exist.
    }
  }

  /** Add or update a document. Tokenizes text and indexes it. */
  async add(docId: string, text: string): Promise<void> {
    const tokens = this.tokenizer.tokenize(text);
    const freq = new Map<string, number>();
    for (const t of tokens) freq.set(t, (freq.get(t) ?? 0) + 1);
    const terms = [...freq.entries()].map(([term, tf]) => ({ term, tf }));

    return this.serialize(async () => {
      const existing = this.strToNum.get(docId);
      if (existing !== undefined) {
        // Tombstone the old numeric ID so stale postings are dropped at compaction.
        // A fresh numId must be allocated so old and new postings never share a docId.
        await this.mgr.remove(existing);
        this.numToStr.delete(existing);
        this.pendingLog.push(JSON.stringify({ op: "rm", str: docId, num: existing }));
      }

      const numId = this.nextNumId++;
      this.strToNum.set(docId, numId);
      this.numToStr.set(numId, docId);
      this.pendingLog.push(JSON.stringify({ op: "add", str: docId, num: numId }));
      await this.mgr.add(numId, terms);
    });
  }

  /** Remove a document by its string docId. Idempotent. */
  async remove(docId: string): Promise<void> {
    return this.serialize(async () => {
      const numId = this.strToNum.get(docId);
      if (numId === undefined) return; // not in index
      await this.mgr.remove(numId);
      this.strToNum.delete(docId);
      this.numToStr.delete(numId);
      this.pendingLog.push(JSON.stringify({ op: "rm", str: docId, num: numId }));
    });
  }

  /**
   * Search for documents matching a query string.
   * Returns results sorted by BM25 score descending.
   */
  async search(
    query: string,
    opts?: { limit?: number; mode?: "and" | "or" },
  ): Promise<Array<{ docId: string; score: number }>> {
    const terms = this.tokenizer.tokenize(query);
    if (terms.length === 0) return [];

    const segments = this.mgr.segments();
    const ranker = new BM25Ranker({ k1: this.k1, b: this.b });

    const mode = opts?.mode ?? "or";
    const limit = opts?.limit;
    const scored = ranker.score(terms, segments, this.mgr.indexTotalDocs, this.mgr.indexTotalLen, limit, mode);

    return scored.flatMap((r) => {
      const str = this.numToStr.get(r.docId);
      if (str === undefined) return [];
      return [{ docId: str, score: r.score }];
    });
  }

  /** Explicitly flush the write buffer. Auto-flush still triggers on threshold. */
  async flush(): Promise<void> {
    await this.mgr.flush();
  }

  /** Merge segments. Snapshots docIds after compact to bound log growth. */
  async compact(): Promise<void> {
    return this.serialize(async () => {
      await this.mgr.compact();
      await this.snapshotDocIds();
    });
  }

  /** Close: flush pending writes and release the advisory lock. */
  async close(): Promise<void> {
    return this.serialize(async () => {
      await this.mgr.close();
      // Snapshot on close — consolidates log into snap, removes log.
      // Also captures in-memory-only removes that never triggered onBeforeManifest.
      await this.snapshotDocIds();
    });
  }

  /** Number of indexed documents (flushed to segments). */
  docCount(): number {
    return this.mgr.indexTotalDocs;
  }

  /** Number of active segments. */
  segmentCount(): number {
    return this.mgr.segments().length;
  }
}
