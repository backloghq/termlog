/**
 * TermLog — high-level facade over SegmentManager.
 *
 * Adds string↔number docId mapping, automatic tokenization, and BM25 search.
 * The SegmentManager, BM25Ranker, etc. are still exported for advanced callers.
 *
 * docIds mapping is persisted in `docids.json` alongside `manifest.json`.
 * The mapping is written atomically alongside the manifest after each flush.
 */

import { SegmentManager } from "./manager.js";
import { BM25Ranker } from "./scoring.js";
import { FsBackend } from "./storage.js";
import { DEFAULT_TOKENIZER } from "./tokenizer.js";
import type { StorageBackend } from "./storage.js";
import type { Tokenizer } from "./tokenizer.js";

const DOCIDS_FILE = "docids.json";

export class MappingCorruptionError extends Error {
  constructor(detail: string) {
    super(`DocId mapping corruption: ${detail}`);
    this.name = "MappingCorruptionError";
  }
}

export interface TermLogOptions {
  dir: string;
  backend?: StorageBackend;
  tokenizer?: Tokenizer;
  flushThreshold?: number;
  mergeThreshold?: number;
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

    const mgr = await SegmentManager.open({
      backend,
      flushThreshold: opts.flushThreshold,
      mergeThreshold: opts.mergeThreshold,
    });

    const tl = new TermLog(mgr, tokenizer, backend, opts.k1 ?? 1.2, opts.b ?? 0.75);
    await tl.loadDocIds();
    return tl;
  }

  private async loadDocIds(): Promise<void> {
    let raw: Buffer;
    try {
      raw = await this.backend.readBlob(DOCIDS_FILE);
    } catch {
      // No docids.json yet — fresh index.
      return;
    }

    let parsed: { nextNumId: number; entries: [string, number][] };
    try {
      parsed = JSON.parse(raw.toString("utf8")) as typeof parsed;
    } catch (err) {
      throw new MappingCorruptionError(String(err));
    }

    this.nextNumId = parsed.nextNumId;
    for (const [str, num] of parsed.entries) {
      this.strToNum.set(str, num);
      this.numToStr.set(num, str);
    }
  }

  private async saveDocIds(): Promise<void> {
    const payload = {
      nextNumId: this.nextNumId,
      entries: [...this.strToNum.entries()],
    };
    const data = Buffer.from(JSON.stringify(payload), "utf8");
    await this.backend.writeBlob(DOCIDS_FILE, data);
  }

  /** Add or update a document. Tokenizes text and indexes it. */
  async add(docId: string, text: string): Promise<void> {
    const tokens = this.tokenizer.tokenize(text);
    const freq = new Map<string, number>();
    for (const t of tokens) freq.set(t, (freq.get(t) ?? 0) + 1);
    const terms = [...freq.entries()].map(([term, tf]) => ({ term, tf }));

    // Allocate or reuse numeric ID.
    let numId = this.strToNum.get(docId);
    if (numId === undefined) {
      numId = this.nextNumId++;
      this.strToNum.set(docId, numId);
      this.numToStr.set(numId, docId);
    }

    await this.mgr.add(numId, terms);
    await this.saveDocIds();
  }

  /** Remove a document by its string docId. Idempotent. */
  async remove(docId: string): Promise<void> {
    const numId = this.strToNum.get(docId);
    if (numId === undefined) return; // not in index
    await this.mgr.remove(numId);
    this.strToNum.delete(docId);
    this.numToStr.delete(numId);
    await this.saveDocIds();
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
    const scored = ranker.score(terms, segments, this.mgr.indexTotalDocs, this.mgr.indexTotalLen, limit);

    // mode "and" is not yet wired through BM25Ranker — or is the default.
    void mode;

    return scored.flatMap((r) => {
      const str = this.numToStr.get(r.docId);
      if (str === undefined) return [];
      return [{ docId: str, score: r.score }];
    });
  }

  /** Explicitly flush the write buffer. Auto-flush still triggers on threshold. */
  async flush(): Promise<void> {
    await this.mgr.flush();
    await this.saveDocIds();
  }

  /** Merge segments. */
  async compact(): Promise<void> {
    await this.mgr.compact();
  }

  /** Close: flush pending writes. */
  async close(): Promise<void> {
    await this.mgr.flush();
    await this.saveDocIds();
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
