/**
 * SegmentManager — coordinates write buffer, segment flush, and manifest.
 *
 * Manifest format (manifest.json):
 *   { version: 1, generation: N, segments: [{id, docCount, totalLen}],
 *     tokenizer: {kind, minLen}, totalDocs, totalLen }
 *
 * Manifest is updated atomically: write manifest.tmp → rename to manifest.json.
 * Readers call `segments()` to get an immutable snapshot of the current reader list;
 * a concurrent flush does not affect that snapshot (segments are immutable, deletions
 * are deferred to compaction).
 */

import { SegmentWriter, SegmentReader } from "./segment.js";
import type { StorageBackend } from "./storage.js";

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

export interface SegmentManagerOpts {
  backend: StorageBackend;
  /** How many buffered docs trigger an automatic flush. Default: 1000. */
  flushThreshold?: number;
  tokenizer?: TokenizerConfig;
}

export class SegmentManager {
  private readonly backend: StorageBackend;
  private readonly flushThreshold: number;
  private readonly tokenizerConfig: TokenizerConfig;

  private generation = 0;
  private manifestSegments: ManifestSegmentEntry[] = [];
  /** Current immutable snapshot of open readers — replaced atomically on flush. */
  private readerSnapshot: SegmentReader[] = [];
  /** Monotonically increasing segment ID counter (based on generation). */
  private nextSegCounter = 0;

  private buffer: BufferedDoc[] = [];
  private totalDocs = 0;
  private totalLen = 0;

  private constructor(
    backend: StorageBackend,
    flushThreshold: number,
    tokenizerConfig: TokenizerConfig,
  ) {
    this.backend = backend;
    this.flushThreshold = flushThreshold;
    this.tokenizerConfig = tokenizerConfig;
  }

  static async open(opts: SegmentManagerOpts): Promise<SegmentManager> {
    const mgr = new SegmentManager(
      opts.backend,
      opts.flushThreshold ?? 1000,
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
   * Auto-flushes when the buffer reaches `flushThreshold`.
   */
  async add(
    docId: number,
    terms: Array<{ term: string; tf: number }>,
  ): Promise<void> {
    const totalLen = terms.reduce((s, t) => s + t.tf, 0);
    this.buffer.push({ docId, terms, totalLen });
    if (this.buffer.length >= this.flushThreshold) {
      await this.flush();
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

  /** Write manifest atomically via tmp-then-rename. */
  private async writeManifest(manifest: Manifest): Promise<void> {
    const data = Buffer.from(JSON.stringify(manifest), "utf8");
    // FsBackend.writeBlob already does atomic rename internally, so writing
    // to MANIFEST_TMP then MANIFEST_FILE gives us two atomic renames.
    // We write MANIFEST_TMP first so a reader can detect a half-written state.
    await this.backend.writeBlob(MANIFEST_TMP, data);
    await this.backend.writeBlob(MANIFEST_FILE, data);
    await this.backend.deleteBlob(MANIFEST_TMP);
  }

  /**
   * Return an immutable snapshot of the current segment readers.
   * Callers may hold this snapshot across a concurrent flush — new segments
   * will not appear in the snapshot (readers are immutable; deletion is deferred).
   */
  segments(): SegmentReader[] {
    return this.readerSnapshot;
  }

  /** The manifest generation counter — incremented on every successful flush. */
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
