/**
 * Segment writer + reader — single binary file per segment.
 *
 * File layout (all offsets written in the footer, so the postings stream
 * can be written sequentially without seeking back):
 *
 *   [postings region]       — concatenated VByte posting lists
 *   [doc-length sidecar]    — uint32 docCount, then [uint32 docId, uint32 length] * docCount
 *   [tombstones region]     — uint32 count, then uint32[count] sorted docIds
 *   [term dictionary]       — TermDict.serialize() output
 *   [footer]                — magic, version, offsets, CRC32s
 *
 * Footer layout (all little-endian uint32 unless noted):
 *   4  bytes  magic              = SEGMENT_MAGIC
 *   4  bytes  version            = SEGMENT_VERSION (2)
 *   4  bytes  postingsOffset     (always 0; retained for forward-compat)
 *   4  bytes  postingsLength
 *   4  bytes  sidecarOffset
 *   4  bytes  sidecarLength
 *   4  bytes  tombstonesOffset
 *   4  bytes  tombstonesLength
 *   4  bytes  dictOffset
 *   4  bytes  dictLength
 *   4  bytes  postingsCrc32
 *   4  bytes  sidecarCrc32
 *   4  bytes  tombstonesCrc32
 *   4  bytes  dictCrc32
 *   4  bytes  docCount
 *   4  bytes  termCount
 *  64  bytes  total footer size
 */

import { encodePostings, decodePostings, postingIterator } from "./codec.js";
import { TermDict } from "./term-dict.js";
import { crc32 } from "./crc32.js";
import type { StorageBackend } from "./storage.js";
import type { Posting } from "./codec.js";
import type { DictEntry } from "./term-dict.js";

const SEGMENT_MAGIC = 0x54524c47; // "TRLG"
const SEGMENT_VERSION = 2;
const FOOTER_SIZE = 64;

export class SegmentCorruptionError extends Error {
  constructor(public readonly region: "postings" | "sidecar" | "tombstones" | "dict" | "footer", detail: string) {
    super(`Segment corruption in ${region}: ${detail}`);
    this.name = "SegmentCorruptionError";
  }
}

/** Accumulated state for one term during segment construction. */
interface TermAccumulator {
  docIds: number[];
  tfs: number[];
}

/**
 * Builds a segment from a stream of (term, docId, tf) calls.
 * Call `addPosting` in any order. `flush` sorts, encodes, and writes atomically.
 * Optionally include a tombstone set for doc IDs removed from prior segments.
 */
export class SegmentWriter {
  private readonly termMap = new Map<string, TermAccumulator>();
  private readonly docLengths = new Map<number, number>();
  private tombstones: number[] = [];

  addPosting(term: string, docId: number, tf: number): void {
    let acc = this.termMap.get(term);
    if (!acc) { acc = { docIds: [], tfs: [] }; this.termMap.set(term, acc); }
    acc.docIds.push(docId);
    acc.tfs.push(tf);
  }

  /** Record the token count (length) for a document. Required for BM25 normalization. */
  setDocLength(docId: number, length: number): void {
    this.docLengths.set(docId, length);
  }

  /** Set tombstones — sorted array of doc IDs removed from prior segments. */
  setTombstones(docIds: number[]): void {
    this.tombstones = [...docIds].sort((a, b) => a - b);
  }

  get termCount(): number { return this.termMap.size; }
  get docCount(): number { return this.docLengths.size; }

  /**
   * Serialize and write to backend as `<id>.seg` atomically.
   * Returns the path written.
   */
  async flush(id: string, backend: StorageBackend): Promise<string> {
    const segPath = `${id}.seg`;

    // --- Postings region ---
    const postingBuffers: Buffer[] = [];
    const dictMap = new Map<string, { postingsOffset: number; postingsLength: number; df: number }>();
    let postingsOffset = 0;

    const sortedTerms = [...this.termMap.entries()].sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
    for (const [term, acc] of sortedTerms) {
      const order = acc.docIds.map((_, i) => i).sort((a, b) => acc.docIds[a] - acc.docIds[b]);
      const sortedIds = order.map((i) => acc.docIds[i]);
      const sortedTfs = order.map((i) => acc.tfs[i]);

      const buf = encodePostings(sortedIds, sortedTfs);
      postingBuffers.push(buf);
      dictMap.set(term, { postingsOffset, postingsLength: buf.length, df: new Set(sortedIds).size });
      postingsOffset += buf.length;
    }
    const postingsRegion = Buffer.concat(postingBuffers);

    // --- Doc-length sidecar ---
    // Format: uint32 docCount, then [uint32 docId, uint32 length] * docCount
    const docIds = [...this.docLengths.keys()].sort((a, b) => a - b);
    const sidecarBuf = Buffer.allocUnsafe(4 + docIds.length * 8);
    sidecarBuf.writeUInt32LE(docIds.length, 0);
    for (let i = 0; i < docIds.length; i++) {
      sidecarBuf.writeUInt32LE(docIds[i], 4 + i * 8);
      sidecarBuf.writeUInt32LE(this.docLengths.get(docIds[i])!, 4 + i * 8 + 4);
    }

    // --- Tombstones region ---
    // Format: uint32 count, then uint32[count] sorted doc IDs
    const sortedTombstones = this.tombstones;
    const tombstonesBuf = Buffer.allocUnsafe(4 + sortedTombstones.length * 4);
    tombstonesBuf.writeUInt32LE(sortedTombstones.length, 0);
    for (let i = 0; i < sortedTombstones.length; i++) {
      tombstonesBuf.writeUInt32LE(sortedTombstones[i], 4 + i * 4);
    }

    // --- Term dictionary ---
    const dict = TermDict.fromMap(dictMap);
    const dictBuf = dict.serialize();

    // --- CRC32s ---
    const postingsCrc  = crc32(postingsRegion);
    const sidecarCrc   = crc32(sidecarBuf);
    const tombstonesCrc = crc32(tombstonesBuf);
    const dictCrc      = crc32(dictBuf);

    // --- Footer ---
    const sidecarOffset     = postingsRegion.length;
    const tombstonesOffset  = sidecarOffset + sidecarBuf.length;
    const dictOffset        = tombstonesOffset + tombstonesBuf.length;

    const footer = Buffer.allocUnsafe(FOOTER_SIZE);
    let fo = 0;
    footer.writeUInt32LE(SEGMENT_MAGIC,           fo); fo += 4;
    footer.writeUInt32LE(SEGMENT_VERSION,         fo); fo += 4;
    footer.writeUInt32LE(0,                       fo); fo += 4; // postingsOffset (always 0)
    footer.writeUInt32LE(postingsRegion.length,   fo); fo += 4;
    footer.writeUInt32LE(sidecarOffset,           fo); fo += 4;
    footer.writeUInt32LE(sidecarBuf.length,       fo); fo += 4;
    footer.writeUInt32LE(tombstonesOffset,        fo); fo += 4;
    footer.writeUInt32LE(tombstonesBuf.length,    fo); fo += 4;
    footer.writeUInt32LE(dictOffset,              fo); fo += 4;
    footer.writeUInt32LE(dictBuf.length,          fo); fo += 4;
    footer.writeUInt32LE(postingsCrc,             fo); fo += 4;
    footer.writeUInt32LE(sidecarCrc,              fo); fo += 4;
    footer.writeUInt32LE(tombstonesCrc,           fo); fo += 4;
    footer.writeUInt32LE(dictCrc,                 fo); fo += 4;
    footer.writeUInt32LE(docIds.length,           fo); fo += 4;
    footer.writeUInt32LE(dict.size,               fo);

    const segData = Buffer.concat([postingsRegion, sidecarBuf, tombstonesBuf, dictBuf, footer]);

    // FsBackend.writeBlob is already atomic (nonce-tmp → rename).
    await backend.writeBlob(segPath, segData);

    return segPath;
  }
}

/** Parsed footer fields. */
interface SegmentFooter {
  postingsLength: number;
  sidecarOffset: number;
  sidecarLength: number;
  tombstonesOffset: number;
  tombstonesLength: number;
  dictOffset: number;
  dictLength: number;
  postingsCrc32: number;
  sidecarCrc32: number;
  tombstonesCrc32: number;
  dictCrc32: number;
  docCount: number;
  termCount: number;
}

/**
 * Reads an immutable segment file. Verifies CRC32 on open.
 * All region data is loaded eagerly (v0.1; lazy mmap is v0.2+).
 */
export class SegmentReader {
  private readonly postingsRegion: Buffer;
  private readonly dict: TermDict;
  private readonly docLenMap: Map<number, number>;
  /** Sorted tombstone docIds — docs that have been removed and belong to prior segments. */
  readonly tombstones: Uint32Array;
  readonly docCount: number;
  readonly termCount: number;

  private constructor(
    postingsRegion: Buffer,
    dict: TermDict,
    docLenMap: Map<number, number>,
    tombstones: Uint32Array,
    docCount: number,
    termCount: number,
  ) {
    this.postingsRegion = postingsRegion;
    this.dict = dict;
    this.docLenMap = docLenMap;
    this.tombstones = tombstones;
    this.docCount = docCount;
    this.termCount = termCount;
  }

  static async open(path: string, backend: StorageBackend): Promise<SegmentReader> {
    const data = await backend.readBlob(path);

    if (data.length < FOOTER_SIZE) {
      throw new SegmentCorruptionError("footer", "file too small to contain a footer");
    }

    // Read footer from the last FOOTER_SIZE bytes
    const footerStart = data.length - FOOTER_SIZE;
    const f = data.subarray(footerStart);
    let fo = 0;

    const magic   = f.readUInt32LE(fo); fo += 4;
    const version = f.readUInt32LE(fo); fo += 4;
    /* postingsOffset */ fo += 4;
    const postingsLength    = f.readUInt32LE(fo); fo += 4;
    const sidecarOffset     = f.readUInt32LE(fo); fo += 4;
    const sidecarLength     = f.readUInt32LE(fo); fo += 4;
    const tombstonesOffset  = f.readUInt32LE(fo); fo += 4;
    const tombstonesLength  = f.readUInt32LE(fo); fo += 4;
    const dictOffset        = f.readUInt32LE(fo); fo += 4;
    const dictLength        = f.readUInt32LE(fo); fo += 4;
    const postingsCrc32     = f.readUInt32LE(fo); fo += 4;
    const sidecarCrc32      = f.readUInt32LE(fo); fo += 4;
    const tombstonesCrc32   = f.readUInt32LE(fo); fo += 4;
    const dictCrc32         = f.readUInt32LE(fo); fo += 4;
    const docCount          = f.readUInt32LE(fo); fo += 4;
    const termCount         = f.readUInt32LE(fo);

    if (magic !== SEGMENT_MAGIC) {
      throw new SegmentCorruptionError("footer", `bad magic: 0x${magic.toString(16)}`);
    }
    if (version !== SEGMENT_VERSION) {
      throw new SegmentCorruptionError("footer", `unknown version: ${version}`);
    }

    const footer: SegmentFooter = {
      postingsLength, sidecarOffset, sidecarLength,
      tombstonesOffset, tombstonesLength,
      dictOffset, dictLength,
      postingsCrc32, sidecarCrc32, tombstonesCrc32, dictCrc32,
      docCount, termCount,
    };

    // Verify postings region
    const postingsRegion = data.subarray(0, footer.postingsLength);
    if (crc32(postingsRegion) !== footer.postingsCrc32) {
      throw new SegmentCorruptionError("postings", "CRC32 mismatch");
    }

    // Verify sidecar
    const sidecarBuf = data.subarray(footer.sidecarOffset, footer.sidecarOffset + footer.sidecarLength);
    if (crc32(sidecarBuf) !== footer.sidecarCrc32) {
      throw new SegmentCorruptionError("sidecar", "CRC32 mismatch");
    }

    // Verify tombstones
    const tombstonesBuf = data.subarray(footer.tombstonesOffset, footer.tombstonesOffset + footer.tombstonesLength);
    if (crc32(tombstonesBuf) !== footer.tombstonesCrc32) {
      throw new SegmentCorruptionError("tombstones", "CRC32 mismatch");
    }

    // Verify dict
    const dictBuf = data.subarray(footer.dictOffset, footer.dictOffset + footer.dictLength);
    if (crc32(dictBuf) !== footer.dictCrc32) {
      throw new SegmentCorruptionError("dict", "CRC32 mismatch");
    }

    // Deserialize dict
    const dict = TermDict.deserialize(dictBuf);

    // Deserialize doc-length sidecar
    const docLenMap = new Map<number, number>();
    const storedDocCount = sidecarBuf.readUInt32LE(0);
    for (let i = 0; i < storedDocCount; i++) {
      const docId = sidecarBuf.readUInt32LE(4 + i * 8);
      const len   = sidecarBuf.readUInt32LE(4 + i * 8 + 4);
      docLenMap.set(docId, len);
    }

    // Deserialize tombstones
    const tombstoneCount = tombstonesBuf.readUInt32LE(0);
    const tombstones = new Uint32Array(tombstoneCount);
    for (let i = 0; i < tombstoneCount; i++) {
      tombstones[i] = tombstonesBuf.readUInt32LE(4 + i * 4);
    }

    return new SegmentReader(postingsRegion, dict, docLenMap, tombstones, footer.docCount, footer.termCount);
  }

  /** Returns true if docId is tombstoned in this segment (binary search). */
  isTombstoned(docId: number): boolean {
    let lo = 0;
    let hi = this.tombstones.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      const v = this.tombstones[mid];
      if (v === docId) return true;
      if (v < docId) lo = mid + 1;
      else hi = mid - 1;
    }
    return false;
  }

  /** Look up a term's metadata. Returns undefined if not present. */
  lookupTerm(term: string): DictEntry | undefined {
    return this.dict.lookup(term);
  }

  /** Lazy posting iterator for a term. Returns an empty iterator if term not found. */
  postings(term: string): Iterator<Posting> {
    const entry = this.dict.lookup(term);
    if (!entry) {
      return { next() { return { done: true, value: undefined as unknown as Posting }; } };
    }
    const slice = this.postingsRegion.subarray(entry.postingsOffset, entry.postingsOffset + entry.postingsLength);
    return postingIterator(slice);
  }

  /** Fully decode postings for a term — convenience for tests and scoring. */
  decodePostings(term: string): { docIds: number[]; tfs: number[] } {
    const entry = this.dict.lookup(term);
    if (!entry) return { docIds: [], tfs: [] };
    const slice = this.postingsRegion.subarray(entry.postingsOffset, entry.postingsOffset + entry.postingsLength);
    return decodePostings(slice);
  }

  /** Return the stored document length for BM25 normalization. */
  docLen(docId: number): number {
    return this.docLenMap.get(docId) ?? 0;
  }

  /** Iterate all (docId, length) pairs in this segment's sidecar — for compaction. */
  docLenEntries(): IterableIterator<[number, number]> {
    return this.docLenMap.entries();
  }

  /** All terms in sorted order (for compaction merging). */
  *terms(): Generator<DictEntry> {
    for (const entry of this.dict) yield entry;
  }
}
