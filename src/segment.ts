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
import { crc32, Crc32Stream } from "./crc32.js";
import type { StorageBackend, WriteStream } from "./storage.js";
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

/**
 * Streaming segment writer. Postings are encoded and written to the stream
 * immediately as each term arrives — no in-memory accumulation of the full
 * postings region. Only the term dictionary entries (one small record per distinct
 * term) and doc-length sidecar are held in memory.
 *
 * Usage:
 *   const stream = await backend.createWriteStream(`${id}.seg`);
 *   const writer = new SegmentWriter(stream);
 *   // For each term in lex-sorted order:
 *   await writer.writeTerm(term, sortedDocIds, sortedTfs);
 *   // Set doc lengths (can be called before writeTerm calls):
 *   writer.setDocLength(docId, length);
 *   writer.setTombstones(sortedDocIds);
 *   // Commit:
 *   await writer.finish();
 */
export class SegmentWriter {
  private readonly stream: WriteStream;
  /** Running CRC32 over the postings region — updated as each term is streamed. */
  private readonly postingsCrc = new Crc32Stream();
  /** Dictionary entries accumulated during writeTerm calls. */
  private readonly dictEntries: Array<{ term: string; postingsOffset: number; postingsLength: number; df: number }> = [];
  /** Running byte offset within the postings region. */
  private postingsOffset = 0;

  /**
   * Packed sidecar: interleaved [docId, len, docId, len, ...] in uint32 pairs.
   * Grows with doubling-style reallocation. 8 bytes/doc vs ~64-80 for Map.
   */
  private sidecarArr = new Uint32Array(64); // initial capacity: 32 docs
  private sidecarCount = 0;

  private tombstonesArr: number[] = [];
  private lastTerm: string | undefined = undefined;

  constructor(stream: WriteStream) {
    this.stream = stream;
  }

  /**
   * Write one term's postings. docIds MUST be in ascending order.
   * Terms MUST be called in lex-sorted order (strictly ascending — no duplicates).
   */
  async writeTerm(term: string, sortedDocIds: number[], sortedTfs: number[]): Promise<void> {
    if (this.lastTerm !== undefined && term <= this.lastTerm) {
      throw new RangeError(
        `writeTerm: terms must be in strictly ascending lex order; got "${term}" after "${this.lastTerm}"`,
      );
    }
    this.lastTerm = term;
    const buf = encodePostings(sortedDocIds, sortedTfs);
    this.postingsCrc.update(buf);
    await this.stream.write(buf);
    this.dictEntries.push({
      term,
      postingsOffset: this.postingsOffset,
      postingsLength: buf.length,
      df: sortedDocIds.length,
    });
    this.postingsOffset += buf.length;
  }

  setDocLength(docId: number, length: number): void {
    // Grow with ~1.5× doubling if needed (two uint32 slots per entry).
    if (this.sidecarCount * 2 >= this.sidecarArr.length) {
      const next = new Uint32Array(Math.ceil(this.sidecarArr.length * 1.5));
      next.set(this.sidecarArr);
      this.sidecarArr = next;
    }
    this.sidecarArr[this.sidecarCount * 2]     = docId;
    this.sidecarArr[this.sidecarCount * 2 + 1] = length;
    this.sidecarCount++;
  }

  setTombstones(docIds: number[]): void {
    this.tombstonesArr = [...docIds].sort((a, b) => a - b);
  }

  get termCount(): number { return this.dictEntries.length; }
  get docCount(): number { return this.sidecarCount; }

  /**
   * Write sidecar, tombstones, term dictionary, footer, then commit the stream.
   * After finish() the stream is closed; do not call writeTerm after finish().
   */
  async finish(): Promise<void> {
    const postingsLen = this.postingsOffset;

    // --- Doc-length sidecar ---
    // Sort by docId ascending (caller may not have added in order).
    const pairs: Array<[number, number]> = [];
    for (let i = 0; i < this.sidecarCount; i++) {
      pairs.push([this.sidecarArr[i * 2], this.sidecarArr[i * 2 + 1]]);
    }
    pairs.sort((a, b) => a[0] - b[0]);

    const sidecarBuf = Buffer.allocUnsafe(4 + pairs.length * 8);
    sidecarBuf.writeUInt32LE(pairs.length, 0);
    for (let i = 0; i < pairs.length; i++) {
      sidecarBuf.writeUInt32LE(pairs[i][0], 4 + i * 8);
      sidecarBuf.writeUInt32LE(pairs[i][1], 4 + i * 8 + 4);
    }
    const sidecarCrc = crc32(sidecarBuf);

    // --- Tombstones region ---
    const tombstonesBuf = Buffer.allocUnsafe(4 + this.tombstonesArr.length * 4);
    tombstonesBuf.writeUInt32LE(this.tombstonesArr.length, 0);
    for (let i = 0; i < this.tombstonesArr.length; i++) {
      tombstonesBuf.writeUInt32LE(this.tombstonesArr[i], 4 + i * 4);
    }
    const tombstonesCrc = crc32(tombstonesBuf);

    // --- Term dictionary ---
    const dictMap = new Map<string, { postingsOffset: number; postingsLength: number; df: number }>();
    for (const e of this.dictEntries) dictMap.set(e.term, e);
    const dict = TermDict.fromMap(dictMap);
    const dictBuf = dict.serialize();
    const dictCrc = crc32(dictBuf);

    // --- Footer ---
    const sidecarOffset    = postingsLen;
    const tombstonesOffset = sidecarOffset + sidecarBuf.length;
    const dictOffset       = tombstonesOffset + tombstonesBuf.length;

    const footer = Buffer.allocUnsafe(FOOTER_SIZE);
    let fo = 0;
    footer.writeUInt32LE(SEGMENT_MAGIC,         fo); fo += 4;
    footer.writeUInt32LE(SEGMENT_VERSION,       fo); fo += 4;
    footer.writeUInt32LE(0,                     fo); fo += 4; // postingsOffset (always 0)
    footer.writeUInt32LE(postingsLen,           fo); fo += 4;
    footer.writeUInt32LE(sidecarOffset,         fo); fo += 4;
    footer.writeUInt32LE(sidecarBuf.length,     fo); fo += 4;
    footer.writeUInt32LE(tombstonesOffset,      fo); fo += 4;
    footer.writeUInt32LE(tombstonesBuf.length,  fo); fo += 4;
    footer.writeUInt32LE(dictOffset,            fo); fo += 4;
    footer.writeUInt32LE(dictBuf.length,        fo); fo += 4;
    footer.writeUInt32LE(this.postingsCrc.digest(), fo); fo += 4;
    footer.writeUInt32LE(sidecarCrc,            fo); fo += 4;
    footer.writeUInt32LE(tombstonesCrc,         fo); fo += 4;
    footer.writeUInt32LE(dictCrc,               fo); fo += 4;
    footer.writeUInt32LE(pairs.length,          fo); fo += 4;
    footer.writeUInt32LE(dict.size,             fo);

    // Stream remaining regions and commit.
    await this.stream.write(sidecarBuf);
    await this.stream.write(tombstonesBuf);
    await this.stream.write(dictBuf);
    await this.stream.write(footer);
    await this.stream.end();
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
  /**
   * Interleaved sorted array: [docId0, len0, docId1, len1, ...].
   * Sorted by docId ascending. Binary search via docLen().
   * Replaces Map<number,number> to avoid 1M+ slot heap allocation for large merged segments.
   */
  private readonly docLenArr: Uint32Array;
  /** Sorted tombstone docIds — docs that have been removed and belong to prior segments. */
  readonly tombstones: Uint32Array;
  readonly docCount: number;
  readonly termCount: number;

  private constructor(
    postingsRegion: Buffer,
    dict: TermDict,
    docLenArr: Uint32Array,
    tombstones: Uint32Array,
    docCount: number,
    termCount: number,
  ) {
    this.postingsRegion = postingsRegion;
    this.dict = dict;
    this.docLenArr = docLenArr;
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

    // Deserialize doc-length sidecar into interleaved Uint32Array [docId, len, ...]
    // sorted by docId (the on-disk format already stores them sorted).
    const storedDocCount = sidecarBuf.readUInt32LE(0);
    const docLenArr = new Uint32Array(storedDocCount * 2);
    for (let i = 0; i < storedDocCount; i++) {
      docLenArr[i * 2]     = sidecarBuf.readUInt32LE(4 + i * 8);
      docLenArr[i * 2 + 1] = sidecarBuf.readUInt32LE(4 + i * 8 + 4);
    }

    // Deserialize tombstones
    const tombstoneCount = tombstonesBuf.readUInt32LE(0);
    const tombstones = new Uint32Array(tombstoneCount);
    for (let i = 0; i < tombstoneCount; i++) {
      tombstones[i] = tombstonesBuf.readUInt32LE(4 + i * 4);
    }

    return new SegmentReader(postingsRegion, dict, docLenArr, tombstones, footer.docCount, footer.termCount);
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
  postings(term: string): Iterator<Posting, undefined> {
    const entry = this.dict.lookup(term);
    if (!entry) {
      return { next(): IteratorReturnResult<undefined> { return { done: true, value: undefined }; } };
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

  /** Return the stored document length for BM25 normalization (binary search). */
  docLen(docId: number): number {
    let lo = 0;
    let hi = (this.docLenArr.length >>> 1) - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      const id = this.docLenArr[mid * 2];
      if (id === docId) return this.docLenArr[mid * 2 + 1];
      if (id < docId) lo = mid + 1;
      else hi = mid - 1;
    }
    return 0;
  }

  /** Iterate all (docId, length) pairs in this segment's sidecar — for compaction. */
  *docLenEntries(): Generator<[number, number]> {
    for (let i = 0; i < this.docLenArr.length; i += 2) {
      yield [this.docLenArr[i], this.docLenArr[i + 1]];
    }
  }

  /** All terms in sorted order (for compaction merging). */
  *terms(): Generator<DictEntry> {
    for (const entry of this.dict) yield entry;
  }
}
