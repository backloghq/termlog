/**
 * Term dictionary v0.1 — sorted array of (term, postingsOffset, postingsLength, df) entries.
 *
 * The dictionary is built in-memory during segment construction, serialized to a
 * compact binary format, and deserialized for lookup. Binary search is used for
 * O(log n) term lookup.
 *
 * Binary format (little-endian throughout):
 *   uint32 entryCount
 *   [entry * entryCount]
 *
 * Each entry:
 *   uint16  termByteLen
 *   bytes   term (UTF-8)
 *   uint32  postingsOffset
 *   uint32  postingsLength
 *   uint32  df   (document frequency)
 */

export interface DictEntry {
  term: string;
  postingsOffset: number;
  postingsLength: number;
  df: number;
}

/**
 * In-memory term dictionary. Entries must be added in sorted order
 * (enforced by the segment writer which sorts before serializing).
 */
export class TermDict {
  private entries: DictEntry[] = [];

  /** Add an entry. Caller is responsible for inserting in sorted term order. */
  add(entry: DictEntry): void {
    this.entries.push(entry);
  }

  get size(): number {
    return this.entries.length;
  }

  /** Binary search for a term. Returns the entry or undefined if not found. */
  lookup(term: string): DictEntry | undefined {
    let lo = 0;
    let hi = this.entries.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >>> 1;
      const cmp = this.entries[mid].term < term ? -1 : this.entries[mid].term > term ? 1 : 0;
      if (cmp === 0) return this.entries[mid];
      if (cmp < 0) lo = mid + 1;
      else hi = mid - 1;
    }
    return undefined;
  }

  /** Iterate all entries in sorted order. */
  *[Symbol.iterator](): Generator<DictEntry> {
    for (const entry of this.entries) yield entry;
  }

  /** Serialize to a Buffer using the binary format described in the file header. */
  serialize(): Buffer {
    // Compute total size first
    const encoder = new TextEncoder();
    const termBytes = this.entries.map((e) => encoder.encode(e.term));
    // 4 (count) + sum of (2 + termLen + 4 + 4 + 4) per entry
    let totalSize = 4;
    for (const tb of termBytes) totalSize += 2 + tb.length + 12;

    const buf = Buffer.allocUnsafe(totalSize);
    let offset = 0;

    buf.writeUInt32LE(this.entries.length, offset); offset += 4;

    for (let i = 0; i < this.entries.length; i++) {
      const e = this.entries[i];
      const tb = termBytes[i];
      buf.writeUInt16LE(tb.length, offset); offset += 2;
      buf.set(tb, offset); offset += tb.length;
      buf.writeUInt32LE(e.postingsOffset, offset); offset += 4;
      buf.writeUInt32LE(e.postingsLength, offset); offset += 4;
      buf.writeUInt32LE(e.df, offset); offset += 4;
    }

    return buf;
  }

  /** Deserialize from a Buffer produced by serialize(). */
  static deserialize(buf: Buffer): TermDict {
    const dict = new TermDict();
    let offset = 0;

    const count = buf.readUInt32LE(offset); offset += 4;
    const decoder = new TextDecoder();

    for (let i = 0; i < count; i++) {
      const termLen = buf.readUInt16LE(offset); offset += 2;
      const term = decoder.decode(buf.subarray(offset, offset + termLen)); offset += termLen;
      const postingsOffset = buf.readUInt32LE(offset); offset += 4;
      const postingsLength = buf.readUInt32LE(offset); offset += 4;
      const df = buf.readUInt32LE(offset); offset += 4;
      dict.entries.push({ term, postingsOffset, postingsLength, df });
    }

    return dict;
  }

  /**
   * Build a sorted TermDict from an unsorted map of term → entry data.
   * Used by the segment writer which accumulates postings in insertion order.
   */
  static fromMap(map: Map<string, { postingsOffset: number; postingsLength: number; df: number }>): TermDict {
    const dict = new TermDict();
    const sorted = [...map.entries()].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    for (const [term, data] of sorted) {
      dict.add({ term, ...data });
    }
    return dict;
  }
}
