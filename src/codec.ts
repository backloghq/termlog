/**
 * Posting list codec — VByte encoding + delta-encoded doc IDs.
 *
 * Wire format for a posting list:
 *   vbyte(count) | [vbyte(delta_i) vbyte(tf_i)] * count
 *
 * Doc IDs are delta-encoded: d[0] stored directly, d[i] stored as (d[i] - d[i-1]).
 * Deltas and tf values are encoded with variable-length bytes (7 bits per byte,
 * MSB is continuation bit).
 */

/** Encode a non-negative integer as VByte into a pre-allocated byte array.
 *  Returns the number of bytes written. Uses Math.floor division to handle
 *  values beyond 32-bit range (avoids signed truncation from bitwise ops). */
function writeVByte(out: number[], n: number): number {
  let written = 0;
  while (n >= 0x80) {
    out.push((n & 0x7f) | 0x80);
    n = Math.floor(n / 128);
    written++;
  }
  out.push(n & 0x7f);
  return written + 1;
}

/** Encode a single non-negative integer as a VByte Buffer. */
export function encodeVByte(n: number): Buffer {
  if (n < 0 || !Number.isInteger(n)) throw new RangeError(`encodeVByte: n must be a non-negative integer, got ${n}`);
  const bytes: number[] = [];
  writeVByte(bytes, n);
  return Buffer.from(bytes);
}

/** Decode a single VByte integer from buf starting at offset.
 *  Returns the decoded value and number of bytes consumed.
 *  Uses multiplication for high bits to avoid signed-integer overflow from JS
 *  bitwise ops (which are capped at signed 32-bit). Supports values up to 2^35-1. */
export function decodeVByte(buf: Buffer, offset: number): { value: number; bytesRead: number } {
  let value = 0;
  let shift = 0;
  let bytesRead = 0;
  while (offset + bytesRead < buf.length) {
    const byte = buf[offset + bytesRead];
    bytesRead++;
    const bits = byte & 0x7f;
    // Use multiplication for shifts >= 28 to avoid signed 32-bit overflow.
    if (shift < 28) {
      value |= bits << shift;
    } else {
      value += bits * Math.pow(2, shift);
    }
    shift += 7;
    if ((byte & 0x80) === 0) break;
    if (shift >= 49) throw new RangeError("decodeVByte: value too large");
  }
  return { value, bytesRead };
}

/**
 * Encode a posting list as a Buffer.
 * @param docIds  Sorted, non-negative doc IDs (must be strictly increasing).
 * @param tfs     Parallel term-frequency array; tfs[i] corresponds to docIds[i].
 */
export function encodePostings(docIds: number[], tfs: number[]): Buffer {
  if (docIds.length !== tfs.length) {
    throw new RangeError(`encodePostings: docIds.length (${docIds.length}) !== tfs.length (${tfs.length})`);
  }
  const bytes: number[] = [];
  writeVByte(bytes, docIds.length);
  let prev = 0;
  for (let i = 0; i < docIds.length; i++) {
    const delta = docIds[i] - prev;
    if (delta < 0) throw new RangeError(`encodePostings: doc IDs must be strictly increasing; got ${docIds[i]} after ${prev}`);
    writeVByte(bytes, delta);
    writeVByte(bytes, tfs[i]);
    prev = docIds[i];
  }
  return Buffer.from(bytes);
}

/**
 * Fully decode a posting list buffer — materializes all entries.
 * Used in tests and for low-frequency bulk access.
 */
export function decodePostings(buf: Buffer): { docIds: number[]; tfs: number[] } {
  let offset = 0;

  const countResult = decodeVByte(buf, offset);
  offset += countResult.bytesRead;
  const count = countResult.value;

  const docIds: number[] = new Array(count);
  const tfs: number[] = new Array(count);
  let prev = 0;

  for (let i = 0; i < count; i++) {
    const deltaResult = decodeVByte(buf, offset);
    offset += deltaResult.bytesRead;
    prev += deltaResult.value;
    docIds[i] = prev;

    const tfResult = decodeVByte(buf, offset);
    offset += tfResult.bytesRead;
    tfs[i] = tfResult.value;
  }

  return { docIds, tfs };
}

export interface Posting {
  docId: number;
  tf: number;
}

/**
 * Lazy posting iterator — decodes one entry at a time.
 * Suitable for the query path where early termination is common.
 */
export function postingIterator(buf: Buffer): Iterator<Posting> {
  let offset = 0;

  const countResult = decodeVByte(buf, offset);
  offset += countResult.bytesRead;
  let remaining = countResult.value;
  let prev = 0;

  return {
    next(): IteratorResult<Posting> {
      if (remaining === 0) return { done: true, value: undefined as unknown as Posting };

      const deltaResult = decodeVByte(buf, offset);
      offset += deltaResult.bytesRead;
      prev += deltaResult.value;
      const docId = prev;

      const tfResult = decodeVByte(buf, offset);
      offset += tfResult.bytesRead;
      const tf = tfResult.value;

      remaining--;
      return { done: false, value: { docId, tf } };
    },
  };
}
