import { describe, it, expect } from "vitest";
import { encodeVByte, decodeVByte, encodePostings, decodePostings, postingIterator } from "../src/codec.js";

describe("encodeVByte / decodeVByte", () => {
  it("round-trips small values (< 128)", () => {
    for (const n of [0, 1, 63, 127]) {
      const buf = encodeVByte(n);
      expect(buf.length).toBe(1);
      const { value, bytesRead } = decodeVByte(buf, 0);
      expect(value).toBe(n);
      expect(bytesRead).toBe(1);
    }
  });

  it("round-trips two-byte values (128–16383)", () => {
    for (const n of [128, 255, 1000, 16383]) {
      const buf = encodeVByte(n);
      expect(buf.length).toBe(2);
      expect(decodeVByte(buf, 0).value).toBe(n);
    }
  });

  it("round-trips large values (> 2^21)", () => {
    for (const n of [0x200000, 0xfffffff, 2 ** 28 + 7, 2 ** 35 - 1]) {
      const buf = encodeVByte(n);
      expect(decodeVByte(buf, 0).value).toBe(n);
    }
  });

  it("decodeVByte respects offset into buffer", () => {
    const a = encodeVByte(300);
    const b = encodeVByte(42);
    const combined = Buffer.concat([a, b]);
    const r1 = decodeVByte(combined, 0);
    expect(r1.value).toBe(300);
    const r2 = decodeVByte(combined, r1.bytesRead);
    expect(r2.value).toBe(42);
  });

  it("throws on negative input", () => {
    expect(() => encodeVByte(-1)).toThrow();
  });
});

describe("encodePostings / decodePostings — round-trip", () => {
  it("empty list round-trips", () => {
    const buf = encodePostings([], []);
    const { docIds, tfs } = decodePostings(buf);
    expect(docIds).toEqual([]);
    expect(tfs).toEqual([]);
  });

  it("single entry round-trips", () => {
    const buf = encodePostings([42], [3]);
    const { docIds, tfs } = decodePostings(buf);
    expect(docIds).toEqual([42]);
    expect(tfs).toEqual([3]);
  });

  it("multiple entries round-trip", () => {
    const ids = [10, 12, 100, 500];
    const fs = [1, 5, 2, 10];
    const buf = encodePostings(ids, fs);
    const { docIds, tfs } = decodePostings(buf);
    expect(docIds).toEqual(ids);
    expect(tfs).toEqual(fs);
  });

  it("delta math: [10, 12, 100] encodes deltas [10, 2, 88]", () => {
    const buf = encodePostings([10, 12, 100], [1, 1, 1]);
    // Skip count byte (1 byte for value 3), then read three delta+tf pairs
    let offset = 0;
    const { bytesRead: cbr } = decodeVByte(buf, 0);
    offset += cbr;
    const deltas: number[] = [];
    for (let i = 0; i < 3; i++) {
      const { value: delta, bytesRead: dbr } = decodeVByte(buf, offset);
      offset += dbr;
      deltas.push(delta);
      const { bytesRead: fbr } = decodeVByte(buf, offset); // skip tf
      offset += fbr;
    }
    expect(deltas).toEqual([10, 2, 88]);
  });

  it("large doc IDs (> 2^28) round-trip", () => {
    const ids = [2 ** 28, 2 ** 28 + 1, 2 ** 28 + 1000, 2 ** 34];
    const fs = [1, 2, 3, 4];
    const { docIds, tfs } = decodePostings(encodePostings(ids, fs));
    expect(docIds).toEqual(ids);
    expect(tfs).toEqual(fs);
  });

  it("tf = 0 round-trips", () => {
    const { docIds, tfs } = decodePostings(encodePostings([1, 2, 3], [0, 0, 0]));
    expect(tfs).toEqual([0, 0, 0]);
    expect(docIds).toEqual([1, 2, 3]);
  });

  it("tf = 65535 round-trips", () => {
    const { tfs } = decodePostings(encodePostings([7], [65535]));
    expect(tfs).toEqual([65535]);
  });

  it("throws when doc IDs are not strictly increasing", () => {
    expect(() => encodePostings([10, 5], [1, 1])).toThrow();
  });

  it("throws when docIds and tfs lengths differ", () => {
    expect(() => encodePostings([1, 2], [1])).toThrow();
  });

  it("size sanity: 1000 sequential doc IDs + tf=1 fits in ~2010 bytes", () => {
    const ids = Array.from({ length: 1000 }, (_, i) => i);
    const fs = new Array(1000).fill(1);
    const buf = encodePostings(ids, fs);
    // Each posting: delta=1 (1 byte) + tf=1 (1 byte) = 2 bytes, plus count vbyte
    expect(buf.length).toBeLessThanOrEqual(2010);
  });
});

describe("postingIterator", () => {
  it("iterates same data as decodePostings", () => {
    const ids = [5, 10, 200, 1000];
    const fs = [3, 1, 7, 2];
    const buf = encodePostings(ids, fs);

    const iter = postingIterator(buf);
    const results: Array<{ docId: number; tf: number }> = [];
    for (let r = iter.next(); !r.done; r = iter.next()) {
      results.push(r.value);
    }
    expect(results.map((r) => r.docId)).toEqual(ids);
    expect(results.map((r) => r.tf)).toEqual(fs);
  });

  it("empty list: iterator is immediately done", () => {
    const buf = encodePostings([], []);
    const iter = postingIterator(buf);
    expect(iter.next().done).toBe(true);
  });

  it("partial advance: consuming first N entries works correctly", () => {
    const ids = [1, 2, 3, 4, 5];
    const fs = [10, 20, 30, 40, 50];
    const buf = encodePostings(ids, fs);
    const iter = postingIterator(buf);

    // Consume first 3
    const first3: number[] = [];
    for (let i = 0; i < 3; i++) {
      const r = iter.next();
      expect(r.done).toBe(false);
      first3.push(r.value.docId);
    }
    expect(first3).toEqual([1, 2, 3]);

    // 4th entry
    const fourth = iter.next();
    expect(fourth.done).toBe(false);
    expect(fourth.value.docId).toBe(4);
    expect(fourth.value.tf).toBe(40);
  });

  it("iterator is done after all entries consumed", () => {
    const buf = encodePostings([1], [1]);
    const iter = postingIterator(buf);
    iter.next(); // consume
    expect(iter.next().done).toBe(true);
  });

  it("randomized round-trip: 1000 iterations of random sorted docId arrays and tfs", () => {
    const RNG_SEED = 42;
    let seed = RNG_SEED;
    const rand = () => {
      seed = (seed * 1664525 + 1013904223) & 0xffffffff;
      return (seed >>> 0) / 0x100000000;
    };

    for (let iter = 0; iter < 1000; iter++) {
      const count = Math.floor(rand() * 20) + 1; // 1..20 entries
      const docIds: number[] = [];
      let last = 0;
      for (let i = 0; i < count; i++) {
        last += Math.floor(rand() * 1000) + 1; // strictly increasing
        docIds.push(last);
      }
      const tfs = docIds.map(() => Math.floor(rand() * 255) + 1);

      const buf = encodePostings(docIds, tfs);
      const decoded = decodePostings(buf);

      expect(decoded.docIds).toHaveLength(count);
      expect(decoded.tfs).toHaveLength(count);
      for (let i = 0; i < count; i++) {
        expect(decoded.docIds[i]).toBe(docIds[i]);
        expect(decoded.tfs[i]).toBe(tfs[i]);
      }
    }
  });
});
