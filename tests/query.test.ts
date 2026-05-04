import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentWriter, SegmentReader } from "../src/segment.js";
import { FsBackend } from "../src/storage.js";
import { SegmentPostingIter, MultiSegmentIter, andQuery, orQuery } from "../src/query.js";

let dir: string;
let backend: FsBackend;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-query-"));
  backend = new FsBackend(dir);
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

/** Write a segment from a list of (docId, term, tf) triples. */
async function writeSegment(
  id: string,
  entries: Array<[number, string, number]>,
  docLens?: Map<number, number>,
): Promise<SegmentReader> {
  // Accumulate per-term postings.
  const termMap = new Map<string, { docIds: number[]; tfs: number[] }>();
  for (const [docId, term, tf] of entries) {
    let e = termMap.get(term);
    if (!e) { e = { docIds: [], tfs: [] }; termMap.set(term, e); }
    e.docIds.push(docId);
    e.tfs.push(tf);
  }
  // Sort docIds within each term.
  for (const e of termMap.values()) {
    const pairs = e.docIds.map((d, i) => [d, e.tfs[i]] as [number, number]);
    pairs.sort((a, b) => a[0] - b[0]);
    e.docIds = pairs.map((p) => p[0]);
    e.tfs = pairs.map((p) => p[1]);
  }

  const stream = await backend.createWriteStream(`${id}.seg`);
  const writer = new SegmentWriter(stream);

  if (docLens) {
    for (const [docId, len] of docLens) writer.setDocLength(docId, len);
  } else {
    const seen = new Set(entries.map(([d]) => d));
    for (const d of seen) writer.setDocLength(d, 1);
  }

  for (const term of [...termMap.keys()].sort()) {
    const { docIds, tfs } = termMap.get(term)!;
    await writer.writeTerm(term, docIds, tfs);
  }
  await writer.finish();
  return SegmentReader.open(`${id}.seg`, backend);
}

// ---------------------------------------------------------------------------
// SegmentPostingIter
// ---------------------------------------------------------------------------

describe("SegmentPostingIter", () => {
  it("iterates through all postings in order", async () => {
    const seg = await writeSegment("s0", [[0, "cat", 1], [1, "cat", 2], [2, "cat", 3]]);
    const it = new SegmentPostingIter(seg.postings("cat"));
    expect(it.docId).toBe(0); expect(it.tf).toBe(1);
    it.advance(); expect(it.docId).toBe(1); expect(it.tf).toBe(2);
    it.advance(); expect(it.docId).toBe(2); expect(it.tf).toBe(3);
    it.advance(); expect(it.isExhausted).toBe(true);
  });

  it("empty posting list is immediately exhausted", async () => {
    const seg = await writeSegment("s0", [[0, "other", 1]]);
    const it = new SegmentPostingIter(seg.postings("missing"));
    expect(it.isExhausted).toBe(true);
    expect(it.docId).toBe(null);
  });

  it("seek skips to target docId", async () => {
    const seg = await writeSegment("s0", [[0, "t", 1], [1, "t", 1], [5, "t", 3], [9, "t", 2]]);
    const it = new SegmentPostingIter(seg.postings("t"));
    it.seek(5);
    expect(it.docId).toBe(5); expect(it.tf).toBe(3);
  });

  it("seek to docId between entries lands on next available", async () => {
    const seg = await writeSegment("s0", [[2, "t", 1], [7, "t", 1], [10, "t", 1]]);
    const it = new SegmentPostingIter(seg.postings("t"));
    it.seek(5);
    expect(it.docId).toBe(7);
  });

  it("seek past end exhausts iterator", async () => {
    const seg = await writeSegment("s0", [[0, "t", 1], [1, "t", 1]]);
    const it = new SegmentPostingIter(seg.postings("t"));
    it.seek(100);
    expect(it.isExhausted).toBe(true);
  });

  it("advance on exhausted iter is a no-op", async () => {
    const seg = await writeSegment("s0", [[0, "t", 1]]);
    const it = new SegmentPostingIter(seg.postings("t"));
    it.advance(); // now exhausted
    it.advance(); // no-op
    expect(it.isExhausted).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// MultiSegmentIter
// ---------------------------------------------------------------------------

describe("MultiSegmentIter", () => {
  it("single segment — behaves like SegmentPostingIter", async () => {
    const seg = await writeSegment("s0", [[0, "dog", 1], [3, "dog", 2]]);
    const it = new MultiSegmentIter("dog", [seg]);
    expect(it.currentDocId).toBe(0);
    let r = it.next(); expect(r?.docId).toBe(0); expect(r?.tf).toBe(1);
    r = it.next(); expect(r?.docId).toBe(3); expect(r?.tf).toBe(2);
    expect(it.isExhausted).toBe(true);
  });

  it("merges two segments in docId order", async () => {
    const s0 = await writeSegment("s0", [[1, "w", 1], [5, "w", 2]]);
    const s1 = await writeSegment("s1", [[2, "w", 3], [4, "w", 1]]);
    const it = new MultiSegmentIter("w", [s0, s1]);
    const results: number[] = [];
    let r = it.next();
    while (r) { results.push(r.docId); r = it.next(); }
    expect(results).toEqual([1, 2, 4, 5]);
  });

  it("missing term in all segments is immediately exhausted", async () => {
    const seg = await writeSegment("s0", [[0, "x", 1]]);
    const it = new MultiSegmentIter("missing", [seg]);
    expect(it.isExhausted).toBe(true);
    expect(it.next()).toBe(null);
  });

  it("seek advances multi-segment position", async () => {
    const s0 = await writeSegment("s0", [[0, "w", 1], [3, "w", 1]]);
    const s1 = await writeSegment("s1", [[1, "w", 2], [4, "w", 3]]);
    const it = new MultiSegmentIter("w", [s0, s1]);
    it.seek(3);
    expect(it.currentDocId).toBe(3);
    const r = it.next(); expect(r?.docId).toBe(3);
  });

  it("no segments returns exhausted iter", async () => {
    const it = new MultiSegmentIter("x", []);
    expect(it.isExhausted).toBe(true);
    expect(it.next()).toBe(null);
  });
});

// ---------------------------------------------------------------------------
// andQuery — zigzag merge
// ---------------------------------------------------------------------------

describe("andQuery", () => {
  it("intersection of two terms in one segment", async () => {
    const seg = await writeSegment("s0", [
      [0, "cat", 1], [1, "cat", 1], [2, "cat", 1],
      [1, "dog", 2], [2, "dog", 2], [3, "dog", 2],
    ]);
    const results = [...andQuery(["cat", "dog"], [seg])];
    const ids = results.map((r) => r.docId);
    expect(ids).toEqual([1, 2]);
    // tfs are present for both terms
    expect(results[0].tfs.get("cat")).toBe(1);
    expect(results[0].tfs.get("dog")).toBe(2);
  });

  it("empty intersection returns nothing", async () => {
    const seg = await writeSegment("s0", [
      [0, "a", 1], [1, "b", 1],
    ]);
    const results = [...andQuery(["a", "b"], [seg])];
    expect(results).toHaveLength(0);
  });

  it("single term AND returns all docs with that term", async () => {
    const seg = await writeSegment("s0", [[0, "x", 1], [1, "x", 2]]);
    const results = [...andQuery(["x"], [seg])];
    expect(results.map((r) => r.docId)).toEqual([0, 1]);
  });

  it("AND across multiple segments", async () => {
    const s0 = await writeSegment("s0", [[0, "a", 1], [1, "a", 1]]);
    const s1 = await writeSegment("s1", [[1, "b", 2], [2, "b", 1]]);
    // "a" is in doc 0 and 1; "b" is in doc 1 and 2 → intersection = {1}
    const results = [...andQuery(["a", "b"], [s0, s1])];
    expect(results.map((r) => r.docId)).toEqual([1]);
    expect(results[0].tfs.get("a")).toBe(1);
    expect(results[0].tfs.get("b")).toBe(2);
  });

  it("AND with three terms", async () => {
    const seg = await writeSegment("s0", [
      [0, "a", 1], [1, "a", 1], [2, "a", 1],
      [1, "b", 1], [2, "b", 1],
      [2, "c", 1],
    ]);
    const results = [...andQuery(["a", "b", "c"], [seg])];
    expect(results.map((r) => r.docId)).toEqual([2]);
  });

  it("no terms returns nothing", async () => {
    const seg = await writeSegment("s0", [[0, "x", 1]]);
    expect([...andQuery([], [seg])]).toHaveLength(0);
  });

  it("no segments returns nothing", async () => {
    expect([...andQuery(["x"], [])]).toHaveLength(0);
  });

  it("results are in ascending docId order", async () => {
    const s0 = await writeSegment("s0", [[5, "t", 1], [10, "t", 1], [15, "t", 1]]);
    const s1 = await writeSegment("s1", [[5, "u", 1], [10, "u", 1], [15, "u", 1]]);
    const ids = [...andQuery(["t", "u"], [s0, s1])].map((r) => r.docId);
    expect(ids).toEqual([5, 10, 15]);
  });
});

// ---------------------------------------------------------------------------
// orQuery — k-way union
// ---------------------------------------------------------------------------

describe("orQuery", () => {
  it("union of two terms in one segment", async () => {
    const seg = await writeSegment("s0", [
      [0, "cat", 1], [2, "cat", 1],
      [1, "dog", 2], [2, "dog", 2],
    ]);
    const results = [...orQuery(["cat", "dog"], [seg])];
    const ids = results.map((r) => r.docId);
    expect(ids).toEqual([0, 1, 2]);
    // doc 2 has both terms
    const doc2 = results.find((r) => r.docId === 2)!;
    expect(doc2.tfs.get("cat")).toBe(1);
    expect(doc2.tfs.get("dog")).toBe(2);
  });

  it("union of disjoint term sets", async () => {
    const seg = await writeSegment("s0", [[0, "a", 3], [1, "b", 5]]);
    const results = [...orQuery(["a", "b"], [seg])];
    expect(results.map((r) => r.docId)).toEqual([0, 1]);
    expect(results[0].tfs.get("a")).toBe(3);
    expect(results[1].tfs.get("b")).toBe(5);
  });

  it("OR across multiple segments", async () => {
    const s0 = await writeSegment("s0", [[0, "x", 1], [2, "x", 1]]);
    const s1 = await writeSegment("s1", [[1, "y", 1], [2, "y", 1]]);
    const ids = [...orQuery(["x", "y"], [s0, s1])].map((r) => r.docId);
    expect(ids).toEqual([0, 1, 2]);
  });

  it("single term OR returns all docs with that term", async () => {
    const seg = await writeSegment("s0", [[3, "q", 1], [7, "q", 2]]);
    const ids = [...orQuery(["q"], [seg])].map((r) => r.docId);
    expect(ids).toEqual([3, 7]);
  });

  it("missing term contributes nothing to union", async () => {
    const seg = await writeSegment("s0", [[0, "real", 1]]);
    const results = [...orQuery(["real", "ghost"], [seg])];
    expect(results.map((r) => r.docId)).toEqual([0]);
    expect(results[0].tfs.get("real")).toBe(1);
    expect(results[0].tfs.has("ghost")).toBe(false);
  });

  it("no terms returns nothing", async () => {
    const seg = await writeSegment("s0", [[0, "x", 1]]);
    expect([...orQuery([], [seg])]).toHaveLength(0);
  });

  it("no segments returns nothing", async () => {
    expect([...orQuery(["x"], [])]).toHaveLength(0);
  });

  it("results are in ascending docId order", async () => {
    const s0 = await writeSegment("s0", [[3, "a", 1], [8, "a", 1]]);
    const s1 = await writeSegment("s1", [[1, "b", 1], [5, "b", 1]]);
    const ids = [...orQuery(["a", "b"], [s0, s1])].map((r) => r.docId);
    expect(ids).toEqual([1, 3, 5, 8]);
  });
});
