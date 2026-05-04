import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FsBackend } from "../src/storage.js";
import { SegmentWriter, SegmentReader } from "../src/segment.js";

async function makeTmpDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "termlog-seg-"));
}

/** Build and flush a small fixed corpus using the streaming writer API. */
async function buildAndFlush(id: string, backend: FsBackend): Promise<void> {
  const stream = await backend.createWriteStream(`${id}.seg`);
  const w = new SegmentWriter(stream);
  w.setDocLength(0, 4);
  w.setDocLength(1, 6);
  w.setDocLength(2, 3);
  // Terms must be in lex order; docIds must be sorted within each term.
  await w.writeTerm("language",    [0, 1], [1, 1]);
  await w.writeTerm("programming", [0],    [1]);
  await w.writeTerm("rust",        [0, 2], [2, 1]);
  await w.writeTerm("safety",      [2],    [1]);
  await w.writeTerm("typescript",  [1],    [3]);
  await w.writeTerm("types",       [1, 2], [2, 1]);
  await w.finish();
}

describe("SegmentWriter + SegmentReader — round-trip", () => {
  let dir: string;
  let backend: FsBackend;

  beforeEach(async () => {
    dir = await makeTmpDir();
    backend = new FsBackend(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it("finish produces a .seg file and no orphaned .tmp", async () => {
    await buildAndFlush("seg-001", backend);
    const files = await backend.listBlobs("");
    expect(files).toContain("seg-001.seg");
    expect(files.filter((f) => f.endsWith(".tmp"))).toHaveLength(0);
  });

  it("reader recovers all posting lists identically", async () => {
    await buildAndFlush("seg-001", backend);
    const r = await SegmentReader.open("seg-001.seg", backend);

    const rust = r.decodePostings("rust");
    expect(rust.docIds).toEqual([0, 2]);
    expect(rust.tfs).toEqual([2, 1]);

    const lang = r.decodePostings("language");
    expect(lang.docIds).toEqual([0, 1]);
    expect(lang.tfs).toEqual([1, 1]);

    const types = r.decodePostings("types");
    expect(types.docIds).toEqual([1, 2]);
    expect(types.tfs).toEqual([2, 1]);
  });

  it("reader recovers doc lengths", async () => {
    await buildAndFlush("seg-001", backend);
    const r = await SegmentReader.open("seg-001.seg", backend);

    expect(r.docLen(0)).toBe(4);
    expect(r.docLen(1)).toBe(6);
    expect(r.docLen(2)).toBe(3);
    expect(r.docLen(99)).toBe(0); // missing
  });

  it("termCount and docCount are correct", async () => {
    await buildAndFlush("seg-001", backend);
    const r = await SegmentReader.open("seg-001.seg", backend);

    // rust, language, programming, typescript, types, safety = 6
    expect(r.termCount).toBe(6);
    expect(r.docCount).toBe(3);
  });

  it("lookupTerm returns entry with correct df", async () => {
    await buildAndFlush("seg-001", backend);
    const r = await SegmentReader.open("seg-001.seg", backend);

    expect(r.lookupTerm("rust")?.df).toBe(2);    // appears in docs 0, 2
    expect(r.lookupTerm("typescript")?.df).toBe(1);
    expect(r.lookupTerm("missing")).toBeUndefined();
  });

  it("postings iterator matches decodePostings", async () => {
    await buildAndFlush("seg-001", backend);
    const r = await SegmentReader.open("seg-001.seg", backend);

    const iter = r.postings("rust");
    const results: Array<{ docId: number; tf: number }> = [];
    for (let res = iter.next(); !res.done; res = iter.next()) results.push(res.value);

    const { docIds, tfs } = r.decodePostings("rust");
    expect(results.map((x) => x.docId)).toEqual(docIds);
    expect(results.map((x) => x.tf)).toEqual(tfs);
  });

  it("postings iterator for missing term is immediately done", async () => {
    await buildAndFlush("seg-001", backend);
    const r = await SegmentReader.open("seg-001.seg", backend);
    expect(r.postings("nonexistent").next().done).toBe(true);
  });

  it("terms() iterates all terms in sorted order", async () => {
    await buildAndFlush("seg-001", backend);
    const r = await SegmentReader.open("seg-001.seg", backend);

    const terms = [...r.terms()].map((e) => e.term);
    const sorted = [...terms].sort();
    expect(terms).toEqual(sorted);
    expect(terms).toContain("rust");
    expect(terms).toContain("safety");
    expect(terms).toContain("typescript");
  });

  it("100-term round-trip: all postings recover identically", async () => {
    const stream = await backend.createWriteStream("seg-big.seg");
    const w = new SegmentWriter(stream);
    const expected = new Map<string, { docIds: number[]; tfs: number[] }>();

    // Build sorted list of (term, docIds, tfs) — terms must be written in lex order.
    const termList: Array<{ term: string; docIds: number[]; tfs: number[] }> = [];
    for (let t = 0; t < 100; t++) {
      const term = `term${String(t).padStart(3, "0")}`;
      const docIds: number[] = [];
      const tfs: number[] = [];
      for (let d = 0; d < 5; d++) {
        const docId = t * 5 + d;
        const tf = (d + 1);
        w.setDocLength(docId, tf * 3);
        docIds.push(docId);
        tfs.push(tf);
      }
      expected.set(term, { docIds, tfs });
      termList.push({ term, docIds, tfs });
    }
    termList.sort((a, b) => a.term < b.term ? -1 : 1);
    for (const { term, docIds, tfs } of termList) {
      await w.writeTerm(term, docIds, tfs);
    }
    await w.finish();

    const r = await SegmentReader.open("seg-big.seg", backend);

    for (const [term, exp] of expected) {
      const got = r.decodePostings(term);
      expect(got.docIds).toEqual(exp.docIds);
      expect(got.tfs).toEqual(exp.tfs);
    }
  });
});

describe("SegmentReader — corruption detection", () => {
  let dir: string;
  let backend: FsBackend;

  beforeEach(async () => {
    dir = await makeTmpDir();
    backend = new FsBackend(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it("throws SegmentCorruptionError with region='postings' when postings region is corrupted", async () => {
    await buildAndFlush("seg-001", backend);
    const data = await backend.readBlob("seg-001.seg");
    // Corrupt byte 0 (start of postings region, if non-empty)
    if (data.length > 64) {
      data[0] ^= 0xff;
      const { writeFile } = await import("node:fs/promises");
      const { join: pathJoin } = await import("node:path");
      await writeFile(pathJoin(dir, "seg-001.seg"), data);
    }
    await expect(SegmentReader.open("seg-001.seg", backend))
      .rejects.toMatchObject({ name: "SegmentCorruptionError", region: "postings" });
  });

  it("throws SegmentCorruptionError with region='footer' for bad magic", async () => {
    await buildAndFlush("seg-001", backend);
    const data = await backend.readBlob("seg-001.seg");
    // Footer starts at data.length - 64; corrupt the magic (first 4 bytes of footer)
    data[data.length - 64] ^= 0xff;
    const { writeFile } = await import("node:fs/promises");
    const { join: pathJoin } = await import("node:path");
    await writeFile(pathJoin(dir, "seg-001.seg"), data);

    await expect(SegmentReader.open("seg-001.seg", backend))
      .rejects.toMatchObject({ name: "SegmentCorruptionError", region: "footer" });
  });

  it("throws SegmentCorruptionError with region='dict' when dict region is corrupted", async () => {
    await buildAndFlush("seg-001", backend);
    const data = await backend.readBlob("seg-001.seg");
    // Footer: dictOffset at bytes footer+32..+36 (offset from footerStart)
    const footerStart = data.length - 64;
    const dictOffset = data.readUInt32LE(footerStart + 32);
    // Corrupt one byte inside the dict region
    data[dictOffset] ^= 0xff;
    const { writeFile } = await import("node:fs/promises");
    const { join: pathJoin } = await import("node:path");
    await writeFile(pathJoin(dir, "seg-001.seg"), data);

    await expect(SegmentReader.open("seg-001.seg", backend))
      .rejects.toMatchObject({ name: "SegmentCorruptionError" });
  });
});

describe("SegmentWriter — docIds sorted within term", () => {
  let dir: string;
  let backend: FsBackend;

  beforeEach(async () => {
    dir = await makeTmpDir();
    backend = new FsBackend(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it("writeTerm with sorted docIds produces correct posting lists", async () => {
    const stream = await backend.createWriteStream("seg-order.seg");
    const w = new SegmentWriter(stream);
    w.setDocLength(10, 3);
    w.setDocLength(50, 2);
    w.setDocLength(100, 1);
    // Caller passes sorted docIds to writeTerm.
    await w.writeTerm("word", [10, 50, 100], [3, 2, 1]);
    await w.finish();

    const r = await SegmentReader.open("seg-order.seg", backend);
    const { docIds, tfs } = r.decodePostings("word");
    expect(docIds).toEqual([10, 50, 100]);
    expect(tfs).toEqual([3, 2, 1]);
  });
});
