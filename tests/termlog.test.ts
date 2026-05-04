/**
 * TermLog facade tests — string docId mapping, tokenization, BM25 search, persistence.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { rm, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { writeFile } from "node:fs/promises";
import { TermLog } from "../src/termlog.js";
import { FsBackend } from "../src/storage.js";

async function makeTmpDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "termlog-facade-"));
}

let dir: string;
let backend: FsBackend;

beforeEach(async () => {
  dir = await makeTmpDir();
  backend = new FsBackend(dir);
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe("TermLog facade", () => {
  it("add + search round-trip with string ids", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl.add("doc-a", "the quick brown fox");
    await tl.add("doc-b", "the lazy dog");
    await tl.flush();

    const results = await tl.search("fox");
    expect(results.map((r) => r.docId)).toContain("doc-a");
    expect(results.find((r) => r.docId === "doc-b")).toBeUndefined();
  });

  it("remove then search excludes the removed doc", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl.add("doc-a", "the quick brown fox");
    await tl.add("doc-b", "the quick brown fox");
    await tl.flush();

    await tl.remove("doc-a");
    await tl.flush();

    const results = await tl.search("fox");
    expect(results.map((r) => r.docId)).not.toContain("doc-a");
    expect(results.map((r) => r.docId)).toContain("doc-b");
  });

  it("reopen restores docId mapping; ids assigned before close still resolve correctly", async () => {
    const tl1 = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl1.add("alpha", "hello world");
    await tl1.add("beta", "hello world");
    await tl1.close();

    const tl2 = await TermLog.open({ dir, backend, flushThreshold: 100 });
    const results = await tl2.search("hello");
    const ids = results.map((r) => r.docId).sort();
    expect(ids).toEqual(["alpha", "beta"]);
  });

  it("concurrent add() calls are serialized — no duplicate docIds or lost writes", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await Promise.all([
      tl.add("x", "apple banana"),
      tl.add("y", "apple cherry"),
      tl.add("z", "banana cherry"),
    ]);
    await tl.flush();

    const appleHits = await tl.search("apple");
    const ids = appleHits.map((r) => r.docId).sort();
    expect(ids).toContain("x");
    expect(ids).toContain("y");
    expect(ids).not.toContain("z");
  });

  it("close() flushes pending writes — reopened TermLog finds them", async () => {
    const tl1 = await TermLog.open({ dir, backend, flushThreshold: 1000 });
    await tl1.add("persist-me", "unique term xyzzy");
    // close() must flush without explicit flush call
    await tl1.close();

    const tl2 = await TermLog.open({ dir, backend, flushThreshold: 1000 });
    const results = await tl2.search("xyzzy");
    expect(results.map((r) => r.docId)).toContain("persist-me");
  });

  it("docCount() and segmentCount() reflect state", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 2 });
    expect(tl.docCount()).toBe(0);
    expect(tl.segmentCount()).toBe(0);

    await tl.add("a", "foo");
    await tl.add("b", "bar");
    // auto-flush should have triggered at threshold=2
    expect(tl.segmentCount()).toBeGreaterThan(0);
  });

  it("search returns results in score-desc order", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    // doc-heavy has "fox" twice; doc-light has it once — heavy should score higher
    await tl.add("heavy", "fox fox fox");
    await tl.add("light", "fox");
    await tl.flush();

    const results = await tl.search("fox");
    expect(results[0].docId).toBe("heavy");
    expect(results[0].score).toBeGreaterThan(results[1].score);
  });

  it("remove a doc not in the index is a no-op", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl.add("doc-a", "hello world");
    await tl.flush();

    await expect(tl.remove("nonexistent")).resolves.not.toThrow();
    const results = await tl.search("hello");
    expect(results.map((r) => r.docId)).toContain("doc-a");
  });

  it("MappingCorruptionError when docids.json is invalid JSON", async () => {
    // Create a valid index, then corrupt docids.json before reopening.
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl.add("x", "hello");
    await tl.close();

    // Overwrite docids.json with invalid content.
    await writeFile(join(dir, "docids.json"), "not valid json");

    await expect(TermLog.open({ dir, backend, flushThreshold: 100 }))
      .rejects.toMatchObject({ name: "MappingCorruptionError" });
  });
});
