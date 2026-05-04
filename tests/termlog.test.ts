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
import { VERSION } from "../src/index.js";

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

describe("VERSION export (#59ac681f)", () => {
  it("VERSION matches package.json version string", () => {
    expect(VERSION).toMatch(/^\d+\.\d+\.\d+/);
    expect(VERSION).toBe("0.1.0");
  });
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

  it("add() on existing docId replaces content — no double-counting (#9d32ce44)", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl.add("doc-a", "apple banana");
    await tl.flush();

    // Update in place with different content.
    await tl.add("doc-a", "cherry dragonfruit");
    await tl.flush();

    // Old terms must not appear.
    const appleHits = await tl.search("apple");
    expect(appleHits.map((r) => r.docId)).not.toContain("doc-a");

    // New terms must appear — and doc-a appears exactly once.
    const cherryHits = await tl.search("cherry");
    const docAHits = cherryHits.filter((r) => r.docId === "doc-a");
    expect(docAHits).toHaveLength(1);
  });

  it("remove-then-re-add same docId is searchable with new content (#42e94805)", async () => {
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl.add("doc-a", "apple banana");
    await tl.flush();

    await tl.remove("doc-a");
    await tl.flush();

    // Re-add with completely different content.
    await tl.add("doc-a", "cherry dragon");
    await tl.flush();

    // Old content must not be found.
    const appleHits = await tl.search("apple");
    expect(appleHits.map((r) => r.docId)).not.toContain("doc-a");

    // New content must be found.
    const cherryHits = await tl.search("cherry");
    expect(cherryHits.map((r) => r.docId)).toContain("doc-a");
  });

  it("MappingCorruptionError when docids.snap is invalid JSON", async () => {
    // Create a valid index, then corrupt docids.snap before reopening.
    const tl = await TermLog.open({ dir, backend, flushThreshold: 100 });
    await tl.add("x", "hello");
    await tl.close();

    // Overwrite docids.snap with invalid content.
    await writeFile(join(dir, "docids.snap"), "not valid json");

    await expect(TermLog.open({ dir, backend, flushThreshold: 100 }))
      .rejects.toMatchObject({ name: "MappingCorruptionError" });
  });
});

// ---------------------------------------------------------------------------
// BLOCKER 1 — facade round-trip integrity after compaction
//
// Verifies that compaction preserves original numIds so TermLog.search returns
// the correct string docId for every surviving document. Densification would
// break this because numToStr maps the original numId, not a renumbered one.
// ---------------------------------------------------------------------------
describe("TermLog — facade round-trip after compact", () => {
  it("search returns correct string docId for all docs after compact with removal", async () => {
    // One segment per add (flushThreshold=1), fanout=999 to suppress auto-compact.
    const tl = await TermLog.open({ dir, backend, flushThreshold: 1, fanout: 999 });
    for (let i = 0; i < 10; i++) {
      await tl.add(`doc-${i}`, `unique content for document number ${i} word${i}`);
    }

    // Remove doc-3 (creates a tombstone segment).
    await tl.remove("doc-3");
    await tl.flush();

    // Compact all segments into one.
    await tl.compact();

    // Every surviving doc must be searchable and return the correct string id.
    for (let i = 0; i < 10; i++) {
      if (i === 3) continue; // removed
      const results = await tl.search(`word${i}`);
      const ids = results.map((r) => r.docId);
      expect(ids, `doc-${i} not found after compact`).toContain(`doc-${i}`);
      expect(ids, `doc-3 resurected in search for word${i}`).not.toContain("doc-3");
    }
  });

  it("removed doc does not resurface after multi-tier cascade compact", async () => {
    // fanout=2 forces aggressive cascading.
    const tl = await TermLog.open({ dir, backend, flushThreshold: 1, fanout: 2 });
    for (let i = 0; i < 8; i++) {
      await tl.add(`doc-${i}`, `token${i} shared`);
    }
    await tl.remove("doc-5");
    await tl.flush();
    await tl.compact();

    const shared = await tl.search("shared");
    const ids = shared.map((r) => r.docId);
    expect(ids).not.toContain("doc-5");
    for (let i = 0; i < 8; i++) {
      if (i === 5) continue;
      expect(ids, `doc-${i} missing after compact`).toContain(`doc-${i}`);
    }
  });
});

// ---------------------------------------------------------------------------
// BLOCKER 2 — tombstone carry-forward across partial merges
//
// Verifies that when a tombstone in a merged segment targets a doc in an
// UNMERGED segment, the tombstone is preserved in the merged output so the
// doc is still excluded from queries after the partial merge.
// ---------------------------------------------------------------------------
describe("TermLog — tombstone carry-forward across partial merge", () => {
  it("tombstone targeting doc in unmerged segment is applied after partial compact", async () => {
    // Strategy: add docs so they land in specific segments, remove one, then
    // trigger a partial merge that does NOT include the segment holding the
    // tombstone's target doc. The tombstone must survive on the merged segment.
    //
    // With fanout=4 and flushThreshold=1:
    //   add doc-0..doc-3 → flush each → 4 tier-0 segs → cascade merges to 1 tier-1
    //   add doc-4..doc-7 → flush each → 4 tier-0 segs → cascade merges to 1 tier-1
    //   Now: [tier-1(docs 0-3), tier-1(docs 4-7)]
    //   remove doc-6 → tombstone goes into next flush
    //   add doc-8 → flushes, tombstone is in that tier-0 seg
    //   compact() merges everything
    //   → doc-6 must not appear in search results
    const tl = await TermLog.open({ dir, backend, flushThreshold: 1, fanout: 4 });

    for (let i = 0; i < 8; i++) {
      await tl.add(`doc-${i}`, `word${i} shared`);
    }

    await tl.remove("doc-6");
    await tl.add("doc-8", "word8 shared");

    await tl.compact();

    const results = await tl.search("shared");
    const ids = results.map((r) => r.docId);
    expect(ids).not.toContain("doc-6");

    // All other docs must be present.
    for (let i = 0; i < 9; i++) {
      if (i === 6) continue;
      expect(ids, `doc-${i} missing`).toContain(`doc-${i}`);
    }
  });
});
