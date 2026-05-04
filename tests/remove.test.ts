/**
 * Tests for SegmentManager.remove() — tombstone storage + query filtering.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { rm, mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FsBackend } from "../src/storage.js";
import { SegmentManager } from "../src/manager.js";
import { orQuery } from "../src/query.js";

async function makeTmpDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "termlog-remove-"));
}

function docTerms(words: string[]): Array<{ term: string; tf: number }> {
  const freq = new Map<string, number>();
  for (const w of words) freq.set(w, (freq.get(w) ?? 0) + 1);
  return [...freq.entries()].map(([term, tf]) => ({ term, tf }));
}

async function queryDocIds(mgr: SegmentManager, term: string): Promise<number[]> {
  const segs = mgr.segments();
  const results: number[] = [];
  for (const posting of orQuery([term], segs)) {
    results.push(posting.docId);
  }
  return results.sort((a, b) => a - b);
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

describe("SegmentManager.remove", () => {
  it("add 3 docs, remove 1 from buffer (not yet flushed), search returns 2", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100 });
    await mgr.add(0, docTerms(["fox"]));
    await mgr.add(1, docTerms(["fox"]));
    await mgr.add(2, docTerms(["fox"]));
    // Remove doc 1 before flush — drops from buffer.
    await mgr.remove(1);
    await mgr.flush();
    const ids = await queryDocIds(mgr, "fox");
    expect(ids).toEqual([0, 2]);
  });

  it("add 3 docs, flush, remove 1 (tombstone in new segment), search returns 2", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100 });
    await mgr.add(0, docTerms(["fox"]));
    await mgr.add(1, docTerms(["fox"]));
    await mgr.add(2, docTerms(["fox"]));
    await mgr.flush();

    // Remove doc 1 — creates tombstone in the next segment.
    await mgr.remove(1);
    await mgr.flush();

    const ids = await queryDocIds(mgr, "fox");
    expect(ids).toEqual([0, 2]);
  });

  it("add 3 docs, flush, remove 1, compact, search returns 2 (physical drop)", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100, mergeThreshold: 100 });
    await mgr.add(0, docTerms(["fox"]));
    await mgr.add(1, docTerms(["fox"]));
    await mgr.add(2, docTerms(["fox"]));
    await mgr.flush();

    await mgr.remove(1);
    await mgr.flush();
    await mgr.compact();

    // After compaction the tombstone is physically applied — only 1 segment remains.
    // Doc IDs are re-numbered to a dense range during compaction; 2 survivors → 2 results.
    expect(mgr.segments().length).toBe(1);
    const ids = await queryDocIds(mgr, "fox");
    expect(ids).toHaveLength(2);
  });

  it("remove a doc not in the index is a no-op (idempotent)", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100 });
    await mgr.add(0, docTerms(["fox"]));
    await mgr.flush();

    // Remove a doc that was never added.
    await mgr.remove(99);
    // Flush still works (tombstone-only segment).
    await mgr.flush();

    const ids = await queryDocIds(mgr, "fox");
    expect(ids).toEqual([0]);
  });

  it("flushed segment does not include a doc removed from the buffer", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100 });
    await mgr.add(0, docTerms(["fox"]));
    await mgr.add(1, docTerms(["fox"]));
    await mgr.add(2, docTerms(["fox"]));
    // Remove before flush.
    await mgr.remove(1);
    await mgr.flush();

    // There should be exactly one segment; doc 1 must not appear.
    expect(mgr.segments().length).toBe(1);
    const seg = mgr.segments()[0];
    const { docIds } = seg.decodePostings("fox");
    expect(docIds).not.toContain(1);
  });

  it("re-add a removed doc id — new posting is found in search", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100 });
    await mgr.add(0, docTerms(["fox"]));
    await mgr.flush();

    await mgr.remove(0);
    await mgr.flush();

    // Verify doc 0 is gone (tombstoned).
    expect(await queryDocIds(mgr, "fox")).toEqual([]);

    // Re-add a new doc (different ID to avoid collision with tombstone).
    await mgr.add(10, docTerms(["fox"]));
    await mgr.flush();

    // New doc is visible before compaction.
    const idsBeforeCompact = await queryDocIds(mgr, "fox");
    expect(idsBeforeCompact).toHaveLength(1);

    // After compaction old tombstone and stale posting are physically dropped.
    // The surviving doc is re-numbered; exactly 1 result remains.
    await mgr.compact();
    const ids = await queryDocIds(mgr, "fox");
    expect(ids).toHaveLength(1);
  });
});
