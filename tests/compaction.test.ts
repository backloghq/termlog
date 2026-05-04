import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentManager } from "../src/manager.js";
import { FsBackend } from "../src/storage.js";

let dir: string;
let backend: FsBackend;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-compact-"));
  backend = new FsBackend(dir);
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

/** Build an index with n docs, one segment per doc (flushThreshold=1). */
async function buildIndex(n: number, mergeThreshold = 999): Promise<SegmentManager> {
  const mgr = await SegmentManager.open({ backend, flushThreshold: 1, mergeThreshold });
  for (let i = 0; i < n; i++) {
    await mgr.add(i, [{ term: "common", tf: 1 }, { term: `doc${i}`, tf: 2 }]);
  }
  return mgr;
}

/** Collect all docIds for a term across all segments in a manager. */
function collectDocIds(mgr: SegmentManager, term: string): number[] {
  const ids: number[] = [];
  for (const seg of mgr.segments()) {
    ids.push(...seg.decodePostings(term).docIds);
  }
  return ids;
}

describe("compact — basic correctness", () => {
  it("no-op on zero segments", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.compact();
    expect(mgr.segments()).toHaveLength(0);
    expect(mgr.commitGeneration()).toBe(0);
  });

  it("no-op on one segment", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "x", tf: 1 }]);
    const genBefore = mgr.commitGeneration();
    await mgr.compact();
    expect(mgr.segments()).toHaveLength(1);
    expect(mgr.commitGeneration()).toBe(genBefore); // unchanged
  });

  it("merges N segments into 1", async () => {
    const mgr = await buildIndex(5);
    expect(mgr.segments()).toHaveLength(5);
    await mgr.compact();
    expect(mgr.segments()).toHaveLength(1);
  });

  it("merged segment contains all terms from all input segments", async () => {
    const mgr = await buildIndex(4);
    await mgr.compact();
    const [merged] = mgr.segments();
    for (let i = 0; i < 4; i++) {
      const { docIds } = merged.decodePostings(`doc${i}`);
      expect(docIds).toHaveLength(1);
    }
    const common = merged.decodePostings("common");
    expect(common.docIds).toHaveLength(4);
  });

  it("merged segment tf values are preserved", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "cat", tf: 5 }]);
    await mgr.add(1, [{ term: "cat", tf: 3 }]);
    await mgr.compact();
    const [merged] = mgr.segments();
    const { tfs } = merged.decodePostings("cat");
    // two distinct docs; sort by docId (new IDs 0 and 1)
    expect(tfs.sort((a, b) => a - b)).toEqual([3, 5]);
  });

  it("doc lengths are preserved after compaction", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "a", tf: 7 }]); // totalLen = 7
    await mgr.add(1, [{ term: "b", tf: 2 }]); // totalLen = 2
    await mgr.compact();
    const [merged] = mgr.segments();
    const lengths = [merged.docLen(0), merged.docLen(1)].sort((a, b) => a - b);
    expect(lengths).toEqual([2, 7]);
  });

  it("generation increments on compact", async () => {
    const mgr = await buildIndex(3);
    const genBefore = mgr.commitGeneration();
    await mgr.compact();
    expect(mgr.commitGeneration()).toBe(genBefore + 1);
  });
});

describe("compact — query equivalence vs baseline", () => {
  it("8-segment index returns same results as single-segment baseline", async () => {
    // Build the fragmented index (one segment per doc).
    const mgr = await buildIndex(8);

    // Collect "common" docIds from fragmented index (pre-merge).
    const fragmentedIds = collectDocIds(mgr, "common");
    expect(fragmentedIds).toHaveLength(8);

    // Build a single-segment baseline.
    const baseDir = await mkdtemp(join(tmpdir(), "termlog-compact-base-"));
    try {
      const baseBackend = new FsBackend(baseDir);
      const base = await SegmentManager.open({ backend: baseBackend, flushThreshold: 999 });
      for (let i = 0; i < 8; i++) {
        await base.add(i, [{ term: "common", tf: 1 }, { term: `doc${i}`, tf: 2 }]);
      }
      await base.flush();
      const [baseSeg] = base.segments();
      const baseCommon = baseSeg.decodePostings("common");

      // Compact and compare.
      await mgr.compact();
      const [mergedSeg] = mgr.segments();
      const mergedCommon = mergedSeg.decodePostings("common");

      expect(mergedCommon.docIds).toHaveLength(baseCommon.docIds.length);
      // Per-term doc counts match.
      for (let i = 0; i < 8; i++) {
        const baseDocI = baseSeg.decodePostings(`doc${i}`);
        const mergedDocI = mergedSeg.decodePostings(`doc${i}`);
        expect(mergedDocI.docIds).toHaveLength(baseDocI.docIds.length);
        expect(mergedDocI.tfs).toEqual(baseDocI.tfs);
      }
    } finally {
      await rm(baseDir, { recursive: true, force: true });
    }
  });
});

describe("compact — atomicity and crash safety", () => {
  it("manifest.tmp is gone after compact", async () => {
    const mgr = await buildIndex(3);
    await mgr.compact();
    const blobs = await backend.listBlobs("");
    expect(blobs.some((b) => b.endsWith("manifest.tmp"))).toBe(false);
  });

  it("old segment files are deleted after manifest commit", async () => {
    const mgr = await buildIndex(3);
    const oldIds = mgr.segments().map((_, i) => `seg-${String(i).padStart(6, "0")}.seg`);
    await mgr.compact();
    const remaining = await backend.listBlobs("");
    for (const oldFile of oldIds) {
      expect(remaining.some((b) => b.endsWith(oldFile))).toBe(false);
    }
  });

  it("reopen after compact sees merged segment only", async () => {
    let mgr = await buildIndex(4);
    await mgr.compact();
    const genAfter = mgr.commitGeneration();

    mgr = await SegmentManager.open({ backend });
    expect(mgr.commitGeneration()).toBe(genAfter);
    expect(mgr.segments()).toHaveLength(1);
    for (let i = 0; i < 4; i++) {
      const { docIds } = mgr.segments()[0].decodePostings(`doc${i}`);
      expect(docIds).toHaveLength(1);
    }
  });

  it("reopen after compact + further flush sees merged + new segments", async () => {
    let mgr = await buildIndex(3);
    await mgr.compact();
    await mgr.add(99, [{ term: "new", tf: 1 }]);
    await mgr.flush();
    const genAfter = mgr.commitGeneration();

    mgr = await SegmentManager.open({ backend });
    expect(mgr.commitGeneration()).toBe(genAfter);
    expect(mgr.segments()).toHaveLength(2);
    const allNew = mgr.segments().flatMap((s) => s.decodePostings("new").docIds);
    expect(allNew).toHaveLength(1);
  });
});

describe("compact — reader snapshot isolation", () => {
  it("snapshot taken before compact continues to see pre-merge segments", async () => {
    const mgr = await buildIndex(3);
    const snapshot = mgr.segments(); // 3 readers
    expect(snapshot).toHaveLength(3);

    await mgr.compact();

    // Snapshot is unaffected.
    expect(snapshot).toHaveLength(3);
    // Current view has 1.
    expect(mgr.segments()).toHaveLength(1);

    // The old segment readers still work (Buffers are held in memory).
    const ids0 = snapshot[0].decodePostings("doc0").docIds;
    expect(ids0).toHaveLength(1);
  });
});

describe("compact — auto-trigger via mergeThreshold", () => {
  it("auto-compacts when segment count reaches mergeThreshold", async () => {
    // flushThreshold=1 means each add() flushes; mergeThreshold=3 triggers compact after 3 segments.
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, mergeThreshold: 3 });
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    await mgr.add(1, [{ term: "b", tf: 1 }]);
    expect(mgr.segments()).toHaveLength(2); // no compact yet

    await mgr.add(2, [{ term: "c", tf: 1 }]); // flush → 3 segs → auto-compact
    expect(mgr.segments()).toHaveLength(1);
  });
});

describe("compact — mid-compaction crash recovery (#6bcb7e46)", () => {
  it("orphaned merged segment file is cleaned up on reopen after manifest was not committed", async () => {
    // Build 3 segments.
    const mgr = await buildIndex(3);
    const segIdsBeforeCompact = mgr.segments().map((_, i) => `seg-${String(i).padStart(6, "0")}`);

    // Simulate crash mid-compaction: a merged segment file exists on disk but
    // the manifest was NOT updated (old manifest still references the 3 originals).
    // We emulate this by writing a fake orphaned segment blob then reopening.
    await backend.writeBlob("seg-999999.seg", Buffer.from("orphan"));
    await mgr.close();

    // Reopen: recoverOrphans() must delete seg-999999.seg (not in manifest).
    const mgr2 = await SegmentManager.open({ backend, dir });
    const blobs = await backend.listBlobs("seg-");
    expect(blobs).not.toContain("seg-999999.seg");
    // The 3 original segments are still referenced and intact.
    expect(mgr2.segments()).toHaveLength(3);
    for (const id of segIdsBeforeCompact) {
      const { docIds } = mgr2.segments()[segIdsBeforeCompact.indexOf(id)].decodePostings("common");
      expect(docIds).toHaveLength(1);
    }
    await mgr2.close();
  });
});
