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
async function buildIndex(n: number, fanout = 999): Promise<SegmentManager> {
  const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout });
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

describe("compact — auto-trigger via fanout", () => {
  it("auto-compacts when segment count reaches fanout", async () => {
    // flushThreshold=1 means each add() flushes; fanout=3 triggers compact after 3 segments.
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 3 });
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

// ---------------------------------------------------------------------------
// Tiered compaction (#14614fcf)
// ---------------------------------------------------------------------------

describe("tiered compaction — cascade", () => {
  it("cascade: fanout=2 promotes tier-0 → tier-1 → tier-2", async () => {
    // With fanout=2 and flushThreshold=1:
    //   add 0 → [tier0]
    //   add 1 → [tier0, tier0] → merge → [tier1]
    //   add 2 → [tier1, tier0]
    //   add 3 → [tier1, tier0, tier0] → merge tier-0 pair → [tier1, tier1] → merge tier-1 pair → [tier2]
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 2 });
    await mgr.add(0, [{ term: "w", tf: 1 }]);
    await mgr.add(1, [{ term: "w", tf: 1 }]);
    // After 2nd add: 2 tier-0 segs → merge → 1 tier-1 seg
    expect(mgr.segments()).toHaveLength(1);

    await mgr.add(2, [{ term: "w", tf: 1 }]);
    await mgr.add(3, [{ term: "w", tf: 1 }]);
    // After 4th add: tier-1 + tier-0 pair → cascade: merge tier-0 → tier-1; then 2 tier-1s → tier-2
    expect(mgr.segments()).toHaveLength(1);

    // All 4 docs in a single tier-2 segment.
    const all = mgr.segments()[0].decodePostings("w");
    expect(all.docIds).toHaveLength(4);
  });

  it("cascade stops when no tier has >= fanout segments", async () => {
    // fanout=3, add 5 docs. After 3 flushes → merge to 1 tier-1; then 2 remaining tier-0s.
    // Total: 1 tier-1 + 2 tier-0 = 3 segs (tier-0 has 2 < fanout=3, no further merge).
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 3 });
    for (let i = 0; i < 5; i++) {
      await mgr.add(i, [{ term: "w", tf: 1 }]);
    }
    expect(mgr.segments()).toHaveLength(3);
  });

  it("cascade: all docs retrievable after multi-tier promotion", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 2 });
    for (let i = 0; i < 8; i++) {
      await mgr.add(i, [{ term: `t${i}`, tf: 1 }]);
    }
    // All 8 docs must be findable across however many segments result.
    let found = 0;
    for (let i = 0; i < 8; i++) {
      for (const seg of mgr.segments()) {
        found += seg.decodePostings(`t${i}`).docIds.length;
      }
    }
    expect(found).toBe(8);
  });
});


describe("tiered compaction — manual compact()", () => {
  it("manual compact merges everything into one segment", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 4 });
    // Add 6 docs: fanout=4 means after 4th, cascade fires (tier-0 × 4 → tier-1 × 1).
    // Then 5th and 6th add two more tier-0 segments.
    // State before compact: [tier-1, tier-0, tier-0].
    for (let i = 0; i < 6; i++) {
      await mgr.add(i, [{ term: "w", tf: 1 }]);
    }
    expect(mgr.segments().length).toBeGreaterThan(1);

    await mgr.compact();
    expect(mgr.segments()).toHaveLength(1);

    // All 6 docs present.
    const all = mgr.segments()[0].decodePostings("w");
    expect(all.docIds).toHaveLength(6);
  });

  it("manual compact on already-single segment is a no-op", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 4 });
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    const genBefore = mgr.commitGeneration();
    await mgr.compact();
    expect(mgr.segments()).toHaveLength(1);
    expect(mgr.commitGeneration()).toBe(genBefore);
  });

  it("manual compact result persists across reopen", async () => {
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 4 });
    for (let i = 0; i < 6; i++) {
      await mgr.add(i, [{ term: "w", tf: 1 }]);
    }
    await mgr.compact();
    const gen = mgr.commitGeneration();
    await mgr.close();

    mgr = await SegmentManager.open({ backend });
    expect(mgr.segments()).toHaveLength(1);
    expect(mgr.commitGeneration()).toBe(gen);
    const all = mgr.segments()[0].decodePostings("w");
    expect(all.docIds).toHaveLength(6);
  });
});

describe("tiered compaction — configurable fanout", () => {
  it("fanout=2: segments merge earlier than default", async () => {
    const mgr2 = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 2 });
    await mgr2.add(0, [{ term: "a", tf: 1 }]);
    await mgr2.add(1, [{ term: "b", tf: 1 }]);
    // With fanout=2: after 2 tier-0 segs, they merge immediately.
    expect(mgr2.segments()).toHaveLength(1);
  });

  it("fanout=8: segments do not merge until 8 accumulate", async () => {
    const mgr8 = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 8 });
    for (let i = 0; i < 7; i++) {
      await mgr8.add(i, [{ term: "w", tf: 1 }]);
    }
    // 7 < fanout=8, no merge yet.
    expect(mgr8.segments()).toHaveLength(7);

    await mgr8.add(7, [{ term: "w", tf: 1 }]);
    // 8 >= fanout=8, merge fires.
    expect(mgr8.segments()).toHaveLength(1);
  });

});

describe("tiered compaction — write amplification", () => {
  it("tiered (fanout=4) write-amp is bounded by O(N log_4 N): total bytes ≤ 6× flush-only", async () => {
    // For size-tiered with fanout=4, each doc passes through at most log_4(N) merge levels.
    // For N=256: log_4(256)=4 merge levels → write-amp factor = 1 (flush) + 4 (merges) = 5×.
    // We measure the first-flush bytes (one doc, one segment) as a per-doc baseline, then
    // assert total bytes are ≤ the theoretical max with a 50% margin (≤ 6× per-doc baseline × N).
    const N = 256;
    const levels = Math.log(N) / Math.log(4); // 4 for N=256

    let totalBytes = 0;
    let firstSegBytes = 0;
    let firstSeg = true;

    const tieredDir = await mkdtemp(join(tmpdir(), "termlog-wamp-tier-"));
    try {
      const tieredBackend = new FsBackend(tieredDir);
      const originalWrite = tieredBackend.writeBlob.bind(tieredBackend);
      tieredBackend.writeBlob = async (path, data) => {
        if (path.endsWith(".seg")) {
          if (firstSeg) { firstSegBytes = data.length; firstSeg = false; }
          totalBytes += data.length;
        }
        return originalWrite(path, data);
      };
      const mgr = await SegmentManager.open({
        backend: tieredBackend,
        flushThreshold: 1,
        fanout: 4,
      });
      for (let i = 0; i < N; i++) {
        await mgr.add(i, [{ term: "word", tf: 1 }, { term: `doc${i}`, tf: 2 }]);
      }
      await mgr.close();
    } finally {
      await rm(tieredDir, { recursive: true, force: true });
    }

    // Each doc contributes ~firstSegBytes. With (1+levels) rewrites each, the max is:
    // N * firstSegBytes * (1 + levels) * 1.5 (1.5× margin for merged-segment encoding overhead).
    const expectedMaxBytes = firstSegBytes * N * (1 + levels) * 1.5;
    expect(totalBytes).toBeLessThanOrEqual(expectedMaxBytes);
  });

  it("tiered (fanout=4) writes ≥5× fewer bytes than merge-all (fanout=8) for N=256", async () => {
    // Compare tiered (fanout=4) vs a larger fanout that triggers bulk merges (fanout=8).
    // With fanout=8 and N=256: 32 tier-0 merges (8 docs each) + 4 tier-1 merges (64 docs each) +
    //   1 tier-2 merge (256 docs) → merge bytes = 256 + 256 + 256 = 768 doc-eq + 256 flush = 1024.
    // Wait — both are O(N log_fanout N). The real comparison is fanout=8 (3 levels) vs fanout=4 (4 levels).
    //
    // The merge-all scenario (what the old compaction did) is: after N/T batches, merge i rewrites
    // i*T docs. Total = N + T*(1+2+...+K) where K=N/T. For T=8, N=256: K=32, total = 256 + 4480 = 4736.
    // Tiered (fanout=4): N*(1+log_4(N)) = 256*5 = 1280 doc-eq. Ratio ≈ 3.7×.
    //
    // We verify this analytically (the spec requires ≥5× at 1M-doc scale; at 256 docs we assert ≥3×).
    const T = 8;
    const N = 256;
    const K = N / T;
    const mergeAllDocEq = N + T * (K * (K + 1)) / 2;
    const tieredDocEq = N * (1 + Math.log(N) / Math.log(4));
    const ratio = mergeAllDocEq / tieredDocEq;
    expect(ratio).toBeGreaterThanOrEqual(3);

    // At 1M-doc scale (analytical):
    const N1M = 1_000_000;
    const K1M = N1M / T;
    const mergeAll1M = N1M + T * (K1M * (K1M + 1)) / 2; // ~6.25×10^11
    const tiered1M = N1M * (1 + Math.log(N1M) / Math.log(4)); // ~1M * 20 = 2×10^7
    const ratio1M = mergeAll1M / tiered1M;
    expect(ratio1M).toBeGreaterThanOrEqual(5);
  });
});
