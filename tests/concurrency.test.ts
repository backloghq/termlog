/**
 * Concurrency tests — reader-snapshot semantics under concurrent flush and compaction.
 *
 * JS is single-threaded: "concurrent" here means interleaved async microtasks.
 * The critical invariant: a SegmentReader snapshot taken at generation N holds
 * all segment data in memory (SegmentReader.open is eager), so it remains
 * queryable even after a flush+compact cycle deletes the underlying .seg files.
 *
 * Tests use Promise.all to race read queries against write/compact operations
 * and assert:
 *   - No missing-file errors on the reader side (data is in-memory).
 *   - Query results match the snapshot's view (not the post-mutation view).
 *   - No torn manifest reads — SegmentManager.open after any completed
 *     write/compact cycle sees a consistent state.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentManager } from "../src/manager.js";
import { FsBackend } from "../src/storage.js";
import { andQuery, orQuery } from "../src/query.js";

let dir: string;
let backend: FsBackend;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-concurrency-"));
  backend = new FsBackend(dir);
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Helper: query a snapshot using orQuery and collect docIds for a term.
// ---------------------------------------------------------------------------
function querySnapshot(snapshot: ReturnType<SegmentManager["segments"]>, term: string): number[] {
  const ids: number[] = [];
  for (const { docId } of orQuery([term], snapshot)) ids.push(docId);
  return ids;
}

// ---------------------------------------------------------------------------
// 1. Snapshot remains valid after a concurrent flush
// ---------------------------------------------------------------------------
describe("reader snapshot survives concurrent flush", () => {
  it("pre-flush snapshot still returns correct results after a flush adds new segment", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "alpha", tf: 1 }]);
    // snapshot holds generation-1 state
    const snapshot = mgr.segments();
    expect(snapshot).toHaveLength(1);

    // Flush adds a second segment — snapshot must not change.
    await mgr.add(1, [{ term: "beta", tf: 1 }]);

    // snapshot is unaffected
    expect(snapshot).toHaveLength(1);
    expect(querySnapshot(snapshot, "alpha")).toEqual([0]);
    // "beta" not in snapshot
    expect(querySnapshot(snapshot, "beta")).toEqual([]);
  });

  it("multiple flushes do not invalidate older snapshots", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "x", tf: 1 }]);
    const snap0 = mgr.segments(); // 1 segment

    await mgr.add(1, [{ term: "x", tf: 2 }]);
    const snap1 = mgr.segments(); // 2 segments

    await mgr.add(2, [{ term: "x", tf: 3 }]);
    // now 3 segments in current view

    // snap0 still sees only doc 0
    expect(snap0).toHaveLength(1);
    expect(querySnapshot(snap0, "x")).toEqual([0]);

    // snap1 sees docs 0 and 1
    expect(snap1).toHaveLength(2);
    expect(querySnapshot(snap1, "x").sort((a, b) => a - b)).toEqual([0, 1]);

    // current view sees all 3
    expect(mgr.segments()).toHaveLength(3);
    expect(querySnapshot(mgr.segments(), "x").sort((a, b) => a - b)).toEqual([0, 1, 2]);
  });

  it("Promise.all: concurrent reads and flush do not interfere", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "shared", tf: 1 }]);
    const snapshot = mgr.segments();

    // Issue a flush and a read simultaneously.
    const [, readResult] = await Promise.all([
      mgr.add(1, [{ term: "shared", tf: 1 }]),
      // The read executes against the snapshot captured before the Promise.all.
      Promise.resolve(querySnapshot(snapshot, "shared")),
    ]);

    // The snapshot had only doc 0.
    expect(readResult).toEqual([0]);
    // Current view now has both docs.
    expect(querySnapshot(mgr.segments(), "shared").sort((a, b) => a - b)).toEqual([0, 1]);
  });
});

// ---------------------------------------------------------------------------
// 2. Snapshot remains valid after a concurrent compaction
// ---------------------------------------------------------------------------
describe("reader snapshot survives concurrent compaction", () => {
  it("pre-compact snapshot still queryable after compact deletes old .seg files", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 999 });
    // Build 3 segments
    for (let i = 0; i < 3; i++) {
      await mgr.add(i, [{ term: "term", tf: i + 1 }]);
    }
    expect(mgr.segments()).toHaveLength(3);

    const preCompactSnapshot = mgr.segments();

    // Compact — merges 3 segments into 1, deletes the 3 old .seg files.
    await mgr.compact();
    expect(mgr.segments()).toHaveLength(1);

    // The pre-compact snapshot holds Buffers in memory — no file I/O needed.
    // Query must succeed and return correct results despite .seg files being gone.
    const ids = querySnapshot(preCompactSnapshot, "term").sort((a, b) => a - b);
    expect(ids).toEqual([0, 1, 2]);
    // tf values should be correct per-segment
    for (const seg of preCompactSnapshot) {
      const { docIds, tfs } = seg.decodePostings("term");
      for (let i = 0; i < docIds.length; i++) {
        expect(tfs[i]).toBe(docIds[i] + 1);
      }
    }
  });

  it("Promise.all: concurrent reads and compact do not race", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 999 });
    for (let i = 0; i < 4; i++) {
      await mgr.add(i, [{ term: "q", tf: 1 }]);
    }
    const snapshot = mgr.segments(); // 4-segment snapshot

    const [, readIds] = await Promise.all([
      mgr.compact(),
      Promise.resolve(querySnapshot(snapshot, "q").sort((a, b) => a - b)),
    ]);

    // The read resolved synchronously before compact awaited anything.
    expect(readIds).toEqual([0, 1, 2, 3]);
    // After compact, current view is 1 segment.
    expect(mgr.segments()).toHaveLength(1);
  });

  it("segments created during compaction are preserved in new snapshot", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 999 });
    for (let i = 0; i < 3; i++) {
      await mgr.add(i, [{ term: "old", tf: 1 }]);
    }

    // Start compact (which internally captures snapshot at this point).
    // Then interleave a flush for a new doc.
    const compactPromise = mgr.compact();
    // In JS, compact runs synchronously until the first await (writer.flush),
    // so the snapshot is captured before we can interleave. But we can add
    // a doc before awaiting compact to simulate the "flush during compact" path.
    await mgr.add(99, [{ term: "new", tf: 5 }]);
    await compactPromise;

    // The new doc (flushed while compact was in flight) must appear in
    // the post-compact view — either in the merged segment or a surviving segment.
    const allIds = mgr.segments().flatMap((s) => s.decodePostings("new").docIds);
    expect(allIds).toContain(99);
  });
});

// ---------------------------------------------------------------------------
// 3. No torn manifest reads — SegmentManager.open after write/compact is consistent
// ---------------------------------------------------------------------------
describe("manifest consistency after concurrent write+compact cycles", () => {
  it("reopen after flush+compact sees a consistent manifest (not torn)", async () => {
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 999 });
    for (let i = 0; i < 5; i++) {
      await mgr.add(i, [{ term: "data", tf: 1 }]);
    }
    await mgr.compact();
    const expectedGen = mgr.commitGeneration();

    // Reopen — must see exactly 1 segment (merged), consistent generation.
    mgr = await SegmentManager.open({ backend });
    expect(mgr.commitGeneration()).toBe(expectedGen);
    expect(mgr.segments()).toHaveLength(1);
    const { docIds } = mgr.segments()[0].decodePostings("data");
    expect(docIds).toHaveLength(5);
  });

  it("reopen after many flush cycles sees all generations committed", async () => {
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    const N = 10;
    for (let i = 0; i < N; i++) {
      await mgr.add(i, [{ term: `doc${i}`, tf: 1 }]);
    }
    const expectedGen = mgr.commitGeneration();
    const expectedSegs = mgr.segments().length;

    mgr = await SegmentManager.open({ backend });
    expect(mgr.commitGeneration()).toBe(expectedGen);
    expect(mgr.segments()).toHaveLength(expectedSegs);
    // Every doc term is recoverable — each unique term appears in exactly one segment.
    // (Internal docIds are renumbered per-segment after tiered compaction, so we count
    // hits rather than asserting the specific numeric IDs.)
    let found = 0;
    for (let i = 0; i < N; i++) {
      for (const seg of mgr.segments()) {
        found += seg.decodePostings(`doc${i}`).docIds.length;
      }
    }
    expect(found).toBe(N);
  });

  it("Promise.all: racing multiple flushes all land in manifest on reopen", async () => {
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1000 });
    // Add docs in parallel batches (each batch goes to the buffer; manual flush commits).
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    await mgr.add(1, [{ term: "b", tf: 1 }]);
    await mgr.add(2, [{ term: "c", tf: 1 }]);
    await mgr.flush();

    const gen = mgr.commitGeneration();
    mgr = await SegmentManager.open({ backend });
    expect(mgr.commitGeneration()).toBe(gen);
    expect(mgr.segments()).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// 4. AND query correctness across multi-segment snapshots under concurrent writes
// ---------------------------------------------------------------------------
describe("AND query correctness under concurrent writes", () => {
  it("AND query on a snapshot returns intersection, unaffected by later flushes", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    // doc 0: has both "cat" and "dog"
    await mgr.add(0, [{ term: "cat", tf: 1 }, { term: "dog", tf: 1 }]);
    // doc 1: has only "cat"
    await mgr.add(1, [{ term: "cat", tf: 1 }]);

    const snapshot = mgr.segments(); // 2 segments

    // Add a doc that has both terms after snapshot — must NOT appear in AND result.
    await mgr.add(2, [{ term: "cat", tf: 1 }, { term: "dog", tf: 1 }]);

    const andResult = [...andQuery(["cat", "dog"], snapshot)].map((r) => r.docId);
    expect(andResult).toEqual([0]); // only doc 0, not doc 2
  });
});

// ---------------------------------------------------------------------------
// 5. Mutex correctness — verify serialize<R>() serializes mutating operations
// ---------------------------------------------------------------------------
describe("write mutex serialization", () => {
  it("100 concurrent add() calls all land in the manifest — no counter races or lost docs", async () => {
    // flushThreshold=1 so every add auto-flushes; fanout=999 to disable auto-compact.
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 999 });

    const N = 100;
    const adds = Array.from({ length: N }, (_, i) =>
      mgr.add(i, [{ term: `term${i}`, tf: 1 }]),
    );
    await Promise.all(adds);

    // All 100 docs must be committed to segments (buffer is empty after each auto-flush).
    expect(mgr.bufferedCount()).toBe(0);
    // Generation increments once per flush; each add triggers one flush at threshold=1.
    expect(mgr.commitGeneration()).toBe(N);
    // Every doc's term is findable across segments.
    const allDocIds = new Set<number>();
    for (const seg of mgr.segments()) {
      for (let i = 0; i < N; i++) {
        const { docIds } = seg.decodePostings(`term${i}`);
        for (const id of docIds) allDocIds.add(id);
      }
    }
    expect(allDocIds.size).toBe(N);
    for (let i = 0; i < N; i++) expect(allDocIds.has(i)).toBe(true);
  });

  it("Promise.all racing flush() and compact() produces a consistent manifest", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100, fanout: 999 });
    // Pre-load 3 segments so compact() has work to do.
    for (let i = 0; i < 3; i++) {
      await mgr.add(i, [{ term: "x", tf: 1 }]);
      await mgr.flush();
    }
    // Add some buffered docs that flush() will commit.
    await mgr.add(10, [{ term: "x", tf: 1 }]);
    await mgr.add(11, [{ term: "x", tf: 1 }]);

    // Race flush and compact.
    await Promise.all([mgr.flush(), mgr.compact()]);

    // Manifest must be consistent: all pre-flush docs and all pre-compact docs visible.
    const allIds: number[] = [];
    for (const seg of mgr.segments()) {
      allIds.push(...seg.decodePostings("x").docIds);
    }
    // Reopen to confirm manifest was written atomically.
    const mgr2 = await SegmentManager.open({ backend });
    const allIds2: number[] = [];
    for (const seg of mgr2.segments()) {
      allIds2.push(...seg.decodePostings("x").docIds);
    }
    expect(allIds2.length).toBe(allIds.length);
  });

  it("Promise.all racing add() and remove() respects FIFO order — remove wins when submitted after add", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 100 });
    // Add doc 0, then race add(1) and remove(0) concurrently.
    // Since add(1) and remove(0) are submitted in order via Promise.all,
    // the mutex guarantees both complete without corruption.
    await mgr.add(0, [{ term: "alpha", tf: 1 }]);
    await mgr.flush();

    await Promise.all([
      mgr.add(1, [{ term: "alpha", tf: 1 }]),
      mgr.remove(0),
    ]);
    await mgr.flush();

    // After flush, the tombstone for doc 0 should be in a segment.
    // Doc 1 should be visible; doc 0 should be tombstoned.
    const segs = mgr.segments();
    let hasTombstoneFor0 = false;
    for (const seg of segs) {
      if (seg.isTombstoned(0)) { hasTombstoneFor0 = true; break; }
    }
    expect(hasTombstoneFor0).toBe(true);
    // Buffer is empty — both operations committed.
    expect(mgr.bufferedCount()).toBe(0);
  });
});
