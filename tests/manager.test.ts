import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentManager } from "../src/manager.js";
import { FsBackend } from "../src/storage.js";

let dir: string;
let backend: FsBackend;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-mgr-"));
  backend = new FsBackend(dir);
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

function makeDocs(n: number): Array<{ docId: number; terms: Array<{ term: string; tf: number }> }> {
  return Array.from({ length: n }, (_, i) => ({
    docId: i,
    terms: [
      { term: "foo", tf: 1 },
      { term: `word${i}`, tf: 2 },
    ],
  }));
}

describe("SegmentManager — fresh index", () => {
  it("opens without a manifest (generation 0, no segments)", async () => {
    const mgr = await SegmentManager.open({ backend });
    expect(mgr.commitGeneration()).toBe(0);
    expect(mgr.segments()).toHaveLength(0);
  });

  it("flush on empty buffer is a no-op", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.flush();
    expect(mgr.commitGeneration()).toBe(0);
    expect(mgr.segments()).toHaveLength(0);
  });
});

describe("SegmentManager — add + flush", () => {
  it("generation increments on each flush", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "hello", tf: 1 }]);
    await mgr.flush();
    expect(mgr.commitGeneration()).toBe(1);

    await mgr.add(1, [{ term: "world", tf: 1 }]);
    await mgr.flush();
    expect(mgr.commitGeneration()).toBe(2);
  });

  it("segments() grows by one per flush", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    await mgr.flush();
    expect(mgr.segments()).toHaveLength(1);

    await mgr.add(1, [{ term: "b", tf: 1 }]);
    await mgr.flush();
    expect(mgr.segments()).toHaveLength(2);
  });

  it("flushed segments contain the expected terms", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "cat", tf: 3 }, { term: "dog", tf: 1 }]);
    await mgr.flush();

    const [seg] = mgr.segments();
    const cats = seg.decodePostings("cat");
    expect(cats.docIds).toEqual([0]);
    expect(cats.tfs).toEqual([3]);

    const dogs = seg.decodePostings("dog");
    expect(dogs.docIds).toEqual([0]);
    expect(dogs.tfs).toEqual([1]);
  });

  it("buffer drains after flush, bufferedCount resets", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "x", tf: 1 }]);
    expect(mgr.bufferedCount()).toBe(1);
    await mgr.flush();
    expect(mgr.bufferedCount()).toBe(0);
  });
});

describe("SegmentManager — auto-flush", () => {
  it("auto-flushes when buffer reaches flushThreshold", async () => {
    const mgr = await SegmentManager.open({ backend, flushThreshold: 3 });
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    await mgr.add(1, [{ term: "b", tf: 1 }]);
    expect(mgr.segments()).toHaveLength(0); // not yet

    await mgr.add(2, [{ term: "c", tf: 1 }]); // triggers auto-flush
    expect(mgr.segments()).toHaveLength(1);
    expect(mgr.commitGeneration()).toBe(1);
    expect(mgr.bufferedCount()).toBe(0);
  });
});

describe("SegmentManager — reader snapshot isolation", () => {
  it("a snapshot taken before flush does not see the new segment", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "old", tf: 1 }]);
    await mgr.flush();

    const snapshot = mgr.segments(); // [seg-000001]
    expect(snapshot).toHaveLength(1);

    await mgr.add(1, [{ term: "new", tf: 1 }]);
    await mgr.flush();

    // The snapshot is unaffected by the second flush.
    expect(snapshot).toHaveLength(1);
    // The current view has both.
    expect(mgr.segments()).toHaveLength(2);
  });
});

describe("SegmentManager — manifest persistence", () => {
  it("reopening restores generation and segment count", async () => {
    let mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "persist", tf: 1 }]);
    await mgr.flush();
    await mgr.add(1, [{ term: "test", tf: 2 }]);
    await mgr.flush();

    // Reopen from same backend.
    mgr = await SegmentManager.open({ backend });
    expect(mgr.commitGeneration()).toBe(2);
    expect(mgr.segments()).toHaveLength(2);
  });

  it("reopening restores totalDocs and totalLen", async () => {
    let mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "a", tf: 3 }]); // totalLen += 3
    await mgr.add(1, [{ term: "b", tf: 2 }]); // totalLen += 2
    await mgr.flush();

    mgr = await SegmentManager.open({ backend });
    expect(mgr.indexTotalDocs).toBe(2);
    expect(mgr.indexTotalLen).toBe(5);
  });

  it("reopened segments contain correct postings", async () => {
    let mgr = await SegmentManager.open({ backend });
    await mgr.add(42, [{ term: "hello", tf: 7 }]);
    await mgr.flush();

    mgr = await SegmentManager.open({ backend });
    const [seg] = mgr.segments();
    const { docIds, tfs } = seg.decodePostings("hello");
    expect(docIds).toEqual([42]);
    expect(tfs).toEqual([7]);
  });

  it("manifest.tmp is cleaned up after successful flush", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "x", tf: 1 }]);
    await mgr.flush();

    const blobs = await backend.listBlobs("");
    expect(blobs.some((b) => b.endsWith("manifest.tmp"))).toBe(false);
  });
});

describe("SegmentManager — multiple docs per segment", () => {
  it("packs many docs into one segment correctly", async () => {
    const mgr = await SegmentManager.open({ backend });
    for (const { docId, terms } of makeDocs(10)) {
      await mgr.add(docId, terms);
    }
    await mgr.flush();

    expect(mgr.segments()).toHaveLength(1);
    const [seg] = mgr.segments();
    // "foo" appears in every doc with tf=1.
    const { docIds } = seg.decodePostings("foo");
    expect(docIds).toHaveLength(10);
    expect(docIds).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });
});
