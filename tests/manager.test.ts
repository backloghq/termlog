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

describe("manifest-before-reader-open invariant", () => {
  it("flushLocked: SegmentReader.open() throwing leaves in-memory state unchanged", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    await mgr.add(1, [{ term: "b", tf: 1 }]);

    const preTotalDocs = mgr.indexTotalDocs;
    const preGen = mgr.commitGeneration();
    const preSegs = mgr.segments().length;

    // Force SegmentReader.open() to throw by making readBlob fail for .seg files.
    const realRead = backend.readBlob.bind(backend);
    backend.readBlob = async (path: string) => {
      if (path.endsWith(".seg")) throw new Error("simulated read failure");
      return realRead(path);
    };

    await expect(mgr.flush()).rejects.toThrow("simulated read failure");

    // Manifest must not have been committed — state unchanged.
    expect(mgr.indexTotalDocs).toBe(preTotalDocs);
    expect(mgr.commitGeneration()).toBe(preGen);
    expect(mgr.segments().length).toBe(preSegs);
  });

  it("tieredCompactLocked: SegmentReader.open() throwing does not commit merged state", async () => {
    // Use flushThreshold=1 so each add() immediately flushes a segment.
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1, fanout: 4 });
    // Add 3 docs → 3 segments (no cascade yet at fanout=4).
    for (let i = 0; i < 3; i++) {
      await mgr.add(i, [{ term: `t${i}`, tf: 1 }]);
    }
    // 3 segments, gen=3.

    // The 4th add: flushLocked opens the new segment (segRead #1) and commits a
    // manifest (gen→4, 4 segments). Then cascadeCompactLocked fires, writes the
    // merged segment, and tries to open it (segRead #2). We throw there to simulate
    // SegmentReader.open() failure mid-tieredCompact.
    // Expected post-throw state: the flush's changes are committed (gen=4, 4 segments),
    // but tieredCompact's manifest write never happened — so the reader snapshot still
    // holds the 4 individual segments, not the merged one.
    let segReadCount = 0;
    const realRead = backend.readBlob.bind(backend);
    backend.readBlob = async (path: string) => {
      if (path.endsWith(".seg")) {
        segReadCount++;
        if (segReadCount === 2) throw new Error("simulated merged-reader open failure");
      }
      return realRead(path);
    };

    await expect(mgr.add(3, [{ term: "t3", tf: 1 }])).rejects.toThrow("simulated merged-reader open failure");

    // tieredCompact's manifest was NOT committed — segment count is 4 (not 1 merged),
    // and generation advanced only once (from the flush), not twice.
    expect(mgr.segments().length).toBe(4);
    expect(mgr.commitGeneration()).toBe(4); // 3 prior flushes + 1 flush for doc 3
    expect(mgr.indexTotalDocs).toBe(4);    // all 4 docs present
  });

  it("flushLocked: writeManifest failure leaves all in-memory state unchanged", async () => {
    const mgr = await SegmentManager.open({ backend });
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    await mgr.add(1, [{ term: "b", tf: 1 }]);
    await mgr.flush();

    // Add two more docs to the buffer (not yet flushed).
    await mgr.add(2, [{ term: "c", tf: 2 }]);
    await mgr.add(3, [{ term: "d", tf: 3 }]);

    const preTotalDocs = mgr.indexTotalDocs;
    const preTotalLen = mgr.indexTotalLen;
    const preGen = mgr.commitGeneration();
    const preSegs = mgr.segments().length;
    const preBuffered = mgr.bufferedCount();

    // Stub writeBlob to throw on manifest write.
    const realWrite = backend.writeBlob.bind(backend);
    backend.writeBlob = async (path: string, data: Buffer) => {
      if (path === "manifest.json") throw new Error("simulated manifest write failure");
      return realWrite(path, data);
    };

    await expect(mgr.flush()).rejects.toThrow("simulated manifest write failure");

    // ALL state must be unchanged — no inflation even after repeated failures.
    expect(mgr.indexTotalDocs).toBe(preTotalDocs);
    expect(mgr.indexTotalLen).toBe(preTotalLen);
    expect(mgr.commitGeneration()).toBe(preGen);
    expect(mgr.segments().length).toBe(preSegs);
    expect(mgr.bufferedCount()).toBe(preBuffered);

    // Verify a second failed flush also does not accumulate inflation.
    await expect(mgr.flush()).rejects.toThrow("simulated manifest write failure");
    expect(mgr.indexTotalDocs).toBe(preTotalDocs);
    expect(mgr.commitGeneration()).toBe(preGen);

    // Restore and verify a successful flush sees the correct final state.
    backend.writeBlob = realWrite;
    await mgr.flush();
    expect(mgr.indexTotalDocs).toBe(preTotalDocs + preBuffered);
    expect(mgr.commitGeneration()).toBe(preGen + 1);
    expect(mgr.segments().length).toBe(preSegs + 1);
    expect(mgr.bufferedCount()).toBe(0);
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
