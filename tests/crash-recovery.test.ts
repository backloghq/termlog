/**
 * Crash recovery tests — five failure scenarios from the spec.
 *
 * Each test manually injects the failure state into a real FS directory,
 * then calls SegmentManager.open() and verifies it recovers correctly.
 */
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtemp, rm, writeFile, readdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentManager, ManifestCorruptionError } from "../src/manager.js";
import { SegmentWriter } from "../src/segment.js";
import { SegmentCorruptionError } from "../src/segment.js";
import { FsBackend } from "../src/storage.js";

let dir: string;
let backend: FsBackend;

beforeEach(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-crash-"));
  backend = new FsBackend(dir);
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

// ---------------------------------------------------------------------------
// Scenario 1: process killed during segment write
// A *.seg.tmp file exists with no manifest entry for it.
// ---------------------------------------------------------------------------
describe("Scenario 1 — orphan .seg.tmp (process killed during segment write)", () => {
  it("deletes orphan .seg.tmp on open and index is still usable", async () => {
    // Create a healthy segment committed to the manifest.
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "hello", tf: 1 }]);
    // mgr is now closed; one segment committed.

    // Inject: a stale .seg.tmp that was never renamed.
    await writeFile(join(dir, "seg-orphan.seg.tmp"), Buffer.from("garbage data"));

    // Reopen — should delete the orphan without error.
    mgr = await SegmentManager.open({ backend });

    const files = await readdir(dir);
    expect(files.some((f) => f.endsWith(".seg.tmp"))).toBe(false);

    // Index is still usable.
    expect(mgr.segments()).toHaveLength(1);
    const { docIds } = mgr.segments()[0].decodePostings("hello");
    expect(docIds).toEqual([0]);
  });
});

// ---------------------------------------------------------------------------
// Scenario 2: process killed between segment commit and manifest write
// A *.seg file exists on disk but is NOT referenced by the manifest.
// ---------------------------------------------------------------------------
describe("Scenario 2 — orphan .seg (segment written, manifest not updated)", () => {
  it("deletes unreferenced .seg on open and manifest is consistent", async () => {
    // Create a healthy 1-segment index.
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "world", tf: 1 }]);

    // Inject: an extra .seg that is NOT in the manifest (orphaned segment).
    const orphanWriter = new SegmentWriter();
    orphanWriter.addPosting("orphan", 99, 1);
    orphanWriter.setDocLength(99, 1);
    await orphanWriter.flush("seg-orphan", backend);

    // Verify the orphan file exists before reopen.
    const filesBefore = await readdir(dir);
    expect(filesBefore.some((f) => f === "seg-orphan.seg")).toBe(true);

    // Reopen — orphan should be deleted.
    mgr = await SegmentManager.open({ backend });

    const filesAfter = await readdir(dir);
    expect(filesAfter.some((f) => f === "seg-orphan.seg")).toBe(false);

    // Manifest data is intact.
    expect(mgr.segments()).toHaveLength(1);
    const { docIds } = mgr.segments()[0].decodePostings("world");
    expect(docIds).toEqual([0]);
  });
});

// ---------------------------------------------------------------------------
// Scenario 3: process killed during manifest rename
// manifest.tmp exists alongside manifest.json.
// On open: prefer manifest.json, delete manifest.tmp.
// ---------------------------------------------------------------------------
describe("Scenario 3 — stale manifest.tmp from interrupted rename", () => {
  it("deletes manifest.tmp and reads manifest.json on open", async () => {
    // Create a healthy index.
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "survive", tf: 1 }]);
    const expectedGeneration = mgr.commitGeneration();

    // Inject: stale manifest.tmp (e.g. a previous generation's content)
    await writeFile(join(dir, "manifest.tmp"), Buffer.from('{"version":1,"generation":0,"segments":[],"tokenizer":{"kind":"unicode","minLen":1},"totalDocs":0,"totalLen":0}'));

    // Reopen — should delete manifest.tmp and load manifest.json.
    mgr = await SegmentManager.open({ backend });

    const files = await readdir(dir);
    expect(files.some((f) => f === "manifest.tmp")).toBe(false);

    expect(mgr.commitGeneration()).toBe(expectedGeneration);
    expect(mgr.segments()).toHaveLength(1);
  });

  it("fresh index with only manifest.tmp (no manifest.json) opens as empty", async () => {
    // Inject a stale manifest.tmp with no manifest.json.
    await writeFile(join(dir, "manifest.tmp"), Buffer.from('{"version":1,"generation":5,"segments":[],"tokenizer":{"kind":"unicode","minLen":1},"totalDocs":10,"totalLen":50}'));

    const mgr = await SegmentManager.open({ backend });

    const files = await readdir(dir);
    expect(files.some((f) => f === "manifest.tmp")).toBe(false);

    // No manifest.json → treated as fresh index.
    expect(mgr.commitGeneration()).toBe(0);
    expect(mgr.segments()).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Scenario 4: segment CRC corruption
// A referenced segment has flipped bytes. SegmentReader.open throws
// SegmentCorruptionError. SegmentManager.open propagates it.
// ---------------------------------------------------------------------------
describe("Scenario 4 — segment CRC corruption", () => {
  it("propagates SegmentCorruptionError on open when a referenced segment is corrupt", async () => {
    // Create a committed 1-segment index.
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "data", tf: 1 }]);
    const segId = "seg-000001"; // second segment counter (generation 1)

    // Figure out the actual segment file name by listing.
    const files = await readdir(dir);
    const segFile = files.find((f) => f.endsWith(".seg") && !f.includes("manifest"));

    // Corrupt the segment: flip bytes in the middle of the file.
    if (segFile) {
      const segPath = join(dir, segFile);
      const data = await import("node:fs/promises").then((fs) => fs.readFile(segPath));
      const corrupted = Buffer.from(data);
      // Flip bytes in the postings region (first 20 bytes).
      for (let i = 0; i < Math.min(20, corrupted.length); i++) {
        corrupted[i] ^= 0xff;
      }
      await writeFile(segPath, corrupted);
    }

    // Reopen — should throw SegmentCorruptionError.
    await expect(SegmentManager.open({ backend })).rejects.toThrow(SegmentCorruptionError);

    void segId; // referenced to avoid lint error
  });
});

// ---------------------------------------------------------------------------
// Scenario 5: manifest JSON corruption
// manifest.json exists but contains invalid JSON.
// SegmentManager.open throws ManifestCorruptionError.
// ---------------------------------------------------------------------------
describe("Scenario 5 — manifest JSON corruption", () => {
  it("throws ManifestCorruptionError when manifest.json is not valid JSON", async () => {
    // Create a valid index first, then corrupt the manifest.
    const mgr1 = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr1.add(0, [{ term: "foo", tf: 1 }]);

    // Overwrite manifest.json with garbage.
    await writeFile(join(dir, "manifest.json"), Buffer.from("NOT VALID JSON }{"));

    await expect(SegmentManager.open({ backend })).rejects.toThrow(ManifestCorruptionError);
  });

  it("throws ManifestCorruptionError when manifest.json is truncated", async () => {
    const mgr2 = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr2.add(0, [{ term: "bar", tf: 1 }]);

    // Truncate the manifest mid-JSON.
    await writeFile(join(dir, "manifest.json"), Buffer.from('{"version":1,"gen'));

    await expect(SegmentManager.open({ backend })).rejects.toThrow(ManifestCorruptionError);
  });
});

// ---------------------------------------------------------------------------
// BLOCKER 3a — manifest references a missing segment file
// Expect SegmentCorruptionError(region="footer"), not raw ENOENT.
// ---------------------------------------------------------------------------
describe("Scenario 6 — manifest references missing segment file", () => {
  it("throws SegmentCorruptionError(footer) when referenced .seg is deleted before reopen", async () => {
    // Build a 1-segment index.
    const mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "data", tf: 1 }]);

    // Find the .seg file and delete it while keeping the manifest.
    const files = await readdir(dir);
    const segFile = files.find((f) => f.endsWith(".seg"));
    expect(segFile).toBeDefined();
    const { unlink } = await import("node:fs/promises");
    await unlink(join(dir, segFile!));

    // Reopen — must throw SegmentCorruptionError with region="footer".
    await expect(SegmentManager.open({ backend })).rejects.toMatchObject({
      name: "SegmentCorruptionError",
      region: "footer",
    });
  });
});

// ---------------------------------------------------------------------------
// BLOCKER 3b — mid-compaction crash: manifest committed, old segs not deleted
// Simulate by manually writing pre-compact segs back after compact and
// asserting recoverOrphans deletes them on next reopen.
// ---------------------------------------------------------------------------
describe("Scenario 7 — mid-compaction crash: manifest committed, old segs survive", () => {
  it("recoverOrphans deletes pre-compact segments that are no longer in the manifest", async () => {
    // Build 3 segments.
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "a", tf: 1 }]);
    await mgr.add(1, [{ term: "b", tf: 1 }]);
    await mgr.add(2, [{ term: "c", tf: 1 }]);

    // Note which .seg files exist before compact.
    const beforeFiles = (await readdir(dir)).filter((f) => f.endsWith(".seg"));

    // Compact: manifest is updated to reference merged segment; old segs are deleted.
    await mgr.compact();
    await mgr.close();

    // Simulate "old segs not yet deleted" by writing them back as orphans.
    const orphanWriter = new SegmentWriter();
    orphanWriter.addPosting("orphan", 999, 1);
    orphanWriter.setDocLength(999, 1);
    // Restore the old seg names as orphans (they're not in the manifest).
    for (const f of beforeFiles) {
      const id = f.slice(0, -4); // strip ".seg"
      await (new SegmentWriter()).flush(id, backend).catch(() => {
        // Flush will throw if SegmentWriter has no data — use orphanWriter.
      });
    }
    // Simpler: just write garbage files with the old names.
    for (const f of beforeFiles) {
      await writeFile(join(dir, f), Buffer.from("orphan-data"));
    }

    // Reopen: recoverOrphans must delete the orphans (they're not in the manifest).
    mgr = await SegmentManager.open({ backend });
    const afterFiles = (await readdir(dir)).filter((f) => f.endsWith(".seg"));

    // None of the pre-compact seg names should survive.
    for (const f of beforeFiles) {
      expect(afterFiles, `orphan ${f} was not cleaned up`).not.toContain(f);
    }
    // The merged segment is still there.
    expect(mgr.segments()).toHaveLength(1);
    await mgr.close();
  });
});

// ---------------------------------------------------------------------------
// Real-FS integration: add + flush without crash, reopen, verify consistency.
// (Complements the mock-state tests above with a true open → write → reopen cycle.)
// ---------------------------------------------------------------------------
describe("Crash recovery — round-trip reopen consistency", () => {
  it("after clean flush, reopen sees exactly the committed state", async () => {
    let mgr = await SegmentManager.open({ backend, flushThreshold: 1 });
    await mgr.add(0, [{ term: "persist", tf: 3 }]);
    await mgr.add(1, [{ term: "persist", tf: 1 }]);
    const gen = mgr.commitGeneration();
    const numSegs = mgr.segments().length;

    // Simulate process exit by discarding mgr and reopening.
    mgr = await SegmentManager.open({ backend });

    expect(mgr.commitGeneration()).toBe(gen);
    expect(mgr.segments()).toHaveLength(numSegs);

    // No orphan files.
    const files = await readdir(dir);
    expect(files.some((f) => f.endsWith(".seg.tmp"))).toBe(false);
    expect(files.some((f) => f.endsWith("manifest.tmp"))).toBe(false);
  });
});
