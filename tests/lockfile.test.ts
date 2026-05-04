/**
 * Tests for advisory lockfile on SegmentManager.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { rm, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FsBackend } from "../src/storage.js";
import { SegmentManager } from "../src/manager.js";

async function makeTmpDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "termlog-lock-"));
}

let dir: string;

beforeEach(async () => {
  dir = await makeTmpDir();
});

afterEach(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe("lockfile", () => {
  it("two SegmentManager.open() on same dir — second throws IndexLockedError", async () => {
    const mgr1 = await SegmentManager.open({ dir, backend: new FsBackend(dir) });
    await expect(
      SegmentManager.open({ dir, backend: new FsBackend(dir) }),
    ).rejects.toMatchObject({ name: "IndexLockedError" });
    await mgr1.close();
  });

  it("after close(), second open() succeeds", async () => {
    const mgr1 = await SegmentManager.open({ dir, backend: new FsBackend(dir) });
    await mgr1.close();
    const mgr2 = await SegmentManager.open({ dir, backend: new FsBackend(dir) });
    await mgr2.close();
  });

  it("stale lock from non-existent pid is auto-claimed", async () => {
    // Write a lock file with a pid that almost certainly doesn't exist.
    await writeFile(join(dir, ".lock"), "999999", "utf-8");
    // Should not throw — stale lock is claimed.
    const mgr = await SegmentManager.open({ dir, backend: new FsBackend(dir) });
    await mgr.close();
  });

  it("no lock file is created when dir is not provided", async () => {
    // Without dir, no lock is acquired — two opens on same backend are allowed
    // (caller's responsibility to avoid concurrent access without dir).
    const backend = new FsBackend(dir);
    const mgr1 = await SegmentManager.open({ backend });
    const mgr2 = await SegmentManager.open({ backend });
    await mgr1.close();
    await mgr2.close();
  });
});
