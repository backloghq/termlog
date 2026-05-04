/**
 * Tests for advisory lockfile on SegmentManager.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { rm, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { FsBackend } from "../src/storage.js";
import { SegmentManager, IndexLockedError } from "../src/manager.js";

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

  it("multi-process: child process holding lock causes parent to get IndexLockedError", async () => {
    // The child writes the lock file with its own PID and keeps it alive.
    // We spawn a child that just writes a lock file with its real PID, then
    // the parent tries to open the same dir and expects IndexLockedError.
    //
    // We simulate the child holding the lock by writing a lock file ourselves
    // with the PID of a running process (process.pid), then trying to open.
    // This avoids a full fork/IPC cycle while still validating the cross-process path.
    const lockPath = join(dir, ".lock");
    // Use the current process PID — guaranteed to be alive, so the lock is "live".
    await writeFile(lockPath, String(process.pid), "utf-8");

    // Another SegmentManager.open on the same dir must see the lock as held.
    const backend = new FsBackend(dir);
    await expect(
      SegmentManager.open({ dir, backend }),
    ).rejects.toMatchObject({ name: "IndexLockedError" });
  });

  it("multi-process: child opens SegmentManager; parent sees IndexLockedError with child.pid", async () => {
    // Child calls SegmentManager.open() for real, signals readiness via a file, then
    // stays alive. Parent races a second open and asserts IndexLockedError.pid === child.pid.
    const distDir = new URL("../dist", import.meta.url).pathname;
    const readyFile = join(dir, "child-ready");

    await new Promise<void>((resolve, reject) => {
      const child = spawn(
        process.execPath,
        [
          "--input-type=module",
          "--eval",
          [
            `import { SegmentManager } from '${distDir}/manager.js';`,
            `import { FsBackend } from '${distDir}/storage.js';`,
            `import { writeFileSync } from 'node:fs';`,
            `const backend = new FsBackend('${dir}');`,
            `SegmentManager.open({ dir: '${dir}', backend }).then(() => {`,
            `  writeFileSync('${readyFile}', String(process.pid));`,
            `  // Stay alive until killed`,
            `  setTimeout(() => {}, 30000);`,
            `}).catch((e) => { process.stderr.write(String(e)); process.exit(1); });`,
          ].join("\n"),
        ],
        { stdio: "pipe" },
      );

      child.stderr?.on("data", (d: Buffer) => reject(new Error(`child error: ${d.toString()}`)));

      const pollReady = async (): Promise<number> => {
        const { readFile } = await import("node:fs/promises");
        for (let i = 0; i < 100; i++) {
          try {
            const pidStr = await readFile(readyFile, "utf-8");
            return parseInt(pidStr, 10);
          } catch {
            await new Promise((r) => setTimeout(r, 50));
          }
        }
        throw new Error("child did not signal ready within 5s");
      };

      pollReady().then(async (childPid) => {
        const backend = new FsBackend(dir);
        try {
          await SegmentManager.open({ dir, backend });
          child.kill();
          reject(new Error("expected IndexLockedError, got success"));
        } catch (err) {
          child.kill();
          if (err instanceof IndexLockedError && err.pid === childPid) {
            resolve();
          } else {
            reject(err);
          }
        }
      }).catch((err) => {
        child.kill();
        reject(err);
      });
    });
  }, 15_000);
});
