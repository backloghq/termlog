import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FsBackend } from "../src/storage.js";
import type { StorageBackend } from "../src/storage.js";

async function makeTmpDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "termlog-storage-"));
}

describe("FsBackend", () => {
  let dir: string;
  let backend: FsBackend;

  beforeEach(async () => {
    dir = await makeTmpDir();
    backend = new FsBackend(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it("isLocalFs() returns true", () => {
    expect(backend.isLocalFs()).toBe(true);
  });

  it("writeBlob + readBlob round-trip", async () => {
    const data = Buffer.from("hello termlog");
    await backend.writeBlob("test.bin", data);
    const read = await backend.readBlob("test.bin");
    expect(read).toEqual(data);
  });

  it("writeBlob is atomic: writes to .tmp then renames", async () => {
    // Verify that the file appears atomically (no partial reads).
    // We can't intercept the rename, but we can verify no .tmp survives.
    const data = Buffer.allocUnsafe(4096).fill(0xab);
    await backend.writeBlob("atomic.bin", data);
    const files = await backend.listBlobs("");
    expect(files).toContain("atomic.bin");
    expect(files.filter((f) => f.endsWith(".tmp"))).toHaveLength(0);
  });

  it("writeBlob overwrites existing file", async () => {
    await backend.writeBlob("overwrite.bin", Buffer.from("first"));
    await backend.writeBlob("overwrite.bin", Buffer.from("second"));
    const read = await backend.readBlob("overwrite.bin");
    expect(read.toString()).toBe("second");
  });

  it("readBlob throws on missing file", async () => {
    await expect(backend.readBlob("nonexistent.bin")).rejects.toThrow();
  });

  it("listBlobs returns only files matching prefix", async () => {
    await backend.writeBlob("seg-001.seg", Buffer.from("a"));
    await backend.writeBlob("seg-002.seg", Buffer.from("b"));
    await backend.writeBlob("manifest.json", Buffer.from("{}"));

    const segs = await backend.listBlobs("seg-");
    expect(segs.sort()).toEqual(["seg-001.seg", "seg-002.seg"]);

    const all = await backend.listBlobs("");
    expect(all).toHaveLength(3);
  });

  it("listBlobs returns empty array when directory does not exist", async () => {
    const fresh = new FsBackend(join(dir, "nonexistent"));
    const result = await fresh.listBlobs("seg-");
    expect(result).toEqual([]);
  });

  it("listBlobs returns empty array when no files match prefix", async () => {
    await backend.writeBlob("manifest.json", Buffer.from("{}"));
    expect(await backend.listBlobs("seg-")).toEqual([]);
  });

  it("deleteBlob removes an existing file", async () => {
    await backend.writeBlob("todelete.bin", Buffer.from("bye"));
    await backend.deleteBlob("todelete.bin");
    await expect(backend.readBlob("todelete.bin")).rejects.toThrow();
  });

  it("deleteBlob is idempotent — does not throw for missing file", async () => {
    await expect(backend.deleteBlob("nope.bin")).resolves.toBeUndefined();
  });

  it("writeBlob creates nested directories", async () => {
    await backend.writeBlob("subdir/nested.bin", Buffer.from("deep"));
    const read = await backend.readBlob("subdir/nested.bin");
    expect(read.toString()).toBe("deep");
  });
});

describe("FsBackend.appendBlob", () => {
  let dir: string;
  let backend: FsBackend;

  beforeEach(async () => {
    dir = await makeTmpDir();
    backend = new FsBackend(dir);
  });

  afterEach(async () => {
    await rm(dir, { recursive: true, force: true });
  });

  it("creates file and appends data", async () => {
    await backend.appendBlob("log.txt", Buffer.from("line1\n"));
    await backend.appendBlob("log.txt", Buffer.from("line2\n"));
    const content = (await backend.readBlob("log.txt")).toString();
    expect(content).toBe("line1\nline2\n");
  });

  it("appends to existing file without truncating", async () => {
    await backend.writeBlob("existing.txt", Buffer.from("start\n"));
    await backend.appendBlob("existing.txt", Buffer.from("appended\n"));
    const content = (await backend.readBlob("existing.txt")).toString();
    expect(content).toBe("start\nappended\n");
  });

  it("creates file if it does not exist", async () => {
    await backend.appendBlob("new.txt", Buffer.from("hello"));
    const content = (await backend.readBlob("new.txt")).toString();
    expect(content).toBe("hello");
  });
});

describe("StorageBackend interface compatibility", () => {
  it("FsBackend satisfies the StorageBackend interface", () => {
    const dir2 = tmpdir();
    const backend2: StorageBackend = new FsBackend(dir2);
    expect(typeof backend2.readBlob).toBe("function");
    expect(typeof backend2.writeBlob).toBe("function");
    expect(typeof backend2.listBlobs).toBe("function");
    expect(typeof backend2.deleteBlob).toBe("function");
    expect(typeof backend2.isLocalFs).toBe("function");
  });

  it("mock backend satisfies StorageBackend interface (S3-shape compat check)", () => {
    const store = new Map<string, Buffer>();

    const mockBackend: StorageBackend = {
      async readBlob(path) {
        const v = store.get(path);
        if (!v) throw new Error(`Not found: ${path}`);
        return v;
      },
      async writeBlob(path, data) { store.set(path, data); },
      async listBlobs(prefix) { return [...store.keys()].filter((k) => k.startsWith(prefix)); },
      async deleteBlob(path) { store.delete(path); },
    };

    // No isLocalFs — optional field
    expect(mockBackend.isLocalFs).toBeUndefined();

    // Functional check
    return mockBackend.writeBlob("x.bin", Buffer.from("hi"))
      .then(() => mockBackend.readBlob("x.bin"))
      .then((buf) => expect(buf.toString()).toBe("hi"));
  });
});
