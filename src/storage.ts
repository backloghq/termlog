/**
 * StorageBackend abstraction — pluggable blob storage.
 * FsBackend ships natively; S3StorageAdapter wraps any S3-compatible backend.
 */

import { readFile, readdir, unlink, rename, open } from "node:fs/promises";
import { dirname, join } from "node:path";
import { mkdir } from "node:fs/promises";

/**
 * Streaming write handle returned by `StorageBackend.createWriteStream`.
 * Chunks become visible at the target path only after `end()` resolves.
 * `abort()` discards the in-progress write and leaves nothing at the path.
 */
export interface WriteStream {
  write(chunk: Buffer): Promise<void>;
  /** Atomically commits all written chunks to `path`. */
  end(): Promise<void>;
  /** Discards the in-progress write. */
  abort(): Promise<void>;
}

export interface StorageBackend {
  readBlob(path: string): Promise<Buffer>;
  writeBlob(path: string, data: Buffer): Promise<void>;
  listBlobs(prefix: string): Promise<string[]>;
  deleteBlob(path: string): Promise<void>;
  /**
   * Open a streaming write handle for `path`. Chunks written via `write()` are
   * buffered/streamed but not visible until `end()` commits atomically.
   * On error call `abort()` to clean up.
   */
  createWriteStream(path: string): Promise<WriteStream>;
  /**
   * Append `data` to the end of `path`, creating it if absent.
   * Optional — callers fall back to read-modify-writeBlob on backends that omit this.
   * Implementations must fsync before returning so data survives a crash.
   */
  appendBlob?(path: string, data: Buffer): Promise<void>;
  /** Hint for concurrency caps — true when backed by the local filesystem. */
  isLocalFs?(): boolean;
}

/**
 * Local filesystem backend. Atomic writes use a `.tmp` sidecar + rename.
 * All paths are relative to the root directory supplied at construction.
 */
export class FsBackend implements StorageBackend {
  private readonly root: string;

  constructor(root: string) {
    this.root = root;
  }

  isLocalFs(): boolean {
    return true;
  }

  private abs(path: string): string {
    return join(this.root, path);
  }

  async readBlob(path: string): Promise<Buffer> {
    return readFile(this.abs(path));
  }

  /** Atomic write: write to a unique <path>.<nonce>.tmp, fsync it, rename over <path>, fsync the directory. */
  async writeBlob(path: string, data: Buffer): Promise<void> {
    const dest = this.abs(path);
    const dir = dirname(dest);
    // Unique per-call nonce so concurrent writeBlob("same-path") calls don't
    // stomp each other's temp file.
    const tmp = `${dest}.${process.hrtime.bigint()}.tmp`;
    await mkdir(dir, { recursive: true });
    // fsync the data before rename so the file content is durable.
    const fh = await open(tmp, "w");
    try {
      await fh.write(data);
      await fh.sync();
    } finally {
      await fh.close();
    }
    await rename(tmp, dest);
    // fsync the directory so the rename (directory entry) is durable.
    const dh = await open(dir, "r");
    try {
      await dh.sync();
    } finally {
      await dh.close();
    }
  }

  /**
   * List all blob paths (relative to root) whose basename starts with `prefix`.
   * Searches only the root directory (no recursive walk — all termlog files live flat).
   */
  async listBlobs(prefix: string): Promise<string[]> {
    let entries: string[];
    try {
      entries = await readdir(this.root);
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code === "ENOENT") return [];
      throw err;
    }
    return entries.filter((name) => name.startsWith(prefix));
  }

  async deleteBlob(path: string): Promise<void> {
    try {
      await unlink(this.abs(path));
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
    }
  }

  /**
   * Append `data` to `path` using O_APPEND (atomic at the kernel level for
   * writes <= PIPE_BUF on the same filesystem), then fsync for crash durability.
   */
  async appendBlob(path: string, data: Buffer): Promise<void> {
    const dest = this.abs(path);
    await mkdir(dirname(dest), { recursive: true });
    const fh = await open(dest, "a");
    try {
      await fh.write(data);
      await fh.sync();
    } finally {
      await fh.close();
    }
  }

  async createWriteStream(path: string): Promise<WriteStream> {
    const dest = this.abs(path);
    const dir = dirname(dest);
    await mkdir(dir, { recursive: true });
    const tmp = `${dest}.${process.hrtime.bigint()}.tmp`;
    const fh = await open(tmp, "w");
    let done = false;

    return {
      async write(chunk: Buffer): Promise<void> {
        await fh.write(chunk);
      },
      async end(): Promise<void> {
        if (done) return;
        done = true;
        await fh.sync();
        await fh.close();
        await rename(tmp, dest);
        const dh = await open(dir, "r");
        try { await dh.sync(); } finally { await dh.close(); }
      },
      async abort(): Promise<void> {
        if (done) return;
        done = true;
        await fh.close().catch(() => undefined);
        await unlink(tmp).catch(() => undefined);
      },
    };
  }
}
