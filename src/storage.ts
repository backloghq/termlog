/**
 * StorageBackend abstraction — mirrors opslog's interface.
 * FsBackend ships natively; users plug in opslog-s3's S3Backend for cloud storage.
 */

import { readFile, writeFile, readdir, unlink, rename } from "node:fs/promises";
import { dirname, join } from "node:path";
import { mkdir } from "node:fs/promises";

export interface StorageBackend {
  readBlob(path: string): Promise<Buffer>;
  writeBlob(path: string, data: Buffer): Promise<void>;
  listBlobs(prefix: string): Promise<string[]>;
  deleteBlob(path: string): Promise<void>;
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

  /** Atomic write: write to <path>.tmp, then rename over <path>. */
  async writeBlob(path: string, data: Buffer): Promise<void> {
    const dest = this.abs(path);
    const tmp = dest + ".tmp";
    await mkdir(dirname(dest), { recursive: true });
    await writeFile(tmp, data);
    await rename(tmp, dest);
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
}
