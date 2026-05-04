/**
 * S3StorageAdapter tests — uses an in-memory mock S3 client.
 * No network calls, no AWS SDK dependency.
 */

import { describe, it, expect, beforeEach } from "vitest";
import { S3StorageAdapter } from "../src/s3-adapter.js";
import type {
  S3Client,
  S3CommandConstructors,
  S3GetObjectOutput,
  S3ListObjectsOutput,
} from "../src/s3-adapter.js";
import type { StorageBackend } from "../src/storage.js";

// ---------------------------------------------------------------------------
// Minimal in-memory mock S3 client
// ---------------------------------------------------------------------------

function buildMockS3(): { client: S3Client; commands: S3CommandConstructors; store: Map<string, Buffer> } {
  const store = new Map<string, Buffer>();

  class GetObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
  class PutObjectCommand { constructor(public input: { Bucket: string; Key: string; Body: Uint8Array }) {} }
  class DeleteObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
  class ListObjectsV2Command { constructor(public input: { Bucket: string; Prefix?: string; ContinuationToken?: string }) {} }

  const client: S3Client = {
    async send(cmd: object): Promise<unknown> {
      if (cmd instanceof GetObjectCommand) {
        const data = store.get(cmd.input.Key);
        if (!data) {
          const err = new Error("NoSuchKey") as Error & { name: string };
          err.name = "NoSuchKey";
          throw err;
        }
        const out: S3GetObjectOutput = {
          Body: {
            async transformToByteArray() { return new Uint8Array(data); },
          },
        };
        return out;
      }
      if (cmd instanceof PutObjectCommand) {
        store.set(cmd.input.Key, Buffer.from(cmd.input.Body));
        return {};
      }
      if (cmd instanceof DeleteObjectCommand) {
        store.delete(cmd.input.Key);
        return {};
      }
      if (cmd instanceof ListObjectsV2Command) {
        const prefix = cmd.input.Prefix ?? "";
        const contents = [...store.keys()]
          .filter((k) => k.startsWith(prefix))
          .map((k) => ({ Key: k }));
        const out: S3ListObjectsOutput = { Contents: contents, IsTruncated: false };
        return out;
      }
      throw new Error(`Unknown command: ${String(cmd)}`);
    },
  };

  return { client, commands: { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command }, store };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("S3StorageAdapter", () => {
  let store: Map<string, Buffer>;
  let adapter: S3StorageAdapter;

  beforeEach(() => {
    const mock = buildMockS3();
    store = mock.store;
    adapter = new S3StorageAdapter({
      client: mock.client,
      commands: mock.commands,
      bucket: "test-bucket",
      prefix: "idx/",
    });
  });

  it("writeBlob + readBlob round-trip", async () => {
    const data = Buffer.from("hello s3");
    await adapter.writeBlob("seg-000001.seg", data);
    const read = await adapter.readBlob("seg-000001.seg");
    expect(read).toEqual(data);
  });

  it("readBlob throws ENOENT-shaped error for missing key", async () => {
    const err = await adapter.readBlob("missing.seg").catch((e) => e) as NodeJS.ErrnoException;
    expect(err.code).toBe("ENOENT");
  });

  it("writeBlob scopes key under prefix", async () => {
    await adapter.writeBlob("manifest.json", Buffer.from("{}"));
    expect(store.has("idx/manifest.json")).toBe(true);
    expect(store.has("manifest.json")).toBe(false);
  });

  it("listBlobs returns paths relative to prefix", async () => {
    store.set("idx/seg-000001.seg", Buffer.from("a"));
    store.set("idx/seg-000002.seg", Buffer.from("b"));
    store.set("idx/manifest.json", Buffer.from("{}"));
    store.set("other/seg-000003.seg", Buffer.from("c")); // different prefix

    const segs = await adapter.listBlobs("seg-");
    expect(segs.sort()).toEqual(["seg-000001.seg", "seg-000002.seg"]);
  });

  it("listBlobs returns empty array when no keys match", async () => {
    const result = await adapter.listBlobs("seg-");
    expect(result).toEqual([]);
  });

  it("deleteBlob removes the object", async () => {
    store.set("idx/todelete.seg", Buffer.from("x"));
    await adapter.deleteBlob("todelete.seg");
    expect(store.has("idx/todelete.seg")).toBe(false);
  });

  it("deleteBlob is idempotent — does not throw for missing key", async () => {
    await expect(adapter.deleteBlob("nonexistent.seg")).resolves.toBeUndefined();
  });

  it("appendBlob is undefined — falls back to snapshot mode in saveDocIds", () => {
    expect(adapter.appendBlob).toBeUndefined();
  });

  it("satisfies StorageBackend interface", () => {
    const backend: StorageBackend = adapter;
    expect(typeof backend.readBlob).toBe("function");
    expect(typeof backend.writeBlob).toBe("function");
    expect(typeof backend.listBlobs).toBe("function");
    expect(typeof backend.deleteBlob).toBe("function");
  });
});

describe("S3StorageAdapter — no prefix", () => {
  it("keys are written without prefix when prefix is empty", async () => {
    const { client, commands, store } = buildMockS3();
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "" });
    await adapter.writeBlob("manifest.json", Buffer.from("{}"));
    expect(store.has("manifest.json")).toBe(true);
  });
});

describe("S3StorageAdapter — pagination", () => {
  it("listBlobs follows IsTruncated / NextContinuationToken across two pages", async () => {
    const store = new Map<string, Buffer>();

    // Build 1500 keys spread across two pages.
    const allKeys: string[] = [];
    for (let i = 0; i < 1500; i++) {
      const key = `idx/seg-${String(i).padStart(6, "0")}.seg`;
      store.set(key, Buffer.from("x"));
      allKeys.push(`seg-${String(i).padStart(6, "0")}.seg`);
    }

    const PAGE_SIZE = 1000;
    let callCount = 0;

    class GetObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
    class PutObjectCommand { constructor(public input: { Bucket: string; Key: string; Body: Uint8Array }) {} }
    class DeleteObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
    class ListObjectsV2Command { constructor(public input: { Bucket: string; Prefix?: string; ContinuationToken?: string }) {} }

    const client: S3Client = {
      async send(cmd: object): Promise<unknown> {
        if (cmd instanceof ListObjectsV2Command) {
          callCount++;
          const prefix = cmd.input.Prefix ?? "";
          const matching = [...store.keys()].filter((k) => k.startsWith(prefix));
          const token = cmd.input.ContinuationToken ? parseInt(cmd.input.ContinuationToken, 10) : 0;
          const page = matching.slice(token, token + PAGE_SIZE);
          const nextToken = token + PAGE_SIZE;
          const isTruncated = nextToken < matching.length;
          return {
            Contents: page.map((k) => ({ Key: k })),
            IsTruncated: isTruncated,
            NextContinuationToken: isTruncated ? String(nextToken) : undefined,
          };
        }
        throw new Error(`Unexpected command`);
      },
    };

    const commands = { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command };
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "idx/" });

    const result = await adapter.listBlobs("seg-");
    expect(result).toHaveLength(1500);
    expect(callCount).toBe(2); // exactly two pages
    // All keys must be present (order may vary).
    expect(result.sort()).toEqual(allKeys.sort());
  });
});

describe("S3StorageAdapter — error propagation", () => {
  it("readBlob propagates non-NoSuchKey errors unchanged", async () => {
    class GetObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
    class PutObjectCommand { constructor(public input: { Bucket: string; Key: string; Body: Uint8Array }) {} }
    class DeleteObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
    class ListObjectsV2Command { constructor(public input: { Bucket: string; Prefix?: string; ContinuationToken?: string }) {} }

    const accessDenied = new Error("Access Denied") as Error & { name: string };
    accessDenied.name = "AccessDenied";

    const client: S3Client = {
      async send(cmd: object): Promise<unknown> {
        if (cmd instanceof GetObjectCommand) throw accessDenied;
        throw new Error("unexpected");
      },
    };

    const commands = { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command };
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "idx/" });

    const err = await adapter.readBlob("some.seg").catch((e) => e) as Error & { name: string };
    // Must not be silently converted to ENOENT — the original error propagates.
    expect(err.name).toBe("AccessDenied");
    expect((err as NodeJS.ErrnoException).code).not.toBe("ENOENT");
  });
});
