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
  S3CreateMultipartUploadOutput,
  S3UploadPartOutput,
} from "../src/s3-adapter.js";
import type { StorageBackend } from "../src/storage.js";
import { WriteStreamError } from "../src/storage.js";

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

// ---------------------------------------------------------------------------
// Multipart upload mock helper
// ---------------------------------------------------------------------------

interface MultipartState {
  uploads: Map<string, { parts: Map<number, Buffer>; completed: boolean; aborted: boolean }>;
  store: Map<string, Buffer>;
  putCalls: Array<{ key: string; body: Buffer }>;
}

function buildMultipartMock(): {
  client: S3Client;
  commands: S3CommandConstructors;
  state: MultipartState;
} {
  const state: MultipartState = {
    uploads: new Map(),
    store: new Map(),
    putCalls: [],
  };

  let uploadCounter = 0;

  class GetObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
  class PutObjectCommand { constructor(public input: { Bucket: string; Key: string; Body: Uint8Array; ContentType?: string }) {} }
  class DeleteObjectCommand { constructor(public input: { Bucket: string; Key: string }) {} }
  class ListObjectsV2Command { constructor(public input: { Bucket: string; Prefix?: string; ContinuationToken?: string }) {} }
  class CreateMultipartUploadCommand { constructor(public input: { Bucket: string; Key: string; ContentType?: string }) {} }
  class UploadPartCommand { constructor(public input: { Bucket: string; Key: string; UploadId: string; PartNumber: number; Body: Uint8Array }) {} }
  class CompleteMultipartUploadCommand { constructor(public input: { Bucket: string; Key: string; UploadId: string; MultipartUpload: { Parts: Array<{ PartNumber: number; ETag: string }> } }) {} }
  class AbortMultipartUploadCommand { constructor(public input: { Bucket: string; Key: string; UploadId: string }) {} }

  const client: S3Client = {
    async send(cmd: object): Promise<unknown> {
      if (cmd instanceof CreateMultipartUploadCommand) {
        const uploadId = `upload-${++uploadCounter}`;
        state.uploads.set(uploadId, { parts: new Map(), completed: false, aborted: false });
        return { UploadId: uploadId } satisfies S3CreateMultipartUploadOutput;
      }
      if (cmd instanceof UploadPartCommand) {
        const upload = state.uploads.get(cmd.input.UploadId);
        if (!upload) throw new Error(`No upload: ${cmd.input.UploadId}`);
        upload.parts.set(cmd.input.PartNumber, Buffer.from(cmd.input.Body));
        return { ETag: `etag-${cmd.input.PartNumber}` } satisfies S3UploadPartOutput;
      }
      if (cmd instanceof CompleteMultipartUploadCommand) {
        const upload = state.uploads.get(cmd.input.UploadId);
        if (!upload) throw new Error(`No upload: ${cmd.input.UploadId}`);
        upload.completed = true;
        // Assemble parts in order into store.
        const orderedParts = cmd.input.MultipartUpload.Parts
          .slice()
          .sort((a, b) => a.PartNumber - b.PartNumber);
        const assembled = Buffer.concat(orderedParts.map((p) => upload.parts.get(p.PartNumber)!));
        state.store.set(cmd.input.Key, assembled);
        return {};
      }
      if (cmd instanceof AbortMultipartUploadCommand) {
        const upload = state.uploads.get(cmd.input.UploadId);
        if (upload) upload.aborted = true;
        return {};
      }
      if (cmd instanceof PutObjectCommand) {
        const body = Buffer.from(cmd.input.Body);
        state.store.set(cmd.input.Key, body);
        state.putCalls.push({ key: cmd.input.Key, body });
        return {};
      }
      if (cmd instanceof GetObjectCommand) {
        const data = state.store.get(cmd.input.Key);
        if (!data) { const e = new Error("NoSuchKey") as Error & { name: string }; e.name = "NoSuchKey"; throw e; }
        return { Body: { async transformToByteArray() { return new Uint8Array(data); } } } satisfies S3GetObjectOutput;
      }
      if (cmd instanceof DeleteObjectCommand) { state.store.delete(cmd.input.Key); return {}; }
      if (cmd instanceof ListObjectsV2Command) { return { Contents: [], IsTruncated: false }; }
      throw new Error(`Unexpected command: ${String(cmd)}`);
    },
  };

  return {
    client,
    commands: {
      GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command,
      CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand,
    },
    state,
  };
}

// ---------------------------------------------------------------------------
// createWriteStream — multipart upload tests
// ---------------------------------------------------------------------------

describe("S3StorageAdapter — createWriteStream multipart", () => {
  const MIN_PART = 5 * 1024 * 1024; // 5 MiB

  it("sub-5MiB write → exactly 1 UploadPart then Complete with 1 part", async () => {
    const { client, commands, state } = buildMultipartMock();
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "p/" });

    const stream = await adapter.createWriteStream("obj.seg");
    const payload = Buffer.alloc(1024, 0x41); // 1 KiB
    await stream.write(payload);
    await stream.end();

    // One upload, completed, not aborted.
    expect(state.uploads.size).toBe(1);
    const [, upload] = [...state.uploads.entries()][0];
    expect(upload.completed).toBe(true);
    expect(upload.aborted).toBe(false);
    expect(upload.parts.size).toBe(1); // exactly 1 UploadPart

    // Content correct in store.
    expect(state.store.get("p/obj.seg")).toEqual(payload);
  });

  it(">5MiB total → at least 2 UploadParts in correct PartNumber order with ETags", { timeout: 15000 }, async () => {
    const { client, commands, state } = buildMultipartMock();
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "p/" });

    const stream = await adapter.createWriteStream("obj.seg");
    // chunk1 is slightly > MIN_PART to trigger an automatic part flush.
    // chunk2 is small — flushed by end(), producing a second part.
    const chunk1 = Buffer.alloc(MIN_PART + 1, 0x41);
    await stream.write(chunk1);
    const chunk2 = Buffer.alloc(1024, 0x42);
    await stream.write(chunk2);
    await stream.end();

    const [, upload] = [...state.uploads.entries()][0];
    expect(upload.completed).toBe(true);
    expect(upload.parts.size).toBeGreaterThanOrEqual(2);

    // Assembled content matches.
    const expected = Buffer.concat([chunk1, chunk2]);
    expect(state.store.get("p/obj.seg")).toEqual(expected);

    // Part numbers are strictly ascending starting at 1.
    const partNumbers = [...upload.parts.keys()].sort((a, b) => a - b);
    expect(partNumbers[0]).toBe(1);
    for (let i = 1; i < partNumbers.length; i++) {
      expect(partNumbers[i]).toBe(partNumbers[i - 1] + 1);
    }
  });

  it("UploadPart throws on second call — error propagates from write(); caller abort() sends AbortMultipartUpload", async () => {
    const { client: baseClient, commands, state } = buildMultipartMock();
    let partCallCount = 0;
    const client: S3Client = {
      async send(cmd: object): Promise<unknown> {
        // Intercept UploadPartCommand to fail on the second call.
        const { UploadPartCommand: UPC } = commands;
        if (UPC && cmd instanceof UPC) {
          partCallCount++;
          if (partCallCount === 2) throw new Error("UploadPart network failure");
        }
        return baseClient.send(cmd);
      },
    };
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "p/" });

    const stream = await adapter.createWriteStream("obj.seg");
    // First write: > 5 MiB → triggers first UploadPart (succeeds).
    await stream.write(Buffer.alloc(MIN_PART + 1, 0x41));
    // Second write: > 5 MiB → triggers second UploadPart (throws).
    const writeErr = await stream.write(Buffer.alloc(MIN_PART + 1, 0x42)).catch((e) => e) as Error;
    expect(writeErr.message).toMatch(/network failure/);

    // Adapter does NOT auto-abort — caller must abort.
    const [uploadId, upload] = [...state.uploads.entries()][0];
    expect(upload.aborted).toBe(false);

    // Caller aborts explicitly.
    await stream.abort();
    expect(upload.aborted).toBe(true);
    void uploadId;
  });

  it("Complete throws — AbortMultipartUpload is sent automatically before re-throwing", async () => {
    const { client: baseClient, commands, state } = buildMultipartMock();
    const client: S3Client = {
      async send(cmd: object): Promise<unknown> {
        const { CompleteMultipartUploadCommand: CMP } = commands;
        if (CMP && cmd instanceof CMP) throw new Error("Complete network failure");
        return baseClient.send(cmd);
      },
    };
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "p/" });

    const stream = await adapter.createWriteStream("obj.seg");
    await stream.write(Buffer.alloc(1024, 0x41));
    const endErr = await stream.end().catch((e) => e) as Error;

    expect(endErr.message).toMatch(/Complete network failure/);

    // AbortMultipartUpload must have been sent automatically.
    const [, upload] = [...state.uploads.entries()][0];
    expect(upload.aborted).toBe(true);
    // Object must NOT appear in the store.
    expect(state.store.has("p/obj.seg")).toBe(false);
  });

  it("zero-byte end() — AbortMultipartUpload sent, falls back to PutObject with empty body", async () => {
    const { client, commands, state } = buildMultipartMock();
    const adapter = new S3StorageAdapter({ client, commands, bucket: "b", prefix: "p/" });

    const stream = await adapter.createWriteStream("obj.seg");
    await stream.end(); // no writes

    // Upload must be aborted (not completed).
    const [, upload] = [...state.uploads.entries()][0];
    expect(upload.aborted).toBe(true);
    expect(upload.completed).toBe(false);

    // Object is present via PutObject fallback with empty body.
    expect(state.putCalls).toHaveLength(1);
    expect(state.putCalls[0].body).toEqual(Buffer.alloc(0));
    expect(state.store.get("p/obj.seg")).toEqual(Buffer.alloc(0));
  });

  it("missing multipart command constructors → throws at createWriteStream, not at write()", async () => {
    const { client, commands } = buildMultipartMock();
    // Strip multipart commands.
    const cmdsWithout: S3CommandConstructors = {
      GetObjectCommand: commands.GetObjectCommand,
      PutObjectCommand: commands.PutObjectCommand,
      DeleteObjectCommand: commands.DeleteObjectCommand,
      ListObjectsV2Command: commands.ListObjectsV2Command,
      // no multipart commands
    };
    const adapter = new S3StorageAdapter({ client, commands: cmdsWithout, bucket: "b", prefix: "p/" });

    const err = await adapter.createWriteStream("obj.seg").catch((e) => e);
    expect(err).toBeInstanceOf(WriteStreamError);
    expect(err.message).toMatch(/requires CreateMultipartUploadCommand/);
  });
});
