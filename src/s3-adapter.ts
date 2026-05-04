/**
 * S3StorageAdapter — StorageBackend implementation for S3-compatible object stores.
 *
 * Zero hard dependencies: accepts any client object conforming to S3Client below.
 * Compatible with AWS SDK v3 S3Client, Cloudflare R2, MinIO, and any other
 * S3-compatible SDK that exposes the same operation shapes.
 *
 * Usage:
 *   import { S3StorageAdapter } from "@backloghq/termlog/s3";
 *   import { S3Client } from "@aws-sdk/client-s3";
 *
 *   const adapter = new S3StorageAdapter({
 *     client: new S3Client({ region: "us-east-1" }),
 *     bucket: "my-bucket",
 *     prefix: "my-index/",   // optional — all keys are scoped under this prefix
 *   });
 *   const index = await TermLog.open({ dir: "my-index", backend: adapter });
 *
 * appendBlob is NOT implemented — S3 has no native append. The docids.log
 * journal falls back to snapshot mode automatically (reads current log,
 * prepends new lines, writes back). This is safe for single-writer usage.
 *
 * IMPORTANT: Termlog is single-writer per index. S3 provides no distributed
 * lock. You must ensure at most one writer per (bucket, prefix) combination.
 */

import type { StorageBackend, BlobWriteStream } from "./storage.js";

/** Minimal S3 operation shapes — compatible with AWS SDK v3 and equivalents. */
export interface S3GetObjectOutput {
  Body?: { transformToByteArray(): Promise<Uint8Array> };
}

export interface S3ListObjectsOutput {
  Contents?: Array<{ Key?: string }>;
  IsTruncated?: boolean;
  NextContinuationToken?: string;
}

export interface S3CreateMultipartUploadOutput {
  UploadId?: string;
}

export interface S3UploadPartOutput {
  ETag?: string;
}

export interface S3Client {
  send(command: object): Promise<unknown>;
}

export interface S3CommandConstructors {
  GetObjectCommand: new (input: { Bucket: string; Key: string }) => object;
  PutObjectCommand: new (input: { Bucket: string; Key: string; Body: Uint8Array; ContentType?: string }) => object;
  DeleteObjectCommand: new (input: { Bucket: string; Key: string }) => object;
  ListObjectsV2Command: new (input: { Bucket: string; Prefix?: string; ContinuationToken?: string }) => object;
  /** Optional — required only for createWriteStream (multipart upload). */
  CreateMultipartUploadCommand?: new (input: { Bucket: string; Key: string; ContentType?: string }) => object;
  UploadPartCommand?: new (input: { Bucket: string; Key: string; UploadId: string; PartNumber: number; Body: Uint8Array }) => object;
  CompleteMultipartUploadCommand?: new (input: { Bucket: string; Key: string; UploadId: string; MultipartUpload: { Parts: Array<{ PartNumber: number; ETag: string }> } }) => object;
  AbortMultipartUploadCommand?: new (input: { Bucket: string; Key: string; UploadId: string }) => object;
}

export interface S3StorageAdapterOpts {
  /** S3-compatible client instance (e.g. `new S3Client(…)` from `@aws-sdk/client-s3`). */
  client: S3Client;
  /**
   * Command constructors from the SDK — passed separately so the adapter has
   * zero hard dependencies (no `import { GetObjectCommand } from "@aws-sdk/…"`).
   *
   * Example:
   *   import { S3Client, GetObjectCommand, PutObjectCommand,
   *            DeleteObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
   *   commands: { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command }
   */
  commands: S3CommandConstructors;
  /** Bucket name. */
  bucket: string;
  /** Optional key prefix — all paths are scoped under `${prefix}${path}`. Default: `""`. */
  prefix?: string;
}

export class S3StorageAdapter implements StorageBackend {
  private readonly client: S3Client;
  private readonly commands: S3CommandConstructors;
  private readonly bucket: string;
  private readonly prefix: string;

  constructor(opts: S3StorageAdapterOpts) {
    this.client = opts.client;
    this.commands = opts.commands;
    this.bucket = opts.bucket;
    this.prefix = opts.prefix ?? "";
    if (!this.prefix) {
      console.warn(
        "[S3StorageAdapter] WARNING: empty prefix on a shared bucket. " +
        "recoverOrphans() will list and may delete unrelated seg-* objects. " +
        "Set a non-empty prefix to scope this index to a safe key namespace.",
      );
    }
  }

  private key(path: string): string {
    return `${this.prefix}${path}`;
  }

  async readBlob(path: string): Promise<Buffer> {
    const cmd = new this.commands.GetObjectCommand({
      Bucket: this.bucket,
      Key: this.key(path),
    });
    let output: S3GetObjectOutput;
    try {
      output = await this.client.send(cmd) as S3GetObjectOutput;
    } catch (err: unknown) {
      const code = (err as { name?: string; Code?: string }).name
        ?? (err as { name?: string; Code?: string }).Code;
      if (code === "NoSuchKey" || code === "NotFound") {
        const e = new Error(`ENOENT: no such file: ${path}`);
        (e as NodeJS.ErrnoException).code = "ENOENT";
        throw e;
      }
      throw err;
    }
    if (!output.Body) {
      const e = new Error(`ENOENT: empty body: ${path}`);
      (e as NodeJS.ErrnoException).code = "ENOENT";
      throw e;
    }
    return Buffer.from(await output.Body.transformToByteArray());
  }

  async writeBlob(path: string, data: Buffer): Promise<void> {
    const cmd = new this.commands.PutObjectCommand({
      Bucket: this.bucket,
      Key: this.key(path),
      Body: data,
      ContentType: "application/octet-stream",
    });
    await this.client.send(cmd);
  }

  async listBlobs(prefix: string): Promise<string[]> {
    const results: string[] = [];
    let token: string | undefined;
    const keyPrefix = this.key(prefix);
    const stripLen = this.prefix.length;

    do {
      const cmd = new this.commands.ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: keyPrefix,
        ...(token ? { ContinuationToken: token } : {}),
      });
      const output = await this.client.send(cmd) as S3ListObjectsOutput;
      for (const obj of output.Contents ?? []) {
        if (obj.Key) results.push(obj.Key.slice(stripLen));
      }
      token = output.IsTruncated ? output.NextContinuationToken : undefined;
    } while (token);

    return results;
  }

  async deleteBlob(path: string): Promise<void> {
    const cmd = new this.commands.DeleteObjectCommand({
      Bucket: this.bucket,
      Key: this.key(path),
    });
    try {
      await this.client.send(cmd);
    } catch (err: unknown) {
      const code = (err as { name?: string; Code?: string }).name
        ?? (err as { name?: string; Code?: string }).Code;
      if (code === "NoSuchKey" || code === "NotFound") return;
      throw err;
    }
  }

  // appendBlob intentionally not implemented — S3 has no native append.
  // saveDocIds() falls back to read-modify-write automatically.

  async createWriteStream(path: string): Promise<BlobWriteStream> {
    const { CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand, PutObjectCommand } = this.commands;
    if (!CreateMultipartUploadCommand || !UploadPartCommand || !CompleteMultipartUploadCommand || !AbortMultipartUploadCommand) {
      throw new Error(
        "S3StorageAdapter.createWriteStream requires CreateMultipartUploadCommand, UploadPartCommand, " +
        "CompleteMultipartUploadCommand, and AbortMultipartUploadCommand in the commands object.",
      );
    }

    const key = this.key(path);
    const { client, bucket } = this;
    const MIN_PART_SIZE = 5 * 1024 * 1024; // 5 MiB S3 multipart minimum

    const initOutput = await client.send(
      new CreateMultipartUploadCommand({ Bucket: bucket, Key: key, ContentType: "application/octet-stream" }),
    ) as S3CreateMultipartUploadOutput;
    const uploadId = initOutput.UploadId!;

    const parts: Array<{ PartNumber: number; ETag: string }> = [];
    let partNumber = 1;
    let partBuffer: Buffer[] = [];
    let bufferedSize = 0;
    let done = false;

    const flush = async (force: boolean): Promise<void> => {
      if (bufferedSize === 0) return;
      if (!force && bufferedSize < MIN_PART_SIZE) return;
      const partData = Buffer.concat(partBuffer);
      partBuffer = [];
      bufferedSize = 0;
      const out = await client.send(
        new UploadPartCommand({ Bucket: bucket, Key: key, UploadId: uploadId, PartNumber: partNumber, Body: partData }),
      ) as S3UploadPartOutput;
      parts.push({ PartNumber: partNumber, ETag: out.ETag ?? "" });
      partNumber++;
    };

    return {
      async write(chunk: Buffer): Promise<void> {
        partBuffer.push(chunk);
        bufferedSize += chunk.length;
        await flush(false);
      },
      async end(): Promise<void> {
        if (done) return;
        if (parts.length === 0 && bufferedSize === 0) {
          // Zero-byte stream — S3 rejects CompleteMultipartUpload with empty Parts.
          // Abort the upload and fall back to an empty PutObject.
          done = true;
          await client.send(
            new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId }),
          ).catch(() => undefined);
          await client.send(
            new PutObjectCommand({ Bucket: bucket, Key: key, Body: Buffer.alloc(0), ContentType: "application/octet-stream" }),
          );
          return;
        }
        await flush(true);
        try {
          await client.send(
            new CompleteMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId, MultipartUpload: { Parts: parts } }),
          );
          done = true;
        } catch (err) {
          // Complete failed — abort the dangling upload before re-throwing.
          try {
            await client.send(
              new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId }),
            );
          } catch { /* best-effort */ }
          done = true;
          throw err;
        }
      },
      async abort(): Promise<void> {
        if (done) return;
        done = true;
        await client.send(
          new AbortMultipartUploadCommand({ Bucket: bucket, Key: key, UploadId: uploadId }),
        ).catch(() => undefined);
      },
    };
  }
}
