# Termlog — Design

A log-structured, segment-based full-text search index. Pure TypeScript. Zero native dependencies. Built to scale past the single-blob limit that constrained AgentDB's v1.4 text index.

## Goals

- 1M+ documents per index without per-file size cliffs.
- Append-only writes; immutable segments; periodic compaction.
- Pluggable storage backend (FS by default, S3 via the existing opslog-s3 pattern).
- BM25 ranking parity with the v1.4 reference implementation.
- Crash-safe: a manifest records committed segments; partial writes are recoverable.

## Non-goals (for v0.1)

- Phrase / proximity / positional queries. (Posting lists don't carry positions; the format reserves a position column for a future release.)
- Per-language analyzers, stemming, stop-word removal. Tokenization is pluggable; default is the same Unicode tokenizer AgentDB ships.
- Realtime / sub-second freshness. New writes are visible after segment flush. Configurable flush thresholds.
- Distributed/multi-writer. Single-writer per index, like opslog.

## Data model

A **TermLog** owns:

- A sequence of immutable **Segments** on disk.
- A **manifest** file recording committed segments (atomic rename, like opslog).
- Optional in-memory write buffer (records before segment flush).
- Per-index metadata: total doc count, sum of doc lengths (for BM25 avgdl), tokenizer config.

A **Segment** contains:

- **Term dictionary** — sorted list of `(term, postingsOffset, postingsLength, df)`.
- **Postings region** — concatenated posting lists, each:
  - VByte-encoded number of postings.
  - Delta + VByte-encoded doc IDs.
  - VByte-encoded term frequencies.
  - (future) variable-byte-encoded positions.
- **Doc-length sidecar** — per-doc length array (for BM25 normalization).
- **Footer** — magic bytes, version, offsets to dict / postings / sidecar; CRC32 over each region.

File layout (single binary file per segment, `.seg` extension):

```
[postings region] [doc-length sidecar] [term dictionary] [footer]
```

Term dict at the end so a writer can stream postings without seeking back to fix offsets.

## Encoding details

**VByte (variable-byte):** standard 7-bits-per-byte continuation encoding. Numbers: doc IDs (delta-encoded), tf, list lengths, dictionary offsets.

**Delta + VByte for doc IDs:** posting list is sorted by doc ID; encode `(d_i - d_{i-1})` as VByte. First entry encoded directly.

**Frame-of-Reference (FoR)** is a follow-up optimization — block-aligned encoding where each block of 128 doc IDs uses the minimum bit-width that fits the block's max delta. Current release ships pure VByte; FoR can be added under the same posting-iterator API.

## Compaction

Size-tiered LSM merge:

- Each flushed segment starts at **tier 0**.
- After each flush, `chooseCompactionTargets()` finds the lowest tier with `>= fanout` segments and merges exactly `fanout` of them into a single new segment at `tier + 1`.
- This cascades: after the merge completes, if the resulting tier now has `>= fanout` segments, another merge fires — and so on until no tier is eligible.
- Write amplification is bounded by `O(N log_{fanout} N)`: each document passes through at most `log_{fanout}(N)` merge levels. With fanout=4 and N=1M that is 10 levels (vs unbounded for naive merge-all).
- During merge, original doc IDs are preserved — the segment format supports sparse uint32 docIds natively. Old segments are deleted only after the merged segment is committed via manifest swap. Tombstones targeting docs in unmerged segments are carried forward on the merged output.
- Compaction is non-blocking for reads (segments are immutable; readers hold a manifest snapshot).
- Manual `compact()` merges all segments into one (output tier = maxExistingTier + 1). Useful for pre-warming read-heavy deployments.
- **fanout** is configurable via `SegmentManagerOpts.fanout` (default 4).

## Manifest format

JSON object (v2):
```jsonc
{
  "version": 2,
  "generation": 42,             // monotonic, incremented per commit
  "segments": [
    { "id": "seg-000042", "docCount": 12345, "totalLen": 2345678, "tier": 2 },
    ...
  ],
  "tokenizer": { "kind": "unicode", "minLen": 1 },
  "totalDocs": 100000,
  "totalLen": 19876543
}
```

Atomic update: write `manifest.tmp`, fsync, rename to `manifest.json`. Reader retries on partial reads (very narrow window).

## Query execution

- **Term lookup** per segment: binary search the term dictionary.
- **Posting iterator** per segment: lazy decode of VByte stream; provides `next()` and `seek(docId)`.
- **Multi-segment iterator**: merges per-segment iterators by doc ID with priority queue.
- **AND** (zigzag merge): take the highest seek among iterators; advance the rest to that doc; repeat.
- **OR** (union): standard k-way merge.
- **BM25 scoring**: layered on top of the iterator. Pulls `(docId, tf)` from postings and `dl` from the sidecar. Uses `(N, df, k1, b, avgdl)` from the index.

## Storage backend

`StorageBackend` is the pluggable blob storage interface:

```ts
interface BlobWriteStream {
  /** Stream a chunk to the destination. Not visible until end() resolves. */
  write(chunk: Buffer): Promise<void>;
  /** Atomically commit all chunks to the target path (fsync + rename on FS; CompleteMultipartUpload on S3). */
  end(): Promise<void>;
  /** Discard the in-progress write; target path remains absent. */
  abort(): Promise<void>;
}

interface StorageBackend {
  readBlob(path: string): Promise<Buffer>;
  writeBlob(path: string, data: Buffer): Promise<void>;
  listBlobs(prefix: string): Promise<string[]>;
  deleteBlob(path: string): Promise<void>;
  /** Open a streaming write handle. Not visible until end() commits atomically. On error, call abort(). */
  createWriteStream(path: string): Promise<BlobWriteStream>;
  /** Optional append (O_APPEND). Callers fall back to read-modify-write if absent. */
  appendBlob?(path: string, data: Buffer): Promise<void>;
}
```

**Streaming-write crash safety:**
- `FsBackend`: writes to a unique `<path>.<nonce>.tmp`, fsyncs data, renames over target, fsyncs directory. Concurrent calls use distinct nonces. `abort()` unlinks the tmp file.
- `S3StorageAdapter`: uses the multipart upload protocol. Parts are buffered in memory until 5 MiB (S3 minimum), then uploaded. `end()` calls `CompleteMultipartUpload`; if Complete fails, `AbortMultipartUpload` is sent automatically before re-throwing. Zero-byte `end()` aborts the upload and falls back to `PutObject` with an empty body. S3 multipart has a maximum object size of 50 GiB — a single segment must fit within this bound. Stale incomplete multipart uploads (crash mid-flush) are not cleaned up by termlog; configure an S3 lifecycle rule to expire incomplete uploads after 1–7 days.

`FsBackend` ships with termlog. For S3-backed indexes, use the included `S3StorageAdapter`:

```ts
import { TermLog } from "@backloghq/termlog";
import { S3StorageAdapter } from "@backloghq/termlog/s3";
import {
  S3Client,
  // Read / list / delete commands (required)
  GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command,
  // Multipart write commands (required for flush and compact)
  CreateMultipartUploadCommand, UploadPartCommand,
  CompleteMultipartUploadCommand, AbortMultipartUploadCommand,
} from "@aws-sdk/client-s3";

const tl = await TermLog.open({
  dir: "my-index",
  backend: new S3StorageAdapter({
    client: new S3Client({ region: "us-east-1" }),
    commands: {
      // Read / list / delete
      GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command,
      // Multipart write — required; omitting any of these throws at first flush
      CreateMultipartUploadCommand, UploadPartCommand,
      CompleteMultipartUploadCommand, AbortMultipartUploadCommand,
    },
    bucket: "my-bucket",
    prefix: "my-index/",  // required — scopes all keys; never use empty prefix on shared bucket
  }),
});
```

## DocId journal growth

`saveDocIds` appends delta entries to `docids.log` on every flush. `snapshotDocIds` collapses the log into `docids.snap` and deletes the log. Snapshots are triggered:

1. On `close()` — consolidates in-memory-only removes.
2. On every `compact()` — bounds growth for long-running servers; the compaction already touches all segments so the snapshot cost is negligible.

## Crash recovery

- Manifest is the source of truth. On open, list segments referenced by the manifest; ignore orphaned segment files.
- A partial segment write (process killed mid-flush) leaves a `.seg.tmp` file that's not yet referenced — cleaned up on open.
- A missing segment file referenced by the manifest throws `SegmentCorruptionError(region="footer")` — not a raw ENOENT — so callers can catch it by type.
- Manifest update is atomic via rename.
- CRC32 footers detect on-disk corruption; corrupted segments fail loudly on first read.

## Public API (v0.1.0 — shipped)

```ts
class TermLog {
  static async open(opts: TermLogOptions): Promise<TermLog>;
  async close(): Promise<void>;

  async add(docId: string, text: string): Promise<void>;
  async remove(docId: string): Promise<void>;
  async search(query: string, opts?: { limit?: number; mode?: "and" | "or" }): Promise<Array<{ docId: string; score: number }>>;
  async flush(): Promise<void>;
  async compact(): Promise<void>;

  // Stats
  docCount(): number;
  segmentCount(): number;
}

interface TermLogOptions {
  dir: string;
  backend?: StorageBackend;   // defaults to FsBackend
  tokenizer?: Tokenizer;      // defaults to UnicodeTokenizer
  flushThreshold?: number;
  fanout?: number;            // size-tiered compaction fanout, default 4
  k1?: number;                // BM25 k1, default 1.2
  b?: number;                 // BM25 b, default 0.75
}
```

## Test strategy

Each module ships with its own tests; integration tests cover crash recovery, concurrent reads during compaction, and BM25 parity vs AgentDB's reference TextIndex on a known corpus.

A 1M-doc stress test asserts: total disk usage, no single segment exceeds a configurable cap, query latency p95 under N ms.

## Out of scope for v0.1

- Phrase / position queries.
- FST term dictionary (binary search only).
- Frame-of-Reference encoding (VByte only).
- Tantivy/Lucene-style real-time NRT readers.
- Multi-writer / distributed.

These are tractable extensions on top of the v0.1 architecture, not redesigns.
