# Termlog — Design

A log-structured, segment-based full-text search index. Pure TypeScript. Zero native dependencies. Built to scale past the single-blob limit that constrained AgentDB's v1.4 text index.

## Goals

- 1M+ documents per index without per-file size cliffs.
- Append-only writes; immutable segments; periodic compaction.
- Pluggable storage backend (FS by default, S3 via the existing opslog-s3 pattern).
- BM25 ranking parity with the v1.4 reference implementation.
- Crash-safe: a manifest records committed segments; partial writes are recoverable.

## Non-goals (for v0.1)

- Phrase / proximity / positional queries. (Posting lists don't carry positions in v0.1; the format reserves a position column for v0.2+.)
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
  - (v0.2+) variable-byte-encoded positions.
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

**Frame-of-Reference (FoR)** is a follow-up optimization for v0.2 — block-aligned encoding where each block of 128 doc IDs uses the minimum bit-width that fits the block's max delta. v0.1 ships pure VByte; FoR can be added under the same posting-iterator API.

## Compaction

Standard LSM merge:

- Threshold: `>= MERGE_THRESHOLD` segments triggers a merge (configurable; default 8).
- Tiered: merge segments of similar size to bound write amplification.
- During merge, doc IDs are re-numbered into a single dense range; old segments are deleted only after the merged segment is committed via manifest swap.
- Compaction is non-blocking for reads (segments are immutable; readers hold a manifest snapshot).

## Manifest format

JSON object:
```jsonc
{
  "version": 1,
  "generation": 42,             // monotonic, incremented per commit
  "segments": [
    { "id": "seg-000042", "docCount": 12345, "totalLen": 2345678 },
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

Mirrors opslog's `StorageBackend` interface:

```ts
interface StorageBackend {
  readBlob(path: string): Promise<Buffer>;
  writeBlob(path: string, data: Buffer): Promise<void>;
  listBlobs(prefix: string): Promise<string[]>;
  deleteBlob(path: string): Promise<void>;
}
```

`FsBackend` ships with termlog; users plug in `@backloghq/opslog-s3`'s `S3Backend` for S3-backed indexes.

## Crash recovery

- Manifest is the source of truth. On open, list segments referenced by the manifest; ignore orphaned segment files.
- A partial segment write (process killed mid-flush) leaves a `.seg.tmp` file that's not yet referenced — cleaned up on open.
- Manifest update is atomic via rename.
- CRC32 footers detect on-disk corruption; corrupted segments fail loudly on first read.

## Public API (incremental)

```ts
class TermLog {
  constructor(opts: { dir: string; tokenizer?: Tokenizer; backend?: StorageBackend; mergeThreshold?: number });
  open(): Promise<void>;
  close(): Promise<void>;

  add(docId: string, text: string): Promise<void>;
  remove(docId: string): Promise<void>;
  search(query: string, opts?: { limit?: number; mode?: "and" | "or" }): Promise<Array<{ docId: string; score: number }>>;
  compact(): Promise<void>;

  // Stats
  docCount(): number;
  segmentCount(): number;
  estimatedBytes(): number;
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
