# @backloghq/termlog

Log-structured full-text search index — segment-based posting lists with LSM compaction, BM25 ranking, zero native dependencies.

**Status:** v0.2.0. `TermLog` facade (string docId, tokenization, BM25 search), segment-based posting lists with tombstones, streaming LSM tiered compaction, crash recovery, advisory lockfile, reader snapshot isolation, S3 backend.

## Install

```
npm install @backloghq/termlog
```

## Usage

```ts
import { TermLog } from "@backloghq/termlog";

const index = await TermLog.open({ dir: "./my-index" });

await index.add("doc-1", "the quick brown fox");
await index.add("doc-2", "the lazy dog");
await index.flush();

const results = await index.search("fox", { limit: 10 });
// [{ docId: "doc-1", score: 0.655... }]  (BM25 — exact value depends on corpus)

await index.remove("doc-1");
await index.close();
```

## Why

Existing FTS engines (Lucene, Tantivy) are great but require native deps or JVM. AgentDB's pre-termlog text index serialized to a single JSON blob with a 256 MB / ~25-30K-doc cap. Termlog replaces that with segment-based storage: each write creates a new immutable segment; segments are merged LSM-tree style; the corpus scales without per-file ceilings.

## Architecture

- **Posting lists** — `term → [docId, tf]`, compressed with VByte / delta encoding. (Positions reserved for v0.2+.)
- **Term dictionary** — sorted on disk; binary search for lookup.
- **Segments** — self-contained immutable files (term dict + postings). New writes create a new segment. Compaction merges N segments into 1.
- **Query execution** — boolean (AND/OR) via posting iterators (zigzag merge for AND, union scan for OR), BM25 scoring on top.
- **Storage** — abstracted via `StorageBackend`; local FS by default, S3 via the included `S3StorageAdapter` (`@backloghq/termlog/s3`).

## S3 backend

```ts
import { TermLog } from "@backloghq/termlog";
import { S3StorageAdapter } from "@backloghq/termlog/s3";
import { S3Client, GetObjectCommand, PutObjectCommand,
         DeleteObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";

const index = await TermLog.open({
  dir: "my-index",
  backend: new S3StorageAdapter({
    client: new S3Client({ region: "us-east-1" }),
    commands: { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command },
    bucket: "my-bucket",
    prefix: "my-index/",  // required — scopes all keys; never use empty prefix on shared bucket
  }),
});
```

## Options

| Option | Default | Description |
|---|---|---|
| `fanout` | 4 | Same-tier segment count that triggers a merge (size-tiered LSM) |
| `flushThreshold` | 1000 | Docs in write buffer before auto-flush |
| `k1` | 1.2 | BM25 term-saturation parameter |
| `b` | 0.75 | BM25 length-normalization parameter |
| `mergeThreshold` | — | Backward-compat alias for `fanout` |

## Errors

| Class | When thrown |
|---|---|
| `ManifestCorruptionError` | manifest.json contains invalid JSON |
| `ManifestVersionError` | manifest version is outside the supported range |
| `SegmentCorruptionError` | CRC32 mismatch or missing segment file (`.region` tells you which) |
| `MappingCorruptionError` | docids.snap or docids.log is corrupt |
| `TokenizerMismatchError` | reopening an index with a different tokenizer config |
| `IndexLockedError` | another process holds the advisory `.lock` file |

## Multi-writer / S3 safety

Termlog is **single-writer per index directory**. On local FS an advisory `.lock` file prevents concurrent opens in the same process group. On S3 (or any shared storage) there is no distributed lock — you must ensure at most one writer per index path.

## License

MIT
