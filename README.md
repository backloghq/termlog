# @backloghq/termlog

Log-structured full-text search index — segment-based posting lists with LSM compaction, BM25 ranking, zero native dependencies.

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

Existing FTS engines (Lucene, Tantivy) require native deps or a JVM. Most pure-JS alternatives serialize the index to a single in-memory blob — fine for small corpora, but they hit per-file size cliffs in the tens of thousands of documents. Termlog uses immutable on-disk segments with LSM compaction so the corpus scales without those ceilings.

## Architecture

- **Posting lists** — `term → [docId, tf]`, compressed with VByte / delta encoding. (Positions reserved for a future release.)
- **Term dictionary** — sorted on disk; binary search for lookup.
- **Segments** — self-contained immutable files (term dict + postings). New writes create a new segment. Compaction merges N segments into 1.
- **Query execution** — boolean (AND/OR) via posting iterators (zigzag merge for AND, union scan for OR), BM25 scoring on top.
- **Storage** — abstracted via `StorageBackend`; local FS by default, S3 via [@backloghq/termlog-s3](https://github.com/backloghq/termlog-s3).

## S3 backend

S3 support is provided by the companion package [@backloghq/termlog-s3](https://github.com/backloghq/termlog-s3):

```bash
npm install @backloghq/termlog @backloghq/termlog-s3
```

```ts
import { TermLog } from "@backloghq/termlog";
import { S3Backend } from "@backloghq/termlog-s3";
import { S3Client } from "@aws-sdk/client-s3";

const index = await TermLog.open({
  dir: "my-index",
  backend: new S3Backend({
    client: new S3Client({ region: "us-east-1" }),
    bucket: "my-bucket",
    prefix: "my-index/",
  }),
});
```

See the [termlog-s3 README](https://github.com/backloghq/termlog-s3) for IAM permissions, lifecycle rules, and MinIO/LocalStack usage.

## Options

| Option | Default | Description |
|---|---|---|
| `fanout` | 4 | Same-tier segment count that triggers a merge (size-tiered LSM) |
| `flushThreshold` | 1000 | Docs in write buffer before auto-flush |
| `k1` | 1.2 | BM25 term-saturation parameter |
| `b` | 0.75 | BM25 length-normalization parameter |

## Errors

| Class | When thrown |
|---|---|
| `ManifestCorruptionError` | manifest.json contains invalid JSON |
| `ManifestVersionError` | manifest version is outside the supported range |
| `SegmentCorruptionError` | CRC32 mismatch or missing segment file (`.region` tells you which) |
| `MappingCorruptionError` | docids.snap or docids.log is corrupt |
| `TokenizerMismatchError` | reopening an index with a different tokenizer config |
| `IndexLockedError` | another process holds the advisory `.lock` file |
| `WriteStreamError` | base class for streaming write failures (S3 multipart, etc.) |

## Multi-writer / S3 safety

Termlog is **single-writer per index directory**. On local FS an advisory `.lock` file prevents concurrent opens in the same process group. On S3 (or any shared storage) there is no distributed lock — you must ensure at most one writer per index path.

## License

MIT
