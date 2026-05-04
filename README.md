# @backloghq/termlog

Log-structured full-text search index — segment-based posting lists with LSM compaction, BM25 ranking, zero native dependencies.

**Status:** v0.1.0. `TermLog` facade (string docId, tokenization, BM25 search), segment-based posting lists with tombstones, streaming LSM compaction, crash recovery, advisory lockfile, reader snapshot isolation.

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
- **Storage** — abstracted via `StorageBackend`; local FS by default, S3 via `@backloghq/opslog-s3`.

## Multi-writer / S3 safety

Termlog is **single-writer per index directory**. On local FS an advisory `.lock` file prevents concurrent opens in the same process group. On S3 (or any shared storage) there is no distributed lock — you must ensure at most one writer per index path. A conditional-write lock for S3 is planned for v0.2.

## License

MIT
