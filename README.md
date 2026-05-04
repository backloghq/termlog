# @backloghq/termlog

Log-structured full-text search index — segment-based posting lists with LSM compaction, BM25 ranking, zero native dependencies.

**Status:** v0.1.0 — log-structured FTS, 167 tests. Segment-based posting lists, LSM compaction, BM25 ranking parity with agentdb TextIndex, crash recovery, reader snapshot isolation, FsBackend nonce-safe concurrent writes.

## Why

Existing FTS engines (Lucene, Tantivy) are great but require native deps or JVM. AgentDB's pre-termlog text index serialized to a single JSON blob with a 256 MB / ~25-30K-doc cap. Termlog replaces that with segment-based storage: each write creates a new immutable segment; segments are merged LSM-tree style; the corpus scales without per-file ceilings.

## Architecture

- **Posting lists** — `term → [docId, tf, positions...]`, compressed with VByte / delta encoding.
- **Term dictionary** — sorted on disk; binary search for lookup; FST for prefix queries (later).
- **Segments** — self-contained immutable files (term dict + postings). New writes create a new segment. Compaction merges N segments into 1.
- **Query execution** — boolean (AND/OR) via posting iterators (zigzag merge for AND, union scan for OR), BM25 scoring on top.
- **Storage** — abstracted via `StorageBackend` (mirrors opslog); local FS by default, S3 via `@backloghq/opslog-s3` reuse.

## License

MIT
