# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

## [0.1.0] - 2026-05-04

### Added

- **Posting list codec** — VByte + delta-encoded posting lists (`vbyte(count) | [vbyte(delta) vbyte(tf)]*`); lazy `postingIterator` for the query path; handles doc IDs up to 2^49-1.
- **Segment format v2** — self-contained binary `.seg` file: postings region, packed `Uint32Array` doc-length sidecar (`[docId, len, ...]` interleaved, 8 bytes/doc), tombstones region, term dictionary, 64-byte footer with CRC32 over each region. `SegmentWriter.writeTerm` API enforces strictly ascending lex order (RangeError on violation); sidecar sort uses a `Uint32Array` index array (no tuple allocation). `SegmentReader.open` verifies all four CRC32s and throws `SegmentCorruptionError(region)` on mismatch or missing file.
- **Term dictionary** — sorted on-disk binary format; `add` (in lex order), `lookup` (binary search, O(log n)), `serialize`/`deserialize`, `fromMap`, `fromSortedEntries` (zero-copy for already-sorted entries).
- **StorageBackend abstraction** — `StorageBackend` interface: `readBlob`, `writeBlob`, `listBlobs`, `deleteBlob`, optional `appendBlob`, `createWriteStream`. `BlobWriteStream` interface (`write`/`end`/`abort`). `WriteStreamError` typed error for missing multipart commands.
- **FsBackend** — atomic `writeBlob` (unique `.tmp` nonce + fsync data + rename + fsync parent dir); `appendBlob` via `O_APPEND|O_CREAT`; `createWriteStream` with exception-safe `end()` (on fsync/close/rename failure: closes handle, unlinks `.tmp`, sets `done=true`, re-throws); idempotent `deleteBlob`; nested directory creation.
- **S3StorageAdapter** at `@backloghq/termlog/s3` — multipart upload (5 MiB part minimum); auto-calls `AbortMultipartUpload` when `CompleteMultipartUpload` throws; zero-byte `end()` aborts upload and falls back to `PutObject`; throws `WriteStreamError` at `createWriteStream` time when multipart command constructors are missing; warns on empty prefix. No `appendBlob` — falls back to read-GET-PUT cycle on the docids journal.
- **SegmentManager + manifest** — write buffer with configurable flush threshold; atomic manifest (`manifest.tmp` → rename); immutable reader snapshots (reads are lock-free); `SegmentReader.open` called before `writeManifest` to prevent in-memory desync on open failure; monotonic `generation` counter; `onBeforeManifest` hook.
- **Size-tiered compaction** — fanout-based LSM merge (default fanout=4, configurable); `chooseCompactionTargets` picks the lowest tier with `>= fanout` segments and cascades until no tier is eligible; write amplification bounded at `O(N log_{fanout} N)`; original doc IDs preserved through merge (sparse uint32 natively supported); tombstones targeting docs in unmerged segments are carried forward on the merged output; streaming k-way heap merge (`MinHeap<T>`); compaction memory bounded via `tombstonedMergedDocs` intersection (vs full `mergedDocIds` Set) and running `mergedTotalLen` accumulator.
- **Query execution** — `andQuery` (zigzag merge), `orQuery` (k-way union); `MultiSegmentIter` with `MinHeap<HeapSlot>` (O(1) peek, O(K log K) seek); `buildTombstoneSet`.
- **BM25 ranking** — `bm25Score` pure function; `BM25Ranker` with configurable `k1`/`b`; top-k `MinHeap<ScoredDoc>` when `limit` is set (O(hits × log(limit))); score-desc tie-broken by numeric docId ascending; AND/OR mode.
- **TermLog facade** — `TermLog.open/add/remove/search/flush/compact/close`; string↔number docId mapping persisted in `docids.snap` + `docids.log` (append-only journal, collapsed to snapshot on `compact()`/`close()`); write mutex via promise-chain `serialize<R>()`; `add()` on an existing docId tombstones the old numeric ID and allocates a fresh one.
- **Tokenizer abstraction** — `Tokenizer` interface; `UnicodeTokenizer` (NFD normalization, configurable `minLen`); kind + minLen persisted in manifest; `TokenizerMismatchError` on reopen with different config.
- **Cross-process advisory lockfile** — `O_EXCL` open; stale-PID auto-claim; `IndexLockedError` on live-process conflict; `close()` releases.
- **Crash recovery** — manifest is source of truth; orphan `.seg.tmp` and orphan `.seg` cleaned on open; stale `manifest.tmp` deleted; missing segment file throws `SegmentCorruptionError(region="footer")`; `recoverOrphans` uses prefix-scoped `listBlobs` (no full-bucket scan).
- **Public errors** — `ManifestCorruptionError`, `ManifestVersionError` (with `found`/`expected` fields), `SegmentCorruptionError` (with `region`), `MappingCorruptionError`, `TokenizerMismatchError` (with `persisted`/`runtime`), `IndexLockedError`, `WriteStreamError`.
- **1M-doc stress target** — tiered cascade peaks under 1024 MB RSS in normal CI (10k docs); `STRESS=1 STRESS_TIERED=1` runs full 1M-doc cascade.
- **300 tests** — posting list codec (19), term dict (20), storage/FsBackend (23, including `createWriteStream` atomicity, abort, double-end/abort idempotence, write-error propagation, end-failure cleanup), S3StorageAdapter (multipart round-trip, >5 MiB flush, abort on partial upload, missing-command WriteStreamError, zero-byte end, Complete-failure auto-abort), Crc32Stream parity (5), segment fuzz round-trip (1000 iterations), 1M-doc sidecar, writeTerm RangeError guard, tiered compaction suite (cascade, recovery, fanout, write-amplification bound), crash recovery, concurrency (reader isolation, 100 concurrent adds, racing flush+compact, racing add+remove), manifest-before-reader-open invariant, writeManifest atomicity (flush and compact — no totals/tombstones inflation on failure, onBeforeManifest failure preserves buffer), sidecar non-ascending guard, BM25 parity vs reference implementation, TermLog facade integration.
- **GitHub Actions CI** — Node 22/24/25 matrix; lint + build + test on push/PR to main.
- Source maps and declaration maps (`sourceMap: true`, `declarationMap: true`).
- `"sideEffects": false` in package.json for bundler tree-shaking.
- `DEFAULT_FLUSH_THRESHOLD` exported constant (1000).
- `VERSION` constant sourced from `package.json` via JSON import attribute.
