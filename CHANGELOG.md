# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

## [0.1.0] - 2026-05-04

### Added
- `S3StorageAdapter` at `@backloghq/termlog/s3` sub-export — wraps any S3-compatible client for S3-backed indexes. Constructor warns when `prefix` is empty (would scope `recoverOrphans` to the entire bucket).
- `readonly SegmentReader[]` return type on `segments()` — signals callers must not mutate the snapshot.
- Source maps and declaration maps (`sourceMap: true`, `declarationMap: true` in tsconfig).
- `"sideEffects": false` in package.json for bundler tree-shaking.
- **Size-tiered compaction**: segments carry a `tier` field. After each flush, `chooseCompactionTargets()` picks the lowest tier with `>= fanout` segments and merges exactly `fanout` of them into a new segment at `tier + 1`. This cascades until no tier is eligible. Write amplification is `O(N log_{fanout} N)`. Manual `compact()` merges everything into one segment (`maxTier + 1`).
- `fanout` option on `SegmentManagerOpts` and `TermLogOptions` — configures the size-tiered compaction fanout (default 4).
- `ManifestSegmentEntry.tier` field — per-segment compaction tier stored in the manifest.
- `SegmentReader.docCount` property — number of docs in the segment's sidecar.
- Tiered compaction test suite: cascade promotion, all docs recoverable after multi-tier promotion, manual `compact()` correctness and persistence across reopen, configurable fanout, write-amplification bound assertions.
- `QueryPosting.segIndex` — index of the segment owning each result docId, enabling O(1) docLen lookup in `BM25Ranker.score()`.
- `MultiSegmentIter` rewritten to use `MinHeap<HeapSlot>` — `currentDocId` peek is O(1), seek rebuilds the heap in O(K log K).
- `SegmentReader.docLenEntries()` — exposes the sidecar's `(docId, length)` pairs for compaction without decoding posting lists.
- Docids append-only journal: `docids.log` (JSONL delta log of add/rm entries) replaces full-rewrite on each flush; `docids.snap` (full snapshot) written on close. Load path: snap → log replay.
- Three journal tests: log written after flush, log replay on crash-reopen, remove entries appear in log.
- `ManifestVersionError` class — dedicated error for unsupported manifest version, with `found` and `expected` fields.
- `DEFAULT_FLUSH_THRESHOLD` exported named constant (1000).
- `VERSION` constant sourced from `package.json` at build time via JSON import attribute.
- `@internal` JSDoc tags on all low-level exports so api-extractor can strip them from the public API surface.
- Structured public fields on error classes: `ManifestCorruptionError.detail`, `MappingCorruptionError.detail`, `TokenizerMismatchError.persisted` / `.runtime`.
- `ManifestVersionError` exported from `src/index.ts`.
- NFD normalization test, remove-then-re-add same docId test, mid-compaction crash test, `VERSION` export assertion.
- Top-k heap in `BM25Ranker.score()` when `limit` is set: uses `MinHeap<ScoredDoc>` keyed by `(score asc, docId desc)` — O(hits × log(limit)) instead of O(hits × log(hits)).
- `StorageBackend.appendBlob?(path, data)` optional method — crash-safe O(1) append for backends that support it.
- `FsBackend.appendBlob` — opens `O_APPEND|O_CREAT`, writes chunk, fsyncs.
- `termlog/s3` entry added to `package.json` exports map.
- `TermLog.add()` update regression test: verifies in-place update produces no double-counted results.
- `TermLog` facade-level write mutex (`serialize<R>()` promise chain) serializing `add/remove/close`.
- `compact()` now snapshots docIds after merge to bound `docids.log` growth.
- Missing segment files referenced by the manifest now throw `SegmentCorruptionError(region="footer")`.
- TermLog facade round-trip test: docId string→number→string round-trip survives compact.
- Tombstone carry-forward tests: tombstone for doc in unmerged segment survives compaction, including the cascade tier scenario.
- Multi-process lockfile contention tests.
- Mid-compaction orphan cleanup test.
- Initial repo scaffold (package.json, tsconfig, eslint, vitest, src/index.ts).
- **Posting list codec (`src/codec.ts`)** — `encodeVByte`/`decodeVByte` (7-bit continuation, handles >32-bit values via `Math.floor` division and multiplication to avoid signed-integer overflow), `encodePostings`/`decodePostings` (delta-encoded doc IDs + VByte tf; format: `vbyte(count) | [vbyte(delta) vbyte(tf)]*`), `postingIterator` (lazy decode for query path). 19 tests covering round-trip identity, delta math, edge cases (empty, single entry, large IDs up to 2^35, tf=0/65535), partial iterator advance, and size sanity.
- **Term dictionary v0.1 (`src/term-dict.ts`)** — `TermDict` class with sorted array of `(term, postingsOffset, postingsLength, df)` entries; `add`, `lookup` (binary search, O(log n)), `serialize`/`deserialize` (compact binary: uint32 count + per-entry uint16 termLen + UTF-8 term bytes + three uint32 fields), `fromMap` (sorts unsorted map for segment writer). 20 tests: empty dict, single/many entries, first/middle/last binary search, 1000-entry dict, Unicode terms (café, 東京), 100-entry round-trip, `fromMap` sort correctness.
- **StorageBackend abstraction (`src/storage.ts`)** — `StorageBackend` interface (`readBlob`, `writeBlob`, `listBlobs`, `deleteBlob`, optional `isLocalFs`); `FsBackend` implementation with atomic writes (`.tmp` + rename), prefix-filtered `listBlobs` (root-flat, no recursive walk), idempotent `deleteBlob`, recursive directory creation. 12 tests: round-trip, atomic write (no stale `.tmp`), overwrite, missing-file errors, prefix filtering, empty-dir and no-match list, delete idempotency, nested dirs, interface compat (FsBackend + mock S3-shape backend).
- **Lint script extended to cover `tests/`** — `eslint src/ tests/`.
- **Segment writer + reader (`src/segment.ts`, `src/crc32.ts`)** — `SegmentWriter` accumulates `addPosting`/`setDocLength`/`setTombstones` calls, sorts by term and doc ID, then `flush(id, backend)` writes a single binary `.seg` file atomically. `SegmentReader.open` verifies CRC32 for all four regions (postings, sidecar, tombstones, dict), throws `SegmentCorruptionError` naming the corrupted region on mismatch, and exposes `lookupTerm`, `postings` (lazy iterator), `decodePostings` (full materialization), `docLen`, `terms` (sorted generator), `tombstones` (Uint32Array), `isTombstoned` (binary search). Segment v2 format (64-byte footer): tombstones region between sidecar and dict (`uint32 count | uint32[] sorted docIds`, CRC32 verified). Pure-JS CRC32 in `src/crc32.ts` (IEEE 802.3 polynomial, table-driven).
- **SegmentManager + manifest (`src/manager.ts`)** — `SegmentManager` class with write buffer, auto-flush at configurable threshold, atomic manifest (`manifest.tmp` → rename), and reader snapshot isolation. `SegmentManager.open` reads an existing manifest and reopens all referenced `SegmentReader`s. `add(docId, terms[])` buffers docs; `flush()` writes a new segment via `SegmentWriter`, increments the monotonic `generation` counter, and replaces the reader snapshot immutably so callers holding a prior snapshot are unaffected. `remove(docId)` drops from buffer or queues a tombstone. `segments()` returns the current immutable snapshot; `commitGeneration()` returns the monotonic counter. Manifest format: `{ version, generation, segments[], tokenizer, totalDocs, totalLen }`. Write mutex via `serialize<R>()` promise-chain pattern serializes `add/flush/compact/remove`; reads remain lock-free. Advisory lockfile via O_EXCL open; `IndexLockedError` on live-process conflict; stale-pid auto-claim; `close()` releases lock. `onBeforeManifest` hook called after segment write, before manifest commit. After compaction, `totalDocs`/`totalLen` derived from segment list rather than running counter. `loadManifest` re-throws non-ENOENT errors. `recoverOrphans` uses prefix-scoped list calls to avoid full-bucket scans on S3.
- **Compaction — streaming k-way merge (`src/heap.ts`)** — `MinHeap<T>` binary min-heap; `compact()` uses two-pass streaming: first pass collects surviving docIds (O(unique surviving docs)), second pass heap-driven merge streams term-by-term (O(K) heap entries + O(largest posting list) accumulator).
- **Query iterators + boolean ops (`src/query.ts`)** — `SegmentPostingIter`, `MultiSegmentIter`, `buildTombstoneSet`, `andQuery` (zigzag merge), `orQuery` (k-way union).
- **BM25 scoring layer (`src/scoring.ts`)** — `bm25Score` pure function; `BM25Ranker` wraps `andQuery` or `orQuery` and emits `{docId, score}` sorted score-desc, tie-broken by numeric docId ascending.
- **TermLog facade (`src/termlog.ts`)** — `TermLog.open/add/remove/search/flush/compact/close` with string↔number docId mapping persisted in `docids.snap`/`docids.log`, automatic tokenization, BM25 search.
- **Tokenizer abstraction (`src/tokenizer.ts`)** — `Tokenizer` interface; `UnicodeTokenizer`; `DEFAULT_TOKENIZER`. Tokenizer kind and minLen persisted in manifest; `TokenizerMismatchError` thrown on reopen with different kind.
- **Full public surface exported from `src/index.ts`**.
- **Crash recovery (`src/manager.ts`, `tests/crash-recovery.test.ts`)** — handles orphan `.seg.tmp` cleanup, orphan `.seg` cleanup, stale `manifest.tmp` deletion, CRC corruption, manifest JSON parse failure.
- **Concurrency tests (`tests/concurrency.test.ts`)** — reader-snapshot isolation, 100 concurrent adds, racing flush+compact, racing add+remove.
- **Test gaps (`tests/gaps.test.ts`)** — CRC32 known-vectors, decodeVByte edge cases, segment version mismatch, sidecar CRC corruption, tombstone region CRC corruption, concurrent writeBlob race, UnicodeTokenizer parity, tokenizer kind round-trip, `TokenizerMismatchError`, custom tokenizer round-trip.
- **Heap unit tests (`tests/heap.test.ts`)**.
- **Stress test (`tests/stress.test.ts`)** — gated behind `STRESS=1`; defaults to 10k docs in normal CI.
- **Benchmark suite (`tests/bench.test.ts`)** — gated behind `BENCH=1`; skips gracefully otherwise.

### Changed
- **Compaction no longer renumbers docIds** — original numIds are preserved through merge. The segment format supports sparse uint32 docIds natively. Previously, densification broke `TermLog.numToStr` lookups and tombstone matching for tier-1+ segments.
- **Unresolved tombstones are carried forward** — when a merged segment holds a tombstone targeting a doc in an unmerged segment, the tombstone is written to the merged output. Previously it was silently dropped, resurrecting the deleted doc in subsequent queries.
- Manifest requires exactly version 2 — any other version throws `ManifestVersionError`. The index only ever writes v2.
- `BM25Ranker` tie-break is now numeric (`a.docId - b.docId`) instead of string comparison.
- `SegmentReader` sidecar storage replaced from `Map<number,number>` to interleaved `Uint32Array[2N]` with binary search.
- `DEFAULT_MERGE_THRESHOLD` constant removed.
- `postingIterator` return type widened to `Iterator<Posting, undefined>`.
- Dead manifest temp-file cleanup in `loadManifest` removed — `recoverOrphans()` already handles this.
- `repository.url` in package.json fixed to `git+https://` form.
- `ManifestVersionError` thrown (instead of `ManifestCorruptionError`) when manifest `version` does not match `MANIFEST_VERSION`.
- `BM25Ranker` class-level and `score()` JSDoc updated to document the `mode` parameter and numeric tie-break.
- `TermLog` module-level JSDoc updated to describe the docids journal/snapshot pattern.
- `StorageBackend` module doc no longer claims to mirror opslog's interface.
- Compaction cascade memory: eliminated `mergedDocIds` Set and `values()` array spread — replaced with `tombstonedMergedDocs` (small subset) and running `mergedTotalLen` accumulator.

### Fixed
- `TermLog.search` returning wrong string docId after compaction.
- Tombstoned docs resurrecting after partial compaction.
- Incorrect comment in `codec.ts` claiming VByte supports "up to 2^35-1" — actual limit is 2^49-1 (7 bytes).
- `TermLog.add()` update-in-place data corruption: calling `add("existing-doc", …)` previously reused the numeric ID. Now tombstones the old numId and allocates a fresh one on every update.
- `FsBackend.writeBlob` now fsyncs the data file before rename and the parent directory after rename.
- Compaction first pass now uses `docLenEntries()` (O(docs)) instead of decoding all posting lists (O(postings)).
- Compaction merge pass uses lazy `postings()` iterator instead of `decodePostings()` full materialization.
- README BM25 score example corrected from `0.693` to `0.655`.
- `SegmentWriter.flush()` was double-writing the segment file.
- `loadManifest` caught all errors and silently treated them as fresh-index; now re-throws non-ENOENT errors.
- `recoverOrphans` used `listBlobs("")` (full-bucket scan on S3); now uses prefix-scoped calls.
- `totalDocs`/`totalLen` not decremented on tombstone compaction; now derived from segment list post-compaction.
- `docids.json` write-after-segment race; mapping now persisted before manifest commit.
- `docids.json` full-rewrite on every flush was O(N); replaced by O(delta) log append.
- `mode: "and"` parameter in `TermLog.search()` was dead code; wired to `andQuery` via `BM25Ranker.score()`.
- `UnicodeTokenizer.minLen` hardcoded as 1 in manifest; now read from the tokenizer instance.
- BM25 parity tests replaced cross-repo import with self-contained reference implementation.
