# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

### Added
- `QueryPosting.segIndex` — index of the segment owning each result docId, enabling O(1) docLen lookup in `BM25Ranker.score()`.
- `MultiSegmentIter` rewritten to use `MinHeap<HeapSlot>` — `currentDocId` peek is O(1), seek rebuilds the heap in O(K log K) instead of O(K²).
- `SegmentReader.docLenEntries()` — exposes the sidecar's `(docId, length)` pairs for compaction without decoding posting lists.
- Docids append-only journal: `docids.log` (JSONL delta log of add/rm entries) replaces full-rewrite `docids.json` on each flush; `docids.snap` (full snapshot) written on close. Load path: snap → log replay → legacy `docids.json` fallback for backward compatibility.
- Three journal tests: log written after flush, log replay on crash-reopen (lock deleted to simulate crash), remove entries appear in log.
- `ManifestVersionError` class — dedicated error for unsupported manifest version, with `found` and `expected` fields (previously a generic `ManifestCorruptionError`).
- `DEFAULT_FLUSH_THRESHOLD` exported named constant (1000); previously a magic literal.
- `VERSION` constant now sourced from `package.json` at build time via JSON import attribute; no longer a manually maintained literal.
- `@internal` JSDoc tags on all low-level exports (codec, term-dict, segment, manager internals, query, scoring) so api-extractor can strip them from the public API surface.
- Structured public fields on error classes: `ManifestCorruptionError.detail`, `MappingCorruptionError.detail`, `TokenizerMismatchError.persisted` / `.runtime`.
- `ManifestVersionError` exported from `src/index.ts`.
- NFD normalization test, remove-then-re-add same docId test, mid-compaction crash test, `VERSION` export assertion.

### Changed
- `ManifestVersionError` is now thrown (instead of `ManifestCorruptionError`) when the manifest's `version` field does not match `MANIFEST_VERSION`.
- `BM25Ranker` class-level and `score()` JSDoc updated to document the `mode` parameter and AND semantics.
- `TermLog` module-level JSDoc updated to describe the docids journal/snapshot pattern (replaces stale `docids.json` reference).
- `StorageBackend` module doc no longer claims to mirror opslog's interface.
- README BM25 score example corrected from `0.693` to `0.655` (the idf alone is `ln(2) ≈ 0.693` but the full BM25 score is lower after length normalization).

### Fixed
- `FsBackend.writeBlob` now fsyncs the data file before rename and the parent directory after rename — ensures both file content and directory entry survive power failure.
- Compaction first pass decoded all posting lists to collect surviving docIds (O(postings)); now uses `docLenEntries()` (O(docs), sidecar already in RAM).
- Compaction merge pass used `decodePostings()` (full materialization per term per segment); replaced with lazy `postings()` iterator — constant per-posting memory.
- `docids.json` full-rewrite on every flush was O(N) for N=total mapped docs; replaced by O(delta) log append — sub-millisecond for any delta size.

## [0.1.0] - 2026-05-04

### Added
- Initial repo scaffold (package.json, tsconfig, eslint, vitest, src/index.ts).
- **Posting list codec (`src/codec.ts`)** — `encodeVByte`/`decodeVByte` (7-bit continuation, handles >32-bit values via `Math.floor` division and multiplication to avoid signed-integer overflow), `encodePostings`/`decodePostings` (delta-encoded doc IDs + VByte tf; format: `vbyte(count) | [vbyte(delta) vbyte(tf)]*`), `postingIterator` (lazy decode for query path). 19 tests covering round-trip identity, delta math, edge cases (empty, single entry, large IDs up to 2^35, tf=0/65535), partial iterator advance, and size sanity.
- **Term dictionary v0.1 (`src/term-dict.ts`)** — `TermDict` class with sorted array of `(term, postingsOffset, postingsLength, df)` entries; `add`, `lookup` (binary search, O(log n)), `serialize`/`deserialize` (compact binary: uint32 count + per-entry uint16 termLen + UTF-8 term bytes + three uint32 fields), `fromMap` (sorts unsorted map for segment writer). 20 tests: empty dict, single/many entries, first/middle/last binary search, 1000-entry dict, Unicode terms (café, 東京), 100-entry round-trip, `fromMap` sort correctness.
- **StorageBackend abstraction (`src/storage.ts`)** — `StorageBackend` interface (`readBlob`, `writeBlob`, `listBlobs`, `deleteBlob`, optional `isLocalFs`); `FsBackend` implementation with atomic writes (`.tmp` + rename), prefix-filtered `listBlobs` (root-flat, no recursive walk), idempotent `deleteBlob`, recursive directory creation. 12 tests: round-trip, atomic write (no stale `.tmp`), overwrite, missing-file errors, prefix filtering, empty-dir and no-match list, delete idempotency, nested dirs, interface compat (FsBackend + mock S3-shape backend).
- **Lint script extended to cover `tests/`** — `eslint src/ tests/`.
- **Segment writer + reader (`src/segment.ts`, `src/crc32.ts`)** — `SegmentWriter` accumulates `addPosting`/`setDocLength`/`setTombstones` calls, sorts by term and doc ID, then `flush(id, backend)` writes a single binary `.seg` file atomically. `SegmentReader.open` verifies CRC32 for all four regions (postings, sidecar, tombstones, dict), throws `SegmentCorruptionError` naming the corrupted region on mismatch, and exposes `lookupTerm`, `postings` (lazy iterator), `decodePostings` (full materialization), `docLen`, `terms` (sorted generator), `tombstones` (Uint32Array), `isTombstoned` (binary search). Segment v2 format (64-byte footer): tombstones region between sidecar and dict (`uint32 count | uint32[] sorted docIds`, CRC32 verified). Pure-JS CRC32 in `src/crc32.ts` (IEEE 802.3 polynomial, table-driven).
- **SegmentManager + manifest (`src/manager.ts`)** — `SegmentManager` class with write buffer, auto-flush at configurable threshold, atomic manifest (`manifest.tmp` → rename), and reader snapshot isolation. `SegmentManager.open` reads an existing manifest and reopens all referenced `SegmentReader`s. `add(docId, terms[])` buffers docs; `flush()` writes a new segment via `SegmentWriter`, increments the monotonic `generation` counter, and replaces the reader snapshot immutably so callers holding a prior snapshot are unaffected. `remove(docId)` drops from buffer or queues a tombstone. `segments()` returns the current immutable snapshot; `commitGeneration()` returns the monotonic counter. Manifest format: `{ version, generation, segments[], tokenizer, totalDocs, totalLen }`. Write mutex via `serialize<R>()` promise-chain pattern (mirrors opslog) serializes `add/flush/compact/remove`; reads remain lock-free. Advisory lockfile via O_EXCL open; `IndexLockedError` on live-process conflict; stale-pid auto-claim; `close()` releases lock. `onBeforeManifest` hook called after segment write, before manifest commit (used by TermLog to keep docids.json consistent). After compaction, `totalDocs`/`totalLen` are derived from the new segment list rather than the running counter so tombstoned docs are reflected immediately. `loadManifest` re-throws non-ENOENT errors (EACCES, EIO, S3 5xx). `recoverOrphans` uses prefix-scoped list calls (`listBlobs("seg-")` + `listBlobs("manifest")`) to avoid full-bucket scans on S3.
- **Compaction — streaming k-way merge (`src/heap.ts`)** — `MinHeap<T>` binary min-heap; `compact()` uses two-pass streaming: first pass collects surviving docIds (O(unique surviving docs)), second pass heap-driven merge streams term-by-term (O(K) heap entries + O(largest posting list) accumulator) instead of materializing `Map<term, Map<docId, tf>>`. Tombstoned docIds are physically dropped during compaction.
- **Query iterators + boolean ops (`src/query.ts`)** — `SegmentPostingIter` wraps a single segment's lazy posting iterator with `advance()` and `seek(docId)`. `MultiSegmentIter` k-way merges per-segment iters for one term in docId order, filtering tombstoned docIds. `buildTombstoneSet(segments)` builds union set for query filtering. `andQuery` (zigzag merge) yields only docIds present in all term iterators; `orQuery` (k-way union) yields every docId in any term iterator with per-term tfs collected.
- **BM25 scoring layer (`src/scoring.ts`)** — `bm25Score(tf, dl, df, N, k1, b, avgdl)` pure function; `BM25Ranker` wraps `andQuery` or `orQuery` (configurable via `mode` param) and emits `{docId, score}` sorted score-desc, tie-broken by string-lexicographic docId asc. `BM25Ranker.score()` accepts `mode: "and" | "or"` parameter (default "or").
- **TermLog facade (`src/termlog.ts`)** — `TermLog.open/add/remove/search/flush/compact/close` with string↔number docId mapping (persisted in `docids.json` before manifest commit via `onBeforeManifest` hook), automatic tokenization, BM25 search. `search()` forwards `mode: "and" | "or"` to `BM25Ranker`. `MappingCorruptionError` on corrupt `docids.json`. Mapping saved only on flush (via hook) and unconditionally on `close()`.
- **Tokenizer abstraction (`src/tokenizer.ts`)** — `Tokenizer` interface with `kind: string` and optional `minLen?: number`; `UnicodeTokenizer` matching agentdb's `/[\p{L}\p{M}\p{N}]+/gu` regex; `DEFAULT_TOKENIZER`. Tokenizer kind and minLen persisted in manifest; `TokenizerMismatchError` thrown on reopen with different kind.
- **Full public surface exported from `src/index.ts`** — all classes, functions, and types.
- **Crash recovery (`src/manager.ts`, `tests/crash-recovery.test.ts`)** — `SegmentManager.open` handles all 5 failure modes: orphan `.seg.tmp` cleanup, orphan `.seg` cleanup, stale `manifest.tmp` deletion, CRC corruption propagated as `SegmentCorruptionError`, manifest JSON parse failure throws `ManifestCorruptionError`.
- **Concurrency tests (`tests/concurrency.test.ts`)** — reader-snapshot isolation, 100 concurrent adds, racing flush+compact, racing add+remove (write mutex verification).
- **Test gaps (`tests/gaps.test.ts`)** — CRC32 known-vectors, decodeVByte edge cases, segment version mismatch, sidecar CRC corruption, tombstone region CRC corruption, concurrent writeBlob race (nonce isolation), UnicodeTokenizer parity, tokenizer kind round-trip, `TokenizerMismatchError`, custom tokenizer round-trip.
- **Heap unit tests (`tests/heap.test.ts`)** — 7 tests: empty, single, ascending order, duplicates, object comparison, interleaved push/pop, size tracking.
- **Stress test (`tests/stress.test.ts`)** — gated behind `STRESS=1`; defaults to 10k docs in normal CI.
- **Benchmark suite (`tests/bench.test.ts`)** — gated behind `BENCH=1`; skips gracefully otherwise.

### Fixed
- `SegmentWriter.flush()` was double-writing the segment file; now writes directly once via `FsBackend.writeBlob`.
- `loadManifest` caught all errors and silently treated them as fresh-index; now re-throws non-ENOENT errors.
- `recoverOrphans` used `listBlobs("")` (full-bucket scan on S3); now uses prefix-scoped calls.
- `totalDocs`/`totalLen` not decremented on tombstone compaction; now derived from segment list post-compaction.
- `docids.json` write-after-segment race; mapping now persisted before manifest commit.
- `docids.json` written on every `add()`/`remove()` (write amplification); now persisted only on flush and close.
- `mode: "and"` parameter in `TermLog.search()` was dead code; wired to `andQuery` via `BM25Ranker.score()`.
- `UnicodeTokenizer.minLen` hardcoded as 1 in manifest; now read from the tokenizer instance.
- BM25 parity tests used a cross-repo import of `agentdb/dist/text-index.js`; replaced with a self-contained hand-coded reference implementation.
