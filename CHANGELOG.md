# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com),
and this project adheres to [Semantic Versioning](https://semver.org).

## [Unreleased]

### Added
- Initial repo scaffold (package.json, tsconfig, eslint, vitest, src/index.ts).
- **Posting list codec (`src/codec.ts`)** — `encodeVByte`/`decodeVByte` (7-bit continuation, handles >32-bit values via `Math.floor` division and multiplication to avoid signed-integer overflow), `encodePostings`/`decodePostings` (delta-encoded doc IDs + VByte tf; format: `vbyte(count) | [vbyte(delta) vbyte(tf)]*`), `postingIterator` (lazy decode for query path). 19 tests covering round-trip identity, delta math, edge cases (empty, single entry, large IDs up to 2^35, tf=0/65535), partial iterator advance, and size sanity.
- **Term dictionary v0.1 (`src/term-dict.ts`)** — `TermDict` class with sorted array of `(term, postingsOffset, postingsLength, df)` entries; `add`, `lookup` (binary search, O(log n)), `serialize`/`deserialize` (compact binary: uint32 count + per-entry uint16 termLen + UTF-8 term bytes + three uint32 fields), `fromMap` (sorts unsorted map for segment writer). 20 tests: empty dict, single/many entries, first/middle/last binary search, 1000-entry dict, Unicode terms (café, 東京), 100-entry round-trip, `fromMap` sort correctness.
- **StorageBackend abstraction (`src/storage.ts`)** — `StorageBackend` interface (`readBlob`, `writeBlob`, `listBlobs`, `deleteBlob`, optional `isLocalFs`); `FsBackend` implementation with atomic writes (`.tmp` + rename), prefix-filtered `listBlobs` (root-flat, no recursive walk), idempotent `deleteBlob`, recursive directory creation. 12 tests: round-trip, atomic write (no stale `.tmp`), overwrite, missing-file errors, prefix filtering, empty-dir and no-match list, delete idempotency, nested dirs, interface compat (FsBackend + mock S3-shape backend).
- **Lint script extended to cover `tests/`** — `eslint src/ tests/`.
- **Segment writer + reader (`src/segment.ts`, `src/crc32.ts`)** — `SegmentWriter` accumulates `addPosting`/`setDocLength` calls in any order, sorts by term and doc ID, then `flush(id, backend)` writes a single binary `.seg` file atomically (`.seg.tmp` → rename via backend). `SegmentReader.open` verifies CRC32 for all three regions (postings, sidecar, dict), throws `SegmentCorruptionError` naming the corrupted region on mismatch, and exposes `lookupTerm`, `postings` (lazy iterator), `decodePostings` (full materialization), `docLen`, `terms` (sorted generator). File layout: `[postings region][doc-length sidecar][term dictionary][52-byte footer with magic, version, offsets, CRC32s]`. Pure-JS CRC32 in `src/crc32.ts` (IEEE 802.3 polynomial, table-driven). 14 tests: round-trip (basic corpus + 100-term), doc lengths, termCount/docCount, df correctness, posting iterator vs full decode, missing term iterator, sorted terms, out-of-order addPosting, CRC corruption detection for postings/footer/dict regions.
