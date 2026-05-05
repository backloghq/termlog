# termlog

Log-structured, segment-based full-text search index for TypeScript. Pure TypeScript, zero native dependencies. Built to scale past the single-blob limit that constrained AgentDB's pre-termlog text index.

## What It Is

A pluggable inverted index with BM25 ranking. Writes accumulate in an in-memory buffer and flush to immutable on-disk segments. Segments are merged LSM-tree style (size-tiered, configurable fanout) so the corpus scales without per-file ceilings. Storage is abstracted via `StorageBackend` — local FS by default, S3 via the companion [@backloghq/termlog-s3](https://github.com/backloghq/termlog-s3) package.

## Architecture

```
<index-dir>/
  manifest.json            # Atomic — generation, segments, tokenizer config
  seg-<id>.seg             # Segment: postings + doc-length sidecar + tombstones + dict + footer
  docids.snap              # Snapshot of string↔number docId mapping
  docids.log               # Append-only journal of mapping deltas since last snap
  .lock                    # Advisory lockfile (single-writer per dir)
```

A flush builds a new `seg-<id>.seg`, opens the reader, then atomically commits the manifest. A cascade compaction merges fanout same-tier segments into one of the next tier. Tombstones for deleted docs are carried forward across partial merges.

### Key invariants

- **Manifest is source of truth** — segments not referenced by the manifest are orphans, cleaned on open.
- **All-or-nothing commit** — both `flushLocked` and `tieredCompactLocked` build new state into locals, await `writeManifest`, then assign all in-memory fields atomically. If `writeManifest` throws, no state mutates and the buffer/tombstones are preserved for retry.
- **Streaming writes** — `SegmentWriter.writeTerm` enforces strictly ascending lex order and streams postings directly through `BlobWriteStream` (no full-segment buffering). Memory peak at the top cascade is bounded by the sidecar's packed `Uint32Array` (~8 bytes/doc) and the surviving-doc Map (~64 bytes/doc).
- **Single writer per dir** — local FS uses an `O_EXCL` advisory lockfile with stale-PID auto-claim. S3 has no distributed lock — caller's responsibility.

## Project Structure

```
src/
  termlog.ts            # TermLog facade — string↔num docId mapping, tokenization, BM25 search
  manager.ts            # SegmentManager — flush, cascade compaction, manifest, recovery
  segment.ts            # SegmentWriter (streaming) + SegmentReader, Uint32Array sidecar
  storage.ts            # StorageBackend + BlobWriteStream interfaces + FsBackend + WriteStreamError
  codec.ts              # VByte + delta-encoded posting lists
  term-dict.ts          # Sorted on-disk term dictionary (binary search lookup)
  query.ts              # andQuery (zigzag), orQuery (k-way union), MultiSegmentIter (heap)
  scoring.ts            # bm25Score + BM25Ranker (top-k heap)
  tokenizer.ts          # UnicodeTokenizer (NFD normalization, configurable minLen)
  crc32.ts              # Streaming CRC32 (Crc32Stream)
  index.ts              # Public exports
tests/
  termlog.test.ts             # TermLog facade integration
  manager.test.ts             # SegmentManager + atomicity + manifest invariants
  segment.test.ts             # SegmentWriter/Reader + 1000-iteration fuzz
  storage.test.ts             # FsBackend createWriteStream atomicity + ENOSPC paths
  codec.test.ts               # VByte + delta encoding round-trip + boundaries
  term-dict.test.ts           # Term dictionary
  query.test.ts               # AND/OR query iterators
  scoring.test.ts             # BM25 numerical edges
  bm25-parity.test.ts         # BM25 parity vs reference implementation
  crash-recovery.test.ts      # Manifest invariants + orphan cleanup
  concurrency.test.ts         # Reader isolation, racing flush+compact, racing add+remove
  lockfile.test.ts            # Cross-process single-writer enforcement
  gaps.test.ts                # Crc32Stream parity + edge cases
  stress.test.ts              # 10k default + 1M-doc tiered cascade (STRESS=1 STRESS_TIERED=1)
```

## Dependencies

- **Runtime**: none. Zero native deps, zero npm runtime deps.
- **Optional companion**: [@backloghq/termlog-s3](https://github.com/backloghq/termlog-s3) for S3-backed indexes.

## Commands

```bash
npm run build          # tsc
npm run lint           # eslint src/ tests/
npm test               # vitest run (default 10k-doc cascade variant included)
npm run test:coverage  # vitest coverage
STRESS=1 STRESS_TIERED=1 npx vitest run tests/stress.test.ts  # full 1M-doc stress
```

## Coding Conventions

- Zero runtime dependencies — pure TypeScript
- Always use conventional commits: `type(scope): description`
- Always look up library/framework docs via Context7 before using APIs
- Lint before committing — all code must pass eslint
- All-or-nothing manifest commits — never mutate `this.X` on SegmentManager before `await writeManifest` resolves
- **IMPORTANT: On every commit, update ALL docs** — README.md, CLAUDE.md, CHANGELOG.md
- Update `CHANGELOG.md` on every change ([Keep a Changelog](https://keepachangelog.com) format)
- Tests use temp directories cleaned up after each test
- Stress tests run in CI on every push (not gated)

## Release Process

1. Update `CHANGELOG.md` with a new version entry
2. Bump version in `package.json`
3. Run `npm run build && npm run lint && npm test && STRESS=1 STRESS_TIERED=1 npx vitest run tests/stress.test.ts`
4. Commit, push, verify CI green
5. After merge: `git tag vX.Y.Z && git push --tags && npm publish --access public`
