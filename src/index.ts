import pkg from "../package.json" with { type: "json" };
/** Current package version — sourced from package.json at build time. */
export const VERSION: string = pkg.version;

// TermLog facade — primary user-facing API
export type { TermLogOptions } from "./termlog.js";
export { TermLog, MappingCorruptionError, TokenizerMismatchError } from "./termlog.js";

// Tokenizer
export type { Tokenizer } from "./tokenizer.js";
export { UnicodeTokenizer, DEFAULT_TOKENIZER } from "./tokenizer.js";

// Storage
export type { StorageBackend, BlobWriteStream } from "./storage.js";
export { FsBackend, WriteStreamError } from "./storage.js";

/**
 * @internal Low-level building blocks — not covered by semver stability guarantees.
 * Subject to breaking changes across minor versions.
 */

// Codec
/** @internal */
export type { Posting } from "./codec.js";
/** @internal */
export { encodePostings, decodePostings, postingIterator, encodeVByte, decodeVByte } from "./codec.js";

// Term dictionary
/** @internal */
export type { DictEntry } from "./term-dict.js";
/** @internal */
export { TermDict } from "./term-dict.js";

// Segment
/** @internal */
export { SegmentWriter, SegmentReader, SegmentCorruptionError } from "./segment.js";

// Manager
/** @internal */
export type { ManifestSegmentEntry, TokenizerConfig, SegmentManagerOpts } from "./manager.js";
/** @internal */
export { SegmentManager, ManifestCorruptionError, ManifestVersionError, IndexLockedError, DEFAULT_FLUSH_THRESHOLD } from "./manager.js";

// Query
/** @internal */
export type { QueryPosting } from "./query.js";
/** @internal */
export { SegmentPostingIter, MultiSegmentIter, andQuery, orQuery, buildTombstoneSet } from "./query.js";

// Scoring
/** @internal */
export type { BM25Opts, ScoredDoc } from "./scoring.js";
/** @internal */
export { bm25Score, BM25Ranker } from "./scoring.js";
