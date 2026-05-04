export const VERSION = "0.1.0";

// TermLog facade — primary user-facing API
export type { TermLogOptions } from "./termlog.js";
export { TermLog, MappingCorruptionError } from "./termlog.js";

// Tokenizer
export type { Tokenizer } from "./tokenizer.js";
export { UnicodeTokenizer, DEFAULT_TOKENIZER } from "./tokenizer.js";

// Storage
export type { StorageBackend } from "./storage.js";
export { FsBackend } from "./storage.js";

// Codec
export type { Posting } from "./codec.js";
export { encodePostings, decodePostings, postingIterator, encodeVByte, decodeVByte } from "./codec.js";

// Term dictionary
export type { DictEntry } from "./term-dict.js";
export { TermDict } from "./term-dict.js";

// Segment
export { SegmentWriter, SegmentReader, SegmentCorruptionError } from "./segment.js";

// Manager
export type { ManifestSegmentEntry, TokenizerConfig, SegmentManagerOpts } from "./manager.js";
export { SegmentManager, ManifestCorruptionError, IndexLockedError } from "./manager.js";

// Query
export type { QueryPosting } from "./query.js";
export { SegmentPostingIter, MultiSegmentIter, andQuery, orQuery, buildTombstoneSet } from "./query.js";

// Scoring
export type { BM25Opts, ScoredDoc } from "./scoring.js";
export { bm25Score, BM25Ranker } from "./scoring.js";
