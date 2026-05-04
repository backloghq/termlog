// @backloghq/termlog — log-structured full-text search index.
// Segment-based posting lists, LSM compaction, BM25 ranking. Pure TypeScript.
//
// Public API (incremental — see DESIGN.md and the backlog tasks for the
// build-out plan):
//
//   import { TermLog } from "@backloghq/termlog";
//
//   const idx = new TermLog({ dir: "./term-data" });
//   await idx.open();
//   await idx.add("doc1", "the quick brown fox");
//   const hits = await idx.search("fox");      // BM25-ranked
//   await idx.compact();                       // merge segments
//   await idx.close();

export const VERSION = "0.0.1";
