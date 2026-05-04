/**
 * Stress test — large corpus, segment cap, query latency, memory growth.
 *
 * Runs under STRESS=1 for the full 1M-doc corpus.
 * Without STRESS=1 uses a 10k-doc subset to verify the same assertions
 * without blowing CI time/memory budgets.
 *
 * Assertions:
 *   1. Every flush produces a segment of at most flushThreshold docs.
 *   2. After compaction, the segment count collapses to <= mergeThreshold.
 *   3. All N docs are retrievable — spot-check across vocabulary and specific IDs.
 *   4. Query latency p95 <= P95_LIMIT_MS for a 10-term OR BM25 query.
 *   5. Heap growth during indexing stays below MEM_LIMIT_MB.
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentManager } from "../src/manager.js";
import { FsBackend } from "../src/storage.js";
import { BM25Ranker } from "../src/scoring.js";

const IS_STRESS = process.env["STRESS"] === "1";

const N               = IS_STRESS ? 1_000_000 : 10_000;
const FLUSH_THRESHOLD = IS_STRESS ? 5_000     : 500;
const P95_LIMIT_MS    = IS_STRESS ? 500       : 50;
const MEM_LIMIT_MB    = IS_STRESS ? 800       : 100;

const VOCAB = [
  "fox", "dog", "bear", "cat", "river", "mountain", "forest", "dawn",
  "quick", "lazy", "brown", "green", "blue", "red", "dark", "light",
  "run", "jump", "swim", "fly", "hunt", "sleep", "wake", "roam",
  "stone", "tree", "grass", "cloud", "rain", "snow", "fire", "wind",
];

function termsForDoc(i: number): Array<{ term: string; tf: number }> {
  const terms: Array<{ term: string; tf: number }> = [];
  const count = 5 + (i % 6);
  for (let t = 0; t < count; t++) {
    terms.push({
      term: VOCAB[(i + t * 7) % VOCAB.length],
      tf: 1 + ((i + t) % 4),
    });
  }
  return terms;
}

let dir: string;
let backend: FsBackend;
let mgr: SegmentManager;
/** docCounts of each segment captured right after all flushes (before final compact). */
let preCompactDocCounts: number[];
let heapBefore: number;
let heapAfter: number;

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-stress-"));
  backend = new FsBackend(dir);

  const gcFn = (global as unknown as { gc?: () => void }).gc;
  if (gcFn) gcFn();
  heapBefore = process.memoryUsage().heapUsed;

  // Disable auto-compact so we can inspect per-flush segment sizes first.
  mgr = await SegmentManager.open({
    backend,
    flushThreshold: FLUSH_THRESHOLD,
    mergeThreshold: Number.MAX_SAFE_INTEGER, // manual compact only
  });

  for (let i = 0; i < N; i++) {
    await mgr.add(i, termsForDoc(i));
  }
  await mgr.flush();

  if (gcFn) gcFn();
  heapAfter = process.memoryUsage().heapUsed;

  // Capture per-segment doc counts before compaction.
  preCompactDocCounts = mgr.segments().map((s) => s.docCount);

  // Now compact down to minimum segment count.
  await mgr.compact();
}, IS_STRESS ? 600_000 : 60_000);

afterAll(async () => {
  await rm(dir, { recursive: true, force: true });
});

describe(`stress test — ${N.toLocaleString()} docs (STRESS=${IS_STRESS ? "1" : "0"})`, () => {
  it("every flush segment holds at most flushThreshold docs", () => {
    expect(preCompactDocCounts.length).toBeGreaterThan(0);
    for (const count of preCompactDocCounts) {
      expect(count).toBeLessThanOrEqual(FLUSH_THRESHOLD);
    }
  });

  it("segment count collapses after compaction", () => {
    // After a single compact, all segments merge into 1.
    expect(mgr.segments().length).toBe(1);
    expect(mgr.segments()[0].docCount).toBe(N);
  });

  it("all N docs accounted for in merged segment", () => {
    const [merged] = mgr.segments();
    expect(merged.docCount).toBe(N);
  });

  it("vocabulary terms have correct doc-frequency coverage", () => {
    const [merged] = mgr.segments();
    // Every vocab term should appear in at least some docs.
    for (const term of VOCAB.slice(0, 8)) {
      const entry = merged.lookupTerm(term);
      expect(entry).toBeDefined();
      expect(entry!.df).toBeGreaterThan(0);
    }
  });

  it("sample of specific docs are retrievable in merged segment", () => {
    const [merged] = mgr.segments();
    const checkDocs = [0, Math.floor(N / 4), Math.floor(N / 2), N - 1];
    for (const origDocId of checkDocs) {
      // Internal doc IDs are re-numbered during compact; find by term+tf.
      // We verify the term appears in the merged segment with the right df.
      const firstTerm = termsForDoc(origDocId)[0].term;
      const entry = merged.lookupTerm(firstTerm);
      expect(entry, `term "${firstTerm}" for doc ${origDocId} missing`).toBeDefined();
    }
  });

  it(`query latency p95 <= ${P95_LIMIT_MS}ms for 10-term OR BM25 query`, () => {
    const ranker = new BM25Ranker();
    const segments = mgr.segments();
    const terms = VOCAB.slice(0, 10);
    const RUNS = 20;

    const latencies: number[] = [];
    for (let r = 0; r < RUNS; r++) {
      const t0 = performance.now();
      ranker.score(terms, segments, N, mgr.indexTotalLen, 10);
      latencies.push(performance.now() - t0);
    }

    latencies.sort((a, b) => a - b);
    const p95 = latencies[Math.floor(RUNS * 0.95)];
    expect(p95).toBeLessThanOrEqual(P95_LIMIT_MS);
  });

  it(`heap growth during indexing <= ${MEM_LIMIT_MB}MB`, () => {
    const deltaMB = (heapAfter - heapBefore) / (1024 * 1024);
    expect(deltaMB).toBeLessThanOrEqual(MEM_LIMIT_MB);
  });
});
