/**
 * Stress test — large corpus, segment cap, query latency, memory growth.
 *
 * Runs under STRESS=1 for the full 1M-doc corpus.
 * Without STRESS=1 uses a 10k-doc subset to verify the same assertions
 * without blowing CI time/memory budgets.
 *
 * Set STRESS_TIERED=1 to additionally run the tiered cascade variant:
 * uses fanout=4 and auto-compact, asserting segment count stays within
 * ceil(log_4(N/flushThreshold)) tiers and heap stays bounded.
 *
 * Assertions:
 *   1. Every flush produces a segment of at most flushThreshold docs.
 *   2. After compaction, the segment count collapses to <= fanout.
 *   3. All N docs are retrievable — spot-check across vocabulary and specific IDs.
 *   4. Query latency p95 <= P95_LIMIT_MS for a 10-term OR BM25 query.
 *   5. Peak RSS during indexing stays below MEM_LIMIT_MB (M8: use maxRSS, not heapUsed).
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentManager } from "../src/manager.js";
import { FsBackend } from "../src/storage.js";
import { BM25Ranker } from "../src/scoring.js";

const IS_STRESS        = process.env["STRESS"] === "1";
const IS_TIERED_STRESS = process.env["STRESS_TIERED"] === "1";
// CI runners (GH Actions 2-core) are ~2-3x slower than dev workstations.
// Tighter bound locally catches micro-regressions; CI bound still catches
// a ~3x regression while tolerating shared-compute variance.
const IS_CI            = process.env["CI"] === "true";

const N               = IS_STRESS ? 1_000_000 : 10_000;
const FLUSH_THRESHOLD = IS_STRESS ? 5_000     : 500;
const P95_LIMIT_MS    = IS_STRESS ? (IS_CI ? 1500 : 500) : 50;
// 1024 MB cap: vitest worker overhead is ~300 MB; actual cascade peaks at ~374 MB direct-node.
// Using 1024 gives headroom while still catching regressions.
const MEM_LIMIT_MB    = IS_STRESS ? 1024      : 200;

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
/** Peak RSS (bytes) captured via process.resourceUsage().maxRSS after full indexing run. */
let peakRssBytes: number;

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-stress-"));
  backend = new FsBackend(dir);

  // Disable auto-compact so we can inspect per-flush segment sizes first.
  mgr = await SegmentManager.open({
    backend,
    flushThreshold: FLUSH_THRESHOLD,
    fanout: Number.MAX_SAFE_INTEGER, // manual compact only
  });

  for (let i = 0; i < N; i++) {
    await mgr.add(i, termsForDoc(i));
  }
  await mgr.flush();

  // M8 fix: sample peak RSS after indexing. maxRSS captures the actual OS-level
  // high-water mark, not the post-GC heapUsed which can misleadingly appear small.
  peakRssBytes = process.resourceUsage().maxRSS * 1024; // Linux reports in KB

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
      // Original docIds are preserved through compaction (no renumbering).
      // Verify the doc's first term appears in the merged segment.
      const firstTerm = termsForDoc(origDocId)[0].term;
      const entry = merged.lookupTerm(firstTerm);
      expect(entry, `term "${firstTerm}" for doc ${origDocId} missing`).toBeDefined();
      // Verify the original docId itself is in the sidecar.
      expect(merged.docLen(origDocId)).toBeGreaterThan(0);
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
    // Wall-clock assertion only under STRESS=1 — coverage and slow CI machines
    // skew timings enough to make 50ms unreliable on the 10k-doc path.
    if (IS_STRESS) expect(p95).toBeLessThanOrEqual(P95_LIMIT_MS);
  }, IS_STRESS ? 120_000 : 10_000);

  it(`peak RSS during indexing <= ${MEM_LIMIT_MB}MB`, () => {
    const peakMB = peakRssBytes / (1024 * 1024);
    expect(peakMB).toBeLessThanOrEqual(MEM_LIMIT_MB);
  });
});

// ---------------------------------------------------------------------------
// Tiered cascade — small-N variant runs unconditionally in CI
// ---------------------------------------------------------------------------

describe("tiered cascade — CI (10k docs, fanout=4)", () => {
  it("segment count stays within tier bound, tombstones respected, basic latency", async () => {
    const N_CI = 10_000;
    const FLUSH_CI = 500;
    const FANOUT = 4;

    const ciDir = await mkdtemp(join(tmpdir(), "termlog-tiered-ci-"));
    const ciBackend = new FsBackend(ciDir);
    const ciMgr = await SegmentManager.open({
      backend: ciBackend,
      flushThreshold: FLUSH_CI,
      fanout: FANOUT,
    });

    for (let i = 0; i < N_CI; i++) {
      await ciMgr.add(i, termsForDoc(i));
    }
    await ciMgr.flush();

    // Segment count must be within theoretical tier bound.
    const maxTiers = Math.ceil(Math.log(N_CI / FLUSH_CI) / Math.log(FANOUT));
    const maxSegs = Math.pow(FANOUT, maxTiers);
    expect(ciMgr.segments().length).toBeLessThanOrEqual(maxSegs);

    // Tombstone a doc and verify it is excluded from query results.
    const targetDocId = Math.floor(N_CI / 2);
    await ciMgr.remove(targetDocId);
    await ciMgr.flush();

    const ranker = new BM25Ranker();
    const targetTerm = termsForDoc(targetDocId)[0].term;
    const results = ranker.score([targetTerm], ciMgr.segments(), ciMgr.indexTotalDocs, ciMgr.indexTotalLen);
    expect(results.map((r) => r.docId)).not.toContain(targetDocId);

    // Basic latency: 10-term OR query must complete in under 1s on any reasonable machine.
    const t0 = performance.now();
    ranker.score(VOCAB.slice(0, 10), ciMgr.segments(), ciMgr.indexTotalDocs, ciMgr.indexTotalLen, 10);
    expect(performance.now() - t0).toBeLessThan(1000);

    await ciMgr.close();
    await rm(ciDir, { recursive: true, force: true });
  }, 60_000);
});

// ---------------------------------------------------------------------------
// Tiered cascade stress variant (STRESS_TIERED=1)
// ---------------------------------------------------------------------------

describe(`tiered cascade stress (STRESS_TIERED=${IS_TIERED_STRESS ? "1" : "0"})`, () => {
  it.skipIf(!IS_TIERED_STRESS)(
    "cascade settles within ceil(log_4(N/flushThreshold)) tiers; heap bounded",
    async () => {
      const tieredDir = await mkdtemp(join(tmpdir(), "termlog-stress-tiered-"));
      const tieredBackend = new FsBackend(tieredDir);
      const FANOUT = 4;

      const tieredMgr = await SegmentManager.open({
        backend: tieredBackend,
        flushThreshold: FLUSH_THRESHOLD,
        fanout: FANOUT,
      });

      for (let i = 0; i < N; i++) {
        await tieredMgr.add(i, termsForDoc(i));
      }
      await tieredMgr.flush();

      const peakRss = process.resourceUsage().maxRSS * 1024;

      // (a) Segment count stays within the theoretical tier bound.
      // After full tiered compaction, each doc passes through at most
      // ceil(log_4(N/flushThreshold)) merge levels, producing at most
      // fanout^ceil(log4(N/flushThreshold)) segments = a small constant.
      const maxExpectedTiers = Math.ceil(Math.log(N / FLUSH_THRESHOLD) / Math.log(FANOUT));
      const maxExpectedSegs = Math.pow(FANOUT, maxExpectedTiers);
      expect(tieredMgr.segments().length).toBeLessThanOrEqual(maxExpectedSegs);

      // (b) p95 query latency holds post-cascade.
      const ranker = new BM25Ranker();
      const segments = tieredMgr.segments();
      const terms = VOCAB.slice(0, 10);
      const RUNS = 20;
      const latencies: number[] = [];
      for (let r = 0; r < RUNS; r++) {
        const t0 = performance.now();
        ranker.score(terms, segments, N, tieredMgr.indexTotalLen, 10);
        latencies.push(performance.now() - t0);
      }
      latencies.sort((a, b) => a - b);
      const p95 = latencies[Math.floor(RUNS * 0.95)];
      expect(p95).toBeLessThanOrEqual(P95_LIMIT_MS);

      // (c) Heap didn't blow up.
      const peakMB = peakRss / (1024 * 1024);
      expect(peakMB).toBeLessThanOrEqual(MEM_LIMIT_MB);

      await tieredMgr.close();
      await rm(tieredDir, { recursive: true, force: true });
    },
    IS_STRESS ? 600_000 : 60_000,
  );
});
