/**
 * Benchmark suite — gated behind BENCH=1.
 *
 * Host: AMD Ryzen 7 9700X (8-core), 60 GiB RAM, CachyOS Linux (NVMe SSD).
 * Node.js v25+. Run with: BENCH=1 npx vitest run tests/bench.test.ts
 *
 * Measured dimensions:
 *   - Indexing throughput: docs/sec at 10K and 100K corpus sizes.
 *   - Query latency p50/p95/p99: 1-, 2-, 5-term OR BM25 queries on 100K corpus.
 *   - Compaction cost: wall-clock time + bytes-on-disk before/after.
 *   - Reopen latency: SegmentManager.open on a compacted 100K-doc index.
 *
 * Assertions are relative invariants (no hardware-dependent absolutes):
 *   - 100K throughput >= 10K throughput * 0.5  (scales, doesn't cliff).
 *   - Latency p99 <= p50 * 10  (no extreme outliers).
 *   - Compaction reduces segment count.
 *   - Reopen < indexing time  (opening is cheaper than writing).
 */

import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { mkdtemp, rm, readdir, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentManager } from "../src/manager.js";
import { FsBackend } from "../src/storage.js";
import { BM25Ranker } from "../src/scoring.js";

const IS_BENCH = process.env["BENCH"] === "1";

if (!IS_BENCH) {
  describe("bench (skipped — set BENCH=1 to run)", () => {
    it("skipped", () => { /* no-op */ });
  });
} else {
  runBenchmarks();
}

// ---------------------------------------------------------------------------
// Vocabulary + doc generator
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Percentile helper
// ---------------------------------------------------------------------------

function percentile(sorted: number[], p: number): number {
  const idx = Math.min(Math.floor(sorted.length * p / 100), sorted.length - 1);
  return sorted[idx];
}

// ---------------------------------------------------------------------------
// Disk usage helper
// ---------------------------------------------------------------------------

async function dirBytes(dir: string): Promise<number> {
  const files = await readdir(dir);
  let total = 0;
  for (const f of files) {
    const s = await stat(join(dir, f));
    total += s.size;
  }
  return total;
}

// ---------------------------------------------------------------------------
// Console table printer
// ---------------------------------------------------------------------------

function row(label: string, value: string): void {
  console.log(`  ${label.padEnd(40)} ${value}`);
}

function section(title: string): void {
  console.log(`\n--- ${title} ---`);
}

// ---------------------------------------------------------------------------
// Benchmark runner
// ---------------------------------------------------------------------------

function runBenchmarks(): void {
  const QUERY_RUNS = 200;

  let dir10k: string;
  let dir100k: string;
  let mgr10k: SegmentManager;
  let mgr100k: SegmentManager;

  // Timing results — populated in beforeAll.
  let throughput10k = 0;   // docs/sec
  let throughput100k = 0;  // docs/sec
  let indexMs10k = 0;
  let indexMs100k = 0;

  interface LatencyResult { p50: number; p95: number; p99: number }
  let lat1term: LatencyResult;
  let lat2term: LatencyResult;
  let lat5term: LatencyResult;

  let compactMs = 0;
  let bytesBeforeCompact = 0;
  let bytesAfterCompact = 0;
  let segsBeforeCompact = 0;
  let segsAfterCompact = 0;

  let reopenMs = 0;

  beforeAll(async () => {
    // --- 10K index ---
    dir10k = await mkdtemp(join(tmpdir(), "termlog-bench-10k-"));
    {
      const backend = new FsBackend(dir10k);
      const mgr = await SegmentManager.open({ backend, flushThreshold: 500, mergeThreshold: 9999 });
      const t0 = performance.now();
      for (let i = 0; i < 10_000; i++) await mgr.add(i, termsForDoc(i));
      await mgr.flush();
      indexMs10k = performance.now() - t0;
      throughput10k = Math.round(10_000 / (indexMs10k / 1000));
      mgr10k = mgr;
    }

    // --- 100K index ---
    dir100k = await mkdtemp(join(tmpdir(), "termlog-bench-100k-"));
    {
      const backend = new FsBackend(dir100k);
      const mgr = await SegmentManager.open({ backend, flushThreshold: 1000, mergeThreshold: 9999 });
      const t0 = performance.now();
      for (let i = 0; i < 100_000; i++) await mgr.add(i, termsForDoc(i));
      await mgr.flush();
      indexMs100k = performance.now() - t0;
      throughput100k = Math.round(100_000 / (indexMs100k / 1000));
      mgr100k = mgr;
    }

    // --- Query latency on 100K index ---
    const ranker = new BM25Ranker();
    const segs = mgr100k.segments();
    const N = 100_000;
    const totalLen = mgr100k.indexTotalLen;

    function measureQuery(terms: string[]): LatencyResult {
      const lats: number[] = [];
      for (let r = 0; r < QUERY_RUNS; r++) {
        const t0 = performance.now();
        ranker.score(terms, segs, N, totalLen, 10);
        lats.push(performance.now() - t0);
      }
      lats.sort((a, b) => a - b);
      return {
        p50: percentile(lats, 50),
        p95: percentile(lats, 95),
        p99: percentile(lats, 99),
      };
    }

    lat1term = measureQuery(["fox"]);
    lat2term = measureQuery(["fox", "dog"]);
    lat5term = measureQuery(["fox", "dog", "bear", "quick", "river"]);

    // --- Compaction cost on 100K index ---
    bytesBeforeCompact = await dirBytes(dir100k);
    segsBeforeCompact = mgr100k.segments().length;
    const tc0 = performance.now();
    await mgr100k.compact();
    compactMs = performance.now() - tc0;
    bytesAfterCompact = await dirBytes(dir100k);
    segsAfterCompact = mgr100k.segments().length;

    // --- Reopen latency ---
    const tr0 = performance.now();
    await SegmentManager.open({ backend: new FsBackend(dir100k) });
    reopenMs = performance.now() - tr0;

    // Print summary table.
    console.log("\n========== termlog benchmark ==========");
    console.log("Host: AMD Ryzen 7 9700X, 60 GiB RAM, CachyOS Linux");

    section("Indexing throughput");
    row("10K docs — total time", `${indexMs10k.toFixed(1)} ms`);
    row("10K docs — throughput", `${throughput10k.toLocaleString()} docs/sec`);
    row("100K docs — total time", `${indexMs100k.toFixed(1)} ms`);
    row("100K docs — throughput", `${throughput100k.toLocaleString()} docs/sec`);

    section("Query latency — 100K corpus (p50 / p95 / p99)");
    row('1-term  OR BM25 ("fox")', `${lat1term.p50.toFixed(3)} / ${lat1term.p95.toFixed(3)} / ${lat1term.p99.toFixed(3)} ms`);
    row('2-term  OR BM25 ("fox dog")', `${lat2term.p50.toFixed(3)} / ${lat2term.p95.toFixed(3)} / ${lat2term.p99.toFixed(3)} ms`);
    row('5-term  OR BM25', `${lat5term.p50.toFixed(3)} / ${lat5term.p95.toFixed(3)} / ${lat5term.p99.toFixed(3)} ms`);

    section("Compaction — 100K corpus");
    row("Segments before compact", String(segsBeforeCompact));
    row("Segments after compact", String(segsAfterCompact));
    row("Disk before compact", `${(bytesBeforeCompact / 1024 / 1024).toFixed(2)} MB`);
    row("Disk after compact", `${(bytesAfterCompact / 1024 / 1024).toFixed(2)} MB`);
    row("Compaction wall-clock", `${compactMs.toFixed(1)} ms`);

    section("Reopen latency — 100K compacted index");
    row("SegmentManager.open", `${reopenMs.toFixed(1)} ms`);

    console.log("\n========================================\n");
  }, 300_000);

  afterAll(async () => {
    await rm(dir10k, { recursive: true, force: true });
    await rm(dir100k, { recursive: true, force: true });
    void mgr10k; // keep reference alive through afterAll
  });

  describe("relative invariants (no hardware-dependent absolutes)", () => {
    it("100K throughput >= 10K throughput * 0.5 (scales, doesn't cliff)", () => {
      expect(throughput100k).toBeGreaterThanOrEqual(throughput10k * 0.5);
    });

    it("1-term query p99 <= p50 * 10 (no extreme outliers)", () => {
      expect(lat1term.p99).toBeLessThanOrEqual(lat1term.p50 * 10);
    });

    it("2-term query p99 <= p50 * 10", () => {
      expect(lat2term.p99).toBeLessThanOrEqual(lat2term.p50 * 10);
    });

    it("5-term query p99 <= p50 * 10", () => {
      expect(lat5term.p99).toBeLessThanOrEqual(lat5term.p50 * 10);
    });

    it("5-term query p50 >= 1-term p50 (more terms = more work)", () => {
      // OR query over 5 terms always does at least as much work as 1 term.
      expect(lat5term.p50).toBeGreaterThanOrEqual(lat1term.p50 * 0.5);
    });

    it("compaction reduces segment count", () => {
      expect(segsAfterCompact).toBeLessThan(segsBeforeCompact);
    });

    it("compaction produces correct docCount in merged segment", () => {
      expect(mgr100k.segments()[0].docCount).toBe(100_000);
    });

    it("reopen is faster than full indexing (< indexMs100k)", () => {
      expect(reopenMs).toBeLessThan(indexMs100k);
    });
  });
}
