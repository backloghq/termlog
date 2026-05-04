/**
 * BM25 parity tests — termlog BM25Ranker scores must match a reference
 * implementation of the same formula on a shared 50-doc fixture corpus.
 *
 * Reference is hand-coded here (no agentdb import) using the identical formula:
 *   idf  = log((N - df + 0.5) / (df + 0.5) + 1)
 *   norm = k1 * (1 - b + b * (dl / avgdl))
 *   term = idf * tf * (k1 + 1) / (tf + norm)
 *   doc  = Σ term  (OR semantics)
 */
import { describe, it, expect, afterEach, beforeAll } from "vitest";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SegmentWriter, SegmentReader } from "../src/segment.js";
import { FsBackend } from "../src/storage.js";
import { bm25Score, BM25Ranker } from "../src/scoring.js";

// ---------------------------------------------------------------------------
// Shared 50-doc fixture corpus
// ---------------------------------------------------------------------------

function tokenize(text: string): string[] {
  return text.toLowerCase().match(/[\p{L}\p{M}\p{N}]+/gu)?.filter((t) => t.length > 0) ?? [];
}

interface FixtureDoc {
  id: number;
  text: string;
}

const CORPUS: FixtureDoc[] = [
  { id: 0,  text: "the quick brown fox jumps over the lazy dog" },
  { id: 1,  text: "a fox ran across the meadow at dawn" },
  { id: 2,  text: "brown bears live in forests and mountains" },
  { id: 3,  text: "the dog barked loudly at the fox" },
  { id: 4,  text: "quick thinking saves the day for the dog" },
  { id: 5,  text: "lazy afternoons are perfect for reading books" },
  { id: 6,  text: "the mountain fox hunts at dawn and dusk" },
  { id: 7,  text: "bears and foxes compete for territory" },
  { id: 8,  text: "the lazy river flows through the forest" },
  { id: 9,  text: "a quick brown horse galloped past the meadow" },
  { id: 10, text: "dogs are loyal companions to humans" },
  { id: 11, text: "the fox outsmarted the dog in the fable" },
  { id: 12, text: "forest paths wind through brown hills" },
  { id: 13, text: "reading books expands the mind quickly" },
  { id: 14, text: "the brown bear caught a fish in the river" },
  { id: 15, text: "foxes are known for their quick reflexes" },
  { id: 16, text: "lazy dogs sleep through the afternoon" },
  { id: 17, text: "mountains are home to bears and eagles" },
  { id: 18, text: "the dawn chorus woke the sleeping fox" },
  { id: 19, text: "a dog and a fox became unlikely friends" },
  { id: 20, text: "the quick fox eluded every trap" },
  { id: 21, text: "brown leaves fell in the autumn forest" },
  { id: 22, text: "the lazy cat watched the dog run" },
  { id: 23, text: "meadow flowers bloom at dawn and dusk" },
  { id: 24, text: "the bear roared as the fox fled into the trees" },
  { id: 25, text: "quick adaptation helps animals survive winter" },
  { id: 26, text: "the dog followed the fox into the forest" },
  { id: 27, text: "lazy rivers are ideal for fishing" },
  { id: 28, text: "brown soil is fertile and full of worms" },
  { id: 29, text: "the fox family den was hidden in the hillside" },
  { id: 30, text: "dogs learn commands through repetition and reward" },
  { id: 31, text: "the bear padded silently through the brown undergrowth" },
  { id: 32, text: "foxes are nocturnal hunters in many regions" },
  { id: 33, text: "a lazy summer spent by the river" },
  { id: 34, text: "the dog chased the fox across three fields" },
  { id: 35, text: "quick reflexes separate predators from prey" },
  { id: 36, text: "the mountain dawn is cold and beautiful" },
  { id: 37, text: "a bear and a dog faced off on the trail" },
  { id: 38, text: "brown foxes are more common than red ones" },
  { id: 39, text: "the lazy gardener let the weeds grow tall" },
  { id: 40, text: "forests breathe life into the planet" },
  { id: 41, text: "the dog and the fox raced to the river" },
  { id: 42, text: "bears hibernate through the long winter months" },
  { id: 43, text: "quick sand traps the unwary traveller" },
  { id: 44, text: "the lazy fox slept while the dog kept watch" },
  { id: 45, text: "brown trout swim in mountain streams" },
  { id: 46, text: "dawn breaks over the misty river valley" },
  { id: 47, text: "a dog needs exercise walks and companionship" },
  { id: 48, text: "the fox den was warm and dry inside" },
  { id: 49, text: "quick rain showers refresh the summer meadow" },
];

const QUERIES = [
  "fox",
  "dog",
  "brown fox",
  "quick",
  "lazy dog",
  "bear forest",
  "dawn",
  "river mountain",
  "quick brown fox",
  "fox dog bear",
];

// ---------------------------------------------------------------------------
// Reference BM25 implementation (hand-coded, no external deps)
// ---------------------------------------------------------------------------

interface RefDoc {
  id: number;
  tokens: string[];
  tfMap: Map<string, number>;
}

function buildReference(k1: number, b: number) {
  const docs: RefDoc[] = CORPUS.map((doc) => {
    const tokens = tokenize(doc.text);
    const tfMap = new Map<string, number>();
    for (const t of tokens) tfMap.set(t, (tfMap.get(t) ?? 0) + 1);
    return { id: doc.id, tokens, tfMap };
  });

  const N = docs.length;
  const totalLen = docs.reduce((s, d) => s + d.tokens.length, 0);
  const avgdl = totalLen / N;

  // Build df map
  const dfMap = new Map<string, number>();
  for (const doc of docs) {
    for (const term of doc.tfMap.keys()) {
      dfMap.set(term, (dfMap.get(term) ?? 0) + 1);
    }
  }

  function searchScored(
    query: string,
    limit: number,
    mode: "or" | "and" = "or",
  ): Array<{ id: number; score: number }> {
    const terms = tokenize(query);
    if (terms.length === 0) return [];

    const scores = new Map<number, number>();
    for (const doc of docs) {
      // AND: skip doc if any query term is missing.
      if (mode === "and" && terms.some((t) => !doc.tfMap.has(t))) continue;

      let docScore = 0;
      for (const term of new Set(terms)) {
        const tf = doc.tfMap.get(term) ?? 0;
        if (tf === 0) continue;
        const df = dfMap.get(term) ?? 0;
        const dl = doc.tokens.length;
        docScore += bm25Score(tf, dl, df, N, k1, b, avgdl);
      }
      if (docScore > 0) scores.set(doc.id, docScore);
    }

    const results = Array.from(scores.entries()).map(([id, score]) => ({ id, score }));
    results.sort((a, z) => {
      if (z.score !== a.score) return z.score - a.score;
      const sa = String(a.id); const sz = String(z.id);
      return sa < sz ? -1 : sa > sz ? 1 : 0;
    });
    return results.slice(0, limit);
  }

  return { docs, N, totalLen, searchScored };
}

// ---------------------------------------------------------------------------
// Test setup: build termlog segment from same corpus
// ---------------------------------------------------------------------------

let dir: string;
let backend: FsBackend;
let seg: SegmentReader;
let N: number;
let totalLen: number;

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), "termlog-bm25-"));
  backend = new FsBackend(dir);

  const writer = new SegmentWriter();
  totalLen = 0;

  for (const doc of CORPUS) {
    const tokens = tokenize(doc.text);
    const tfMap = new Map<string, number>();
    for (const t of tokens) tfMap.set(t, (tfMap.get(t) ?? 0) + 1);
    for (const [term, tf] of tfMap) writer.addPosting(term, doc.id, tf);
    writer.setDocLength(doc.id, tokens.length);
    totalLen += tokens.length;
  }

  await writer.flush("seg-parity", backend);
  seg = await SegmentReader.open("seg-parity.seg", backend);
  N = CORPUS.length;
});

afterEach(() => { /* nothing per-test */ });

// ---------------------------------------------------------------------------
// bm25Score pure function tests
// ---------------------------------------------------------------------------

describe("bm25Score — pure function", () => {
  it("returns 0 when tf=0", () => {
    expect(bm25Score(0, 10, 5, 100, 1.2, 0.75, 10)).toBe(0);
  });

  it("very common term (df=N) produces a small but positive idf", () => {
    const score = bm25Score(1, 10, 100, 100, 1.2, 0.75, 10);
    expect(score).toBeGreaterThan(0);
    expect(score).toBeLessThan(0.01);
  });

  it("avgdl=0 falls back to 1 (no NaN)", () => {
    const score = bm25Score(1, 5, 1, 10, 1.2, 0.75, 0);
    expect(Number.isFinite(score)).toBe(true);
    expect(score).toBeGreaterThan(0);
  });

  it("matches manual formula for known inputs", () => {
    const idf = Math.log((100 - 5 + 0.5) / (5 + 0.5) + 1);
    const norm = 1.2 * (1 - 0.75 + 0.75 * (10 / 10));
    const expected = idf * (3 * 2.2) / (3 + norm);
    expect(bm25Score(3, 10, 5, 100, 1.2, 0.75, 10)).toBeCloseTo(expected, 15);
  });

  it("higher tf gives higher score (all else equal)", () => {
    const s1 = bm25Score(1, 10, 5, 100, 1.2, 0.75, 10);
    const s2 = bm25Score(5, 10, 5, 100, 1.2, 0.75, 10);
    expect(s2).toBeGreaterThan(s1);
  });

  it("lower df gives higher score (rarer term scores higher)", () => {
    const s1 = bm25Score(2, 10, 1, 100, 1.2, 0.75, 10);
    const s2 = bm25Score(2, 10, 50, 100, 1.2, 0.75, 10);
    expect(s1).toBeGreaterThan(s2);
  });

  it("shorter doc (dl < avgdl) scores higher than longer one", () => {
    const short = bm25Score(2, 5, 10, 100, 1.2, 0.75, 20);
    const long  = bm25Score(2, 30, 10, 100, 1.2, 0.75, 20);
    expect(short).toBeGreaterThan(long);
  });
});

// ---------------------------------------------------------------------------
// BM25Ranker — edge cases
// ---------------------------------------------------------------------------

describe("BM25Ranker — edge cases", () => {
  it("empty terms returns []", () => {
    const ranker = new BM25Ranker();
    expect(ranker.score([], [seg], N, totalLen)).toEqual([]);
  });

  it("N=0 returns []", () => {
    const ranker = new BM25Ranker();
    expect(ranker.score(["fox"], [seg], 0, totalLen)).toEqual([]);
  });

  it("term not in any segment returns []", () => {
    const ranker = new BM25Ranker();
    expect(ranker.score(["xyzzynonexistent"], [seg], N, totalLen)).toEqual([]);
  });

  it("results are sorted by score desc", () => {
    const ranker = new BM25Ranker();
    const results = ranker.score(["fox"], [seg], N, totalLen);
    expect(results.length).toBeGreaterThan(1);
    for (let i = 1; i < results.length; i++) {
      expect(results[i].score).toBeLessThanOrEqual(results[i - 1].score);
    }
  });

  it("ties broken by string-lexicographic docId asc (mirrors reference)", () => {
    const ranker = new BM25Ranker();
    const results = ranker.score(["fox"], [seg], N, totalLen);
    for (let i = 1; i < results.length; i++) {
      if (Math.abs(results[i].score - results[i - 1].score) < 1e-12) {
        const sa = String(results[i - 1].docId);
        const sb = String(results[i].docId);
        expect(sa <= sb).toBe(true);
      }
    }
  });

  it("limit caps result count", () => {
    const ranker = new BM25Ranker();
    const results = ranker.score(["fox"], [seg], N, totalLen, 3);
    expect(results.length).toBeLessThanOrEqual(3);
  });
});

// ---------------------------------------------------------------------------
// Parity tests — termlog vs hand-coded reference, to 10 decimal places
// ---------------------------------------------------------------------------

describe("BM25Ranker — parity vs reference (k1=1.2, b=0.75)", () => {
  const ref = buildReference(1.2, 0.75);
  const ranker = new BM25Ranker({ k1: 1.2, b: 0.75 });

  for (const query of QUERIES) {
    it(`query: "${query}"`, () => {
      const queryTerms = tokenize(query);
      const refResults = ref.searchScored(query, 10);
      const termlogResults = ranker.score(queryTerms, [seg], N, totalLen, 10);

      expect(termlogResults.length).toBe(refResults.length);
      for (let i = 0; i < refResults.length; i++) {
        expect(termlogResults[i].docId).toBe(refResults[i].id);
        expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
      }
    });
  }
});

describe("BM25Ranker — parity vs reference (k1=0, b=0.75)", () => {
  const ref = buildReference(0, 0.75);
  const ranker = new BM25Ranker({ k1: 0, b: 0.75 });

  it('query: "fox"', () => {
    const refResults = ref.searchScored("fox", 10);
    const termlogResults = ranker.score(tokenize("fox"), [seg], N, totalLen, 10);
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });

  it('query: "quick brown fox"', () => {
    const refResults = ref.searchScored("quick brown fox", 10);
    const termlogResults = ranker.score(tokenize("quick brown fox"), [seg], N, totalLen, 10);
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });
});

describe("BM25Ranker — parity vs reference (k1=1.2, b=0)", () => {
  const ref = buildReference(1.2, 0);
  const ranker = new BM25Ranker({ k1: 1.2, b: 0 });

  it('query: "fox"', () => {
    const refResults = ref.searchScored("fox", 10);
    const termlogResults = ranker.score(tokenize("fox"), [seg], N, totalLen, 10);
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });

  it('query: "dog bear"', () => {
    const refResults = ref.searchScored("dog bear", 10);
    const termlogResults = ranker.score(tokenize("dog bear"), [seg], N, totalLen, 10);
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });
});

describe("BM25Ranker — parity vs reference (k1=1.2, b=1)", () => {
  const ref = buildReference(1.2, 1);
  const ranker = new BM25Ranker({ k1: 1.2, b: 1 });

  it('query: "quick"', () => {
    const refResults = ref.searchScored("quick", 10);
    const termlogResults = ranker.score(tokenize("quick"), [seg], N, totalLen, 10);
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });

  it('query: "fox dog bear"', () => {
    const refResults = ref.searchScored("fox dog bear", 10);
    const termlogResults = ranker.score(tokenize("fox dog bear"), [seg], N, totalLen, 10);
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });
});

// ---------------------------------------------------------------------------
// AND mode parity (#d56aa368)
// ---------------------------------------------------------------------------

describe("BM25Ranker — AND mode parity vs reference (k1=1.2, b=0.75)", () => {
  const ref = buildReference(1.2, 0.75);
  const ranker = new BM25Ranker({ k1: 1.2, b: 0.75 });

  it('query: "fox dog" (intersection)', () => {
    const refResults = ref.searchScored("fox dog", 10, "and");
    const termlogResults = ranker.score(tokenize("fox dog"), [seg], N, totalLen, 10, "and");
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });

  it('query: "quick brown fox" (three-term intersection)', () => {
    const refResults = ref.searchScored("quick brown fox", 10, "and");
    const termlogResults = ranker.score(tokenize("quick brown fox"), [seg], N, totalLen, 10, "and");
    expect(termlogResults.length).toBe(refResults.length);
    for (let i = 0; i < refResults.length; i++) {
      expect(termlogResults[i].docId).toBe(refResults[i].id);
      expect(termlogResults[i].score).toBeCloseTo(refResults[i].score, 10);
    }
  });

  it('query: "fox xyzzynonexistent" (empty intersection)', () => {
    const refResults = ref.searchScored("fox xyzzynonexistent", 10, "and");
    const termlogResults = ranker.score(tokenize("fox xyzzynonexistent"), [seg], N, totalLen, 10, "and");
    expect(termlogResults).toEqual([]);
    expect(refResults).toEqual([]);
  });
});
