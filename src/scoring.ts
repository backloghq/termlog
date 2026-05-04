/**
 * BM25 scoring layer — mirrors agentdb's TextIndex.searchScored() formula.
 *
 * Formula:
 *   idf       = log((N - df + 0.5) / (df + 0.5) + 1)
 *   norm      = k1 * (1 - b + b * (dl / avgdl))
 *   termScore = idf * tf * (k1 + 1) / (tf + norm)
 *   docScore  = Σ termScore  (OR semantics — sum across all query terms)
 *
 * Defaults: k1 = 1.2, b = 0.75  (Lucene/agentdb defaults)
 */

import type { SegmentReader } from "./segment.js";
import { orQuery } from "./query.js";

export interface BM25Opts {
  k1?: number;
  b?: number;
}

export interface ScoredDoc {
  docId: number;
  score: number;
}

/**
 * Pure BM25 term-score function. Mirrors agentdb's formula exactly.
 * Returns the contribution of one term to a document's score.
 *
 * @param tf    - term frequency in the document
 * @param dl    - document length (total token count)
 * @param df    - document frequency (number of docs containing this term)
 * @param N     - total number of documents in the index
 * @param k1    - term saturation parameter (default 1.2)
 * @param b     - length normalization parameter (default 0.75)
 * @param avgdl - average document length; falls back to 1 if 0 (empty corpus)
 */
export function bm25Score(
  tf: number,
  dl: number,
  df: number,
  N: number,
  k1: number,
  b: number,
  avgdl: number,
): number {
  const effectiveAvgdl = avgdl > 0 ? avgdl : 1;
  const idf = Math.log((N - df + 0.5) / (df + 0.5) + 1);
  const norm = k1 * (1 - b + b * (dl / effectiveAvgdl));
  return idf * (tf * (k1 + 1)) / (tf + norm);
}

/**
 * BM25 ranker — wraps the OR query iterator and scores each candidate document.
 *
 * Usage:
 *   const ranker = new BM25Ranker(segments, { N, totalLen });
 *   const results = ranker.score(terms, { limit: 10 });
 *
 * The caller must supply `N` (total doc count) and `totalLen` (sum of all doc
 * lengths) because those statistics live in the manifest, not in individual
 * segments. The segment readers supply per-term `df` and per-doc `dl`.
 */
export class BM25Ranker {
  private readonly k1: number;
  private readonly b: number;

  constructor(opts?: BM25Opts) {
    this.k1 = opts?.k1 ?? 1.2;
    this.b = opts?.b ?? 0.75;
  }

  /**
   * Score all documents matching any of `terms` using OR semantics.
   * Returns results sorted by score desc, ties broken by docId asc
   * (mirrors agentdb's sort order).
   *
   * @param terms    - query terms (pre-tokenized)
   * @param segments - segment readers to search
   * @param N        - total indexed document count
   * @param totalLen - sum of all document lengths
   * @param limit    - optional cap on results
   */
  score(
    terms: string[],
    segments: SegmentReader[],
    N: number,
    totalLen: number,
    limit?: number,
  ): ScoredDoc[] {
    if (terms.length === 0 || N === 0) return [];

    const avgdl = totalLen > 0 ? totalLen / N : 1;
    const { k1, b } = this;

    // Pre-compute df for each unique term across all segments.
    const dfMap = new Map<string, number>();
    for (const term of new Set(terms)) {
      let df = 0;
      for (const seg of segments) {
        const entry = seg.lookupTerm(term);
        if (entry) df += entry.df;
      }
      dfMap.set(term, df);
    }

    // OR-iterate all candidates; accumulate per-doc scores.
    const scores = new Map<number, number>();

    for (const { docId, tfs } of orQuery(terms, segments)) {
      let docScore = 0;
      // Look up dl: docLen lives in the segment that contains this docId.
      let dl = 0;
      for (const seg of segments) {
        const l = seg.docLen(docId);
        if (l > 0) { dl = l; break; }
      }

      for (const [term, tf] of tfs) {
        if (!tf) continue; // skip tf=0 placeholders (mirrors agentdb)
        const df = dfMap.get(term) ?? 0;
        if (df === 0) continue;
        docScore += bm25Score(tf, dl, df, N, k1, b, avgdl);
      }

      if (docScore > 0) {
        scores.set(docId, (scores.get(docId) ?? 0) + docScore);
      }
    }

    if (scores.size === 0) return [];

    let results: ScoredDoc[] = Array.from(scores.entries()).map(([docId, score]) => ({
      docId,
      score,
    }));

    // Sort: score desc, then tie-break by string-lexicographic docId asc.
    // agentdb stores IDs as strings; its tie-break is `a.id < b.id` (lex), so
    // "11" < "2" — not numeric. Mirror that here for parity.
    results.sort((a, z) => {
      if (z.score !== a.score) return z.score - a.score;
      const sa = String(a.docId); const sz = String(z.docId);
      return sa < sz ? -1 : sa > sz ? 1 : 0;
    });

    if (limit !== undefined) results = results.slice(0, limit);
    return results;
  }
}
