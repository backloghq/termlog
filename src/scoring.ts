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
import { andQuery, orQuery } from "./query.js";
import { MinHeap } from "./heap.js";

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
 * BM25 ranker — wraps the boolean query iterators and scores each candidate document.
 *
 * Usage:
 *   const ranker = new BM25Ranker({ k1: 1.2, b: 0.75 });
 *   const results = ranker.score(terms, segments, N, totalLen, limit, "or");
 *
 * Supports two modes:
 *   - "or"  (default) — union: every docId matching any term is scored.
 *   - "and"           — intersection: only docIds present in every term's posting list.
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
   * Score documents matching `terms` using the specified boolean mode.
   * Returns results sorted by score desc, ties broken by string-lexicographic docId asc.
   *
   * @param terms    - query terms (pre-tokenized)
   * @param segments - segment readers to search
   * @param N        - total indexed document count
   * @param totalLen - sum of all document lengths
   * @param limit    - optional cap on results (applied after sorting)
   * @param mode     - "or" (default) for union, "and" for intersection
   */
  score(
    terms: string[],
    segments: readonly SegmentReader[],
    N: number,
    totalLen: number,
    limit?: number,
    mode: "and" | "or" = "or",
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

    // Score each candidate. orQuery/andQuery emit each docId exactly once.
    const queryIter = mode === "and" ? andQuery(terms, segments) : orQuery(terms, segments);

    if (limit !== undefined && limit > 0) {
      // Path B: top-k min-heap — O(hits * log(limit)) instead of O(hits * log(hits)).
      // The heap root is the worst element (lowest score; lex-largest docId on tie)
      // so we can evict it when a better candidate arrives.
      const heap = new MinHeap<ScoredDoc>((a, z) => {
        if (a.score !== z.score) return a.score - z.score;
        // Same score: larger numId is worse → put at root (evicted first).
        return z.docId - a.docId;
      });

      for (const { docId, tfs, segIndex } of queryIter) {
        let docScore = 0;
        const dl = segments[segIndex]?.docLen(docId) ?? 0;
        for (const [term, tf] of tfs) {
          if (!tf) continue;
          const df = dfMap.get(term) ?? 0;
          if (df === 0) continue;
          docScore += bm25Score(tf, dl, df, N, k1, b, avgdl);
        }
        if (docScore <= 0) continue;

        if (heap.size < limit) {
          heap.push({ docId, score: docScore });
        } else {
          const worst = heap.peek()!;
          const betterThanWorst =
            docScore > worst.score ||
            (docScore === worst.score && docId < worst.docId);
          if (betterThanWorst) {
            heap.pop();
            heap.push({ docId, score: docScore });
          }
        }
      }

      if (heap.size === 0) return [];
      // Drain heap: arrives in worst-first order; reverse for score-desc, lex-asc.
      const results: ScoredDoc[] = [];
      while (heap.size > 0) results.push(heap.pop()!);
      results.reverse();
      return results;
    }

    // No limit: collect all hits, then sort. O(hits log hits).
    const results: ScoredDoc[] = [];
    for (const { docId, tfs, segIndex } of queryIter) {
      let docScore = 0;
      const dl = segments[segIndex]?.docLen(docId) ?? 0;
      for (const [term, tf] of tfs) {
        if (!tf) continue;
        const df = dfMap.get(term) ?? 0;
        if (df === 0) continue;
        docScore += bm25Score(tf, dl, df, N, k1, b, avgdl);
      }
      if (docScore > 0) results.push({ docId, score: docScore });
    }

    if (results.length === 0) return [];

    // Sort: score desc, tie-break by ascending numId.
    results.sort((a, z) => {
      if (z.score !== a.score) return z.score - a.score;
      return a.docId - z.docId;
    });
    return results;
  }
}
