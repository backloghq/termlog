/**
 * Tokenizer abstraction — pluggable text analysis.
 * Default: UnicodeTokenizer matching agentdb's regex (lowercase, min 1 char).
 */

export interface Tokenizer {
  tokenize(text: string): string[];
}

/**
 * Default Unicode tokenizer — extracts sequences of Unicode word characters,
 * lowercases them, and filters by minimum length. Matches agentdb's TextIndex
 * tokenization so BM25 scores are comparable across both implementations.
 */
export class UnicodeTokenizer implements Tokenizer {
  private readonly minLen: number;

  constructor(minLen = 1) {
    this.minLen = minLen;
  }

  tokenize(text: string): string[] {
    const tokens = text.toLowerCase().match(/[\p{L}\p{M}\p{N}]+/gu) ?? [];
    return tokens.filter((t) => t.length >= this.minLen);
  }
}

export const DEFAULT_TOKENIZER = new UnicodeTokenizer(1);
