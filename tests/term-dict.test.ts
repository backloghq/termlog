import { describe, it, expect } from "vitest";
import { TermDict } from "../src/term-dict.js";
import type { DictEntry } from "../src/term-dict.js";

function makeEntry(term: string, offset = 0, length = 10, df = 1): DictEntry {
  return { term, postingsOffset: offset, postingsLength: length, df };
}

describe("TermDict — lookup", () => {
  it("returns undefined for missing term in empty dict", () => {
    const dict = new TermDict();
    expect(dict.lookup("hello")).toBeUndefined();
  });

  it("finds a single entry", () => {
    const dict = new TermDict();
    dict.add(makeEntry("rust", 0, 20, 3));
    const found = dict.lookup("rust");
    expect(found).toBeDefined();
    expect(found!.df).toBe(3);
    expect(found!.postingsLength).toBe(20);
  });

  it("returns undefined for missing term in populated dict", () => {
    const dict = new TermDict();
    for (const term of ["alpha", "beta", "gamma", "delta"]) dict.add(makeEntry(term));
    expect(dict.lookup("zeta")).toBeUndefined();
    expect(dict.lookup("a")).toBeUndefined();
  });

  it("finds first, middle, and last entries", () => {
    const terms = ["apple", "banana", "cherry", "date", "elderberry"];
    const dict = new TermDict();
    terms.forEach((t, i) => dict.add(makeEntry(t, i * 100, 50, i + 1)));

    expect(dict.lookup("apple")?.df).toBe(1);
    expect(dict.lookup("cherry")?.df).toBe(3);
    expect(dict.lookup("elderberry")?.df).toBe(5);
  });

  it("binary search works on 1000-entry dict", () => {
    const dict = new TermDict();
    const terms = Array.from({ length: 1000 }, (_, i) => `term${String(i).padStart(5, "0")}`);
    terms.sort();
    terms.forEach((t, i) => dict.add(makeEntry(t, i * 8, 8, i + 1)));

    expect(dict.lookup("term00000")?.postingsOffset).toBe(0);
    expect(dict.lookup("term00500")?.df).toBe(501);
    expect(dict.lookup("term00999")?.postingsOffset).toBe(999 * 8);
    expect(dict.lookup("term01000")).toBeUndefined();
  });
});

describe("TermDict — size and iteration", () => {
  it("size returns entry count", () => {
    const dict = new TermDict();
    expect(dict.size).toBe(0);
    dict.add(makeEntry("a"));
    dict.add(makeEntry("b"));
    expect(dict.size).toBe(2);
  });

  it("iterates entries in insertion order", () => {
    const dict = new TermDict();
    const terms = ["cat", "dog", "eel", "fox"];
    terms.forEach((t) => dict.add(makeEntry(t)));
    const result = [...dict].map((e) => e.term);
    expect(result).toEqual(terms);
  });
});

describe("TermDict — serialize / deserialize round-trip", () => {
  it("empty dict round-trips", () => {
    const dict = new TermDict();
    const buf = dict.serialize();
    const restored = TermDict.deserialize(buf);
    expect(restored.size).toBe(0);
  });

  it("single entry round-trips", () => {
    const dict = new TermDict();
    dict.add({ term: "hello", postingsOffset: 42, postingsLength: 100, df: 7 });
    const restored = TermDict.deserialize(dict.serialize());
    expect(restored.size).toBe(1);
    const e = restored.lookup("hello");
    expect(e).toBeDefined();
    expect(e!.postingsOffset).toBe(42);
    expect(e!.postingsLength).toBe(100);
    expect(e!.df).toBe(7);
  });

  it("multiple entries round-trip with all fields preserved", () => {
    const entries: DictEntry[] = [
      { term: "alpha", postingsOffset: 0,   postingsLength: 10,  df: 1 },
      { term: "beta",  postingsOffset: 10,  postingsLength: 20,  df: 5 },
      { term: "gamma", postingsOffset: 30,  postingsLength: 100, df: 12 },
    ];
    const dict = new TermDict();
    entries.forEach((e) => dict.add(e));
    const restored = TermDict.deserialize(dict.serialize());

    for (const orig of entries) {
      const found = restored.lookup(orig.term);
      expect(found).toBeDefined();
      expect(found!.postingsOffset).toBe(orig.postingsOffset);
      expect(found!.postingsLength).toBe(orig.postingsLength);
      expect(found!.df).toBe(orig.df);
    }
  });

  it("Unicode terms round-trip", () => {
    const dict = new TermDict();
    dict.add({ term: "café", postingsOffset: 0, postingsLength: 4, df: 2 });
    dict.add({ term: "東京", postingsOffset: 4, postingsLength: 8, df: 1 });
    const restored = TermDict.deserialize(dict.serialize());
    expect(restored.lookup("café")?.df).toBe(2);
    expect(restored.lookup("東京")?.df).toBe(1);
  });

  it("100-entry round-trip preserves all lookups", () => {
    const dict = new TermDict();
    const terms = Array.from({ length: 100 }, (_, i) => `word${String(i).padStart(3, "0")}`);
    terms.sort();
    terms.forEach((t, i) => dict.add({ term: t, postingsOffset: i * 16, postingsLength: 16, df: i + 1 }));

    const restored = TermDict.deserialize(dict.serialize());
    expect(restored.size).toBe(100);
    for (let i = 0; i < 100; i++) {
      const found = restored.lookup(terms[i]);
      expect(found).toBeDefined();
      expect(found!.postingsOffset).toBe(i * 16);
      expect(found!.df).toBe(i + 1);
    }
  });
});

describe("TermDict.fromMap", () => {
  it("sorts entries by term and builds a searchable dict", () => {
    const map = new Map([
      ["zebra",  { postingsOffset: 100, postingsLength: 10, df: 1 }],
      ["apple",  { postingsOffset: 0,   postingsLength: 20, df: 5 }],
      ["mango",  { postingsOffset: 50,  postingsLength: 15, df: 3 }],
    ]);
    const dict = TermDict.fromMap(map);
    expect(dict.size).toBe(3);

    // All lookups work
    expect(dict.lookup("apple")?.df).toBe(5);
    expect(dict.lookup("mango")?.df).toBe(3);
    expect(dict.lookup("zebra")?.df).toBe(1);

    // Iteration is sorted
    const terms = [...dict].map((e) => e.term);
    expect(terms).toEqual(["apple", "mango", "zebra"]);
  });
});
