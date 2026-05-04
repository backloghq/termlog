/**
 * MinHeap unit tests.
 */

import { describe, it, expect } from "vitest";
import { MinHeap } from "../src/heap.js";

const numCmp = (a: number, b: number) => a - b;

describe("MinHeap", () => {
  it("pop from empty heap returns undefined", () => {
    const h = new MinHeap<number>(numCmp);
    expect(h.pop()).toBeUndefined();
    expect(h.peek()).toBeUndefined();
    expect(h.size).toBe(0);
  });

  it("single element: push + peek + pop", () => {
    const h = new MinHeap<number>(numCmp);
    h.push(42);
    expect(h.size).toBe(1);
    expect(h.peek()).toBe(42);
    expect(h.pop()).toBe(42);
    expect(h.size).toBe(0);
  });

  it("extracts elements in ascending order (min-heap property)", () => {
    const h = new MinHeap<number>(numCmp);
    for (const v of [5, 3, 8, 1, 9, 2]) h.push(v);
    const out: number[] = [];
    while (h.size > 0) out.push(h.pop()!);
    expect(out).toEqual([1, 2, 3, 5, 8, 9]);
  });

  it("handles duplicate values correctly", () => {
    const h = new MinHeap<number>(numCmp);
    h.push(3); h.push(1); h.push(3); h.push(1);
    expect(h.pop()).toBe(1);
    expect(h.pop()).toBe(1);
    expect(h.pop()).toBe(3);
    expect(h.pop()).toBe(3);
    expect(h.size).toBe(0);
  });

  it("works with object entries compared by a field", () => {
    const h = new MinHeap<{ term: string; idx: number }>(
      (a, b) => a.term < b.term ? -1 : a.term > b.term ? 1 : a.idx - b.idx,
    );
    h.push({ term: "zebra", idx: 2 });
    h.push({ term: "apple", idx: 0 });
    h.push({ term: "mango", idx: 1 });
    expect(h.pop()?.term).toBe("apple");
    expect(h.pop()?.term).toBe("mango");
    expect(h.pop()?.term).toBe("zebra");
  });

  it("interleaved push/pop maintains heap invariant", () => {
    const h = new MinHeap<number>(numCmp);
    h.push(10); h.push(5);
    expect(h.pop()).toBe(5);
    h.push(3); h.push(7);
    expect(h.pop()).toBe(3);
    expect(h.pop()).toBe(7);
    expect(h.pop()).toBe(10);
  });

  it("size tracks correctly through push and pop", () => {
    const h = new MinHeap<number>(numCmp);
    expect(h.size).toBe(0);
    h.push(1); expect(h.size).toBe(1);
    h.push(2); expect(h.size).toBe(2);
    h.pop();   expect(h.size).toBe(1);
    h.pop();   expect(h.size).toBe(0);
    h.pop();   expect(h.size).toBe(0);
  });
});
