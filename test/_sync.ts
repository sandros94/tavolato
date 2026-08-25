import { expect } from "vitest";

/**
 * Asserts that a maybe-promise stayed synchronous, and hands back the value.
 *
 * Every call site is also a test of the fast path: without a codec, or with a
 * synchronous one, `append`, `finish` and `readParquet` must return the value
 * itself and not a promise wrapping it. Wrapping the whole suite in this is how
 * that guarantee is checked everywhere rather than in one dedicated case.
 */
export function sync<T>(value: T | Promise<T>): T {
  expect(value).not.toBeInstanceOf(Promise);
  return value as T;
}
