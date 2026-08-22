import { expect } from "vitest";
import { isTavolatoError, TavolatoError } from "../src/index.ts";

/** Runs `run`, asserts it threw a `TavolatoError` with `code`, and returns it. */
export function expectError(code: string, run: () => unknown): TavolatoError {
  let caught: unknown;
  try {
    run();
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(TavolatoError);
  expect(isTavolatoError(caught, code)).toBe(true);
  return caught as TavolatoError;
}
