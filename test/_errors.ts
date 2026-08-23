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

/**
 * Runs `run` and reports what it threw as a bare code — `"no error"` when it
 * threw nothing at all.
 *
 * For probes that poke a method many times and assert on the *set* of outcomes,
 * where "it went through" is itself a failure worth naming.
 */
export function errorCode(run: () => unknown): string {
  try {
    run();
    return "no error";
  } catch (error) {
    if (!(error instanceof TavolatoError)) throw error;
    return error.code;
  }
}

/**
 * The deferred twin of {@link expectError}: awaits `pending`, asserts it
 * rejected with a `TavolatoError` carrying `code`, and returns it.
 */
export async function expectRejection(code: string, pending: unknown): Promise<TavolatoError> {
  let caught: unknown;
  try {
    await pending;
  } catch (error) {
    caught = error;
  }
  expect(caught).toBeInstanceOf(TavolatoError);
  expect(isTavolatoError(caught, code)).toBe(true);
  return caught as TavolatoError;
}
