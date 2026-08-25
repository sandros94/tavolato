/*
 * ---------------------------------------------------------------------------
 * Maybe-promise plumbing.
 *
 * A compression hook is free to be synchronous — most runtimes ship a sync
 * inflate — or asynchronous, and tavolato refuses to pick for you. Everything a
 * hook touches is therefore threaded through `chain` rather than `await`ed, so
 * a file written or read without a codec, or with a synchronous one, never
 * leaves the stack: no microtask, no promise in the result.
 * ---------------------------------------------------------------------------
 */

/** A value that may already be there, or may still be on its way. */
export type MaybePromise<T> = T | Promise<T>;

/**
 * Whether `value` is a thenable.
 *
 * Duck-typed rather than `instanceof Promise` so that a promise from another
 * realm — a worker, a `vm` context — and a hand-rolled thenable both count.
 */
export function isThenable<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return typeof (value as PromiseLike<T> | undefined)?.then === "function";
}

/**
 * Continues with `fn` once `value` has settled, staying synchronous when it
 * already has.
 *
 * `Awaited<R>` mirrors what `then` does at runtime: an `fn` that itself returns
 * a maybe-promise collapses into one level, exactly as chaining it would.
 */
export function chain<T, R>(
  value: T | PromiseLike<T>,
  fn: (value: T) => R,
): MaybePromise<Awaited<R>> {
  // The two casts stand for that flattening, which the type system cannot see
  // through: whichever branch runs, the result settles to `Awaited<R>`.
  return isThenable(value)
    ? (value.then(fn) as Promise<Awaited<R>>)
    : (fn(value as T) as MaybePromise<Awaited<R>>);
}

/**
 * Runs `step` for `0 … count - 1` in order, waiting for each before starting
 * the next.
 *
 * The range is walked in a plain loop for as long as the steps stay
 * synchronous; the first one that defers hands the rest of the range to its
 * continuation. A synchronous traversal therefore costs no promises, and an
 * asynchronous one costs no stack.
 */
export function chainEach(count: number, step: (index: number) => unknown): MaybePromise<void> {
  const run = (from: number): MaybePromise<void> => {
    for (let index = from; index < count; index++) {
      const result = step(index);
      if (isThenable(result)) return chain(result, () => run(index + 1));
    }
  };
  return run(0);
}
