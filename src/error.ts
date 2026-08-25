/**
 * All known error codes thrown by tavolato.
 *
 *   ERR_SCHEMA_* — problems found by `defineSchema`, adapters or `createWriter`
 *   ERR_ROW_*    — problems with a row handed to `append` / `appendAll`
 *   ERR_WRITER_* — problems with how the writer itself is being driven
 *   ERR_READ_*   — problems with the bytes handed to `readParquet` / `readSchema`
 *   ERR_STORE_*  — problems a `tavolato/uns3` store hits talking to object storage
 */
export type TavolatoErrorCode =
  // Schema definition
  | "ERR_SCHEMA_EMPTY" // no columns declared
  | "ERR_SCHEMA_COLUMN_INVALID" // malformed column definition or unsupported column type
  // Row validation
  | "ERR_ROW_NOT_AN_OBJECT" // the appended row is not a plain object
  | "ERR_ROW_UNKNOWN_COLUMN" // row carries a key that is not part of the schema
  | "ERR_ROW_VALUE_MISSING" // required column is absent, `null` or `undefined`
  | "ERR_ROW_VALUE_INVALID" // value does not fit the declared column type
  // Writer lifecycle / options
  | "ERR_WRITER_FINISHED" // `append` / `finish` called after `finish`
  | "ERR_WRITER_OPTION_INVALID" // bad `createWriter` option
  | "ERR_WRITER_BUSY" // used again before the promise a previous call returned settled
  | "ERR_WRITER_CODEC_FAILED" // the compression hook threw, rejected, or returned something unusable
  // File reading
  | "ERR_READ_OPTION_INVALID" // bad `readParquet` option
  | "ERR_READ_MALFORMED" // the bytes are not a well-formed Parquet file
  | "ERR_READ_UNSUPPORTED" // well-formed Parquet, but outside the subset tavolato writes
  // Object storage, through `tavolato/uns3`
  | "ERR_STORE_INPUT_INVALID" // `store.put` was handed something it cannot upload
  | "ERR_STORE_OBJECT_CHANGED" // the object was replaced between two of a read's requests
  | "ERR_STORE_RANGE_UNSATISFIED" // a ranged read came back with bytes other than the ones asked for
  | (string & {}); // forward-compatible escape hatch

/**
 * Base error class for all tavolato errors. Every error thrown by the library
 * is an instance of `TavolatoError`, so a single `instanceof` check is enough
 * to tell library errors apart from other thrown values. Use the `code`
 * property (or the `isTavolatoError` type guard) to narrow further.
 *
 * @example
 * import { isTavolatoError } from "tavolato";
 *
 * try {
 *   writer.append({ n: "not a number" });
 * } catch (error) {
 *   if (isTavolatoError(error, "ERR_ROW_VALUE_INVALID")) {
 *     console.log(error.column); // "n"
 *   }
 * }
 */
export class TavolatoError<TCode extends TavolatoErrorCode = TavolatoErrorCode> extends Error {
  readonly code: TCode;
  /** Name of the column the error relates to, when the error is column-scoped. */
  readonly column?: string;
  override readonly cause?: unknown;

  constructor(message: string, code: TCode, column?: string, cause?: unknown) {
    super(message);
    this.name = "TavolatoError";
    this.code = code;
    this.column = column;
    this.cause = cause;
  }
}

/**
 * Renders an offending value for an error message, without ever stringifying
 * an object: a message about a bad value must not run somebody's `toString`.
 *
 * @internal
 */
export function describe(value: unknown): string {
  if (value === null) return "null";
  switch (typeof value) {
    case "undefined": {
      return "undefined";
    }
    case "bigint": {
      return `${value}n`;
    }
    case "string": {
      return JSON.stringify(value);
    }
    case "number":
    case "boolean": {
      return String(value);
    }
    case "symbol": {
      return value.toString();
    }
    default: {
      // object and function
      return Object.prototype.toString.call(value);
    }
  }
}

/** Validates a required public options bag at its API boundary. @internal */
export function assertOptionsObject(
  value: unknown,
  label: string,
  code: TavolatoErrorCode,
): asserts value is object {
  if (typeof value !== "object" || value === null) {
    throw new TavolatoError(`${label} must be an object, received ${describe(value)}`, code);
  }
}

/** Validates an optional public options bag while preserving omission. @internal */
export function assertOptionalOptionsObject(
  value: unknown,
  label: string,
  code: TavolatoErrorCode,
): asserts value is object | undefined {
  if (value !== undefined) assertOptionsObject(value, label, code);
}

/**
 * The bytes are not a well-formed Parquet file: wrong magic, a truncated
 * stream, a length that does not fit, a structure that contradicts itself.
 *
 * @internal
 */
export function malformed(message: string, column?: string, cause?: unknown): TavolatoError {
  return new TavolatoError(message, "ERR_READ_MALFORMED", column, cause);
}

/**
 * The file is valid Parquet, but uses something tavolato never writes and
 * therefore refuses to read. `found` names the offending feature; the rest of
 * the sentence — the scope promise — is worded here, once.
 *
 * `remedy` is appended where the refusal is one the caller can lift: a codec
 * tavolato knows by name but cannot implement, or an annotation whose
 * JavaScript type is a decision only the caller can make.
 *
 * @internal
 */
export function unsupported(found: string, column?: string, remedy?: string): TavolatoError {
  return new TavolatoError(
    `Cannot read ${found}: tavolato only reads the files it writes — flat schemas of string, json, f64, f32, i64, i32, bool and timestamp columns, PLAIN encoded, UNCOMPRESSED, in v1 data pages${
      remedy === undefined ? "" : ` — ${remedy}`
    }`,
    "ERR_READ_UNSUPPORTED",
    column,
  );
}

/** The file feature and remedy carried out of an in-box adapter's `read`. @internal */
export interface AdapterUnsupportedDetails {
  readonly found: string;
  readonly remedy: string;
}

/*
 * Only errors made here enter this map. A caller-authored adapter throwing an
 * `ERR_READ_UNSUPPORTED` remains a failed callback and is wrapped as malformed;
 * in-box adapters can instead hand a valid but unrepresentable value back to
 * the reader, which adds the column context before exposing the refusal.
 */
const ADAPTER_UNSUPPORTED = new WeakMap<TavolatoError, AdapterUnsupportedDetails>();

/** Creates a refusal an in-box adapter can hand back to the reader. @internal */
export function adapterUnsupported(found: string, remedy: string): TavolatoError {
  const error = unsupported(found, undefined, remedy);
  ADAPTER_UNSUPPORTED.set(error, { found, remedy });
  return error;
}

/** Recovers an in-box adapter refusal without trusting caller-thrown errors. @internal */
export function adapterUnsupportedDetails(error: unknown): AdapterUnsupportedDetails | undefined {
  return error instanceof TavolatoError ? ADAPTER_UNSUPPORTED.get(error) : undefined;
}

/** The remedy for a column whose meaning is a decision only the caller can make. */
export const TYPES_REMEDY: string = "pass a matching type in ReadOptions.types to read it anyway";

/**
 * Something handed to `readParquet` in its options cannot be used as given —
 * as opposed to the *file* being unreadable, which is what the other two say.
 *
 * @internal
 */
export function badOption(message: string, column?: string, cause?: unknown): TavolatoError {
  return new TavolatoError(message, "ERR_READ_OPTION_INVALID", column, cause);
}

/**
 * Type guard that narrows `error` to `TavolatoError`, optionally to a single
 * error code.
 *
 * @example
 * if (isTavolatoError(error, "ERR_WRITER_FINISHED")) {
 *   // error.code is "ERR_WRITER_FINISHED"
 * }
 */
export function isTavolatoError<T extends TavolatoErrorCode>(
  error: unknown,
  code: T,
): error is TavolatoError<T>;
export function isTavolatoError(error: unknown): error is TavolatoError;
export function isTavolatoError(error: unknown, code?: string): boolean {
  if (!(error instanceof TavolatoError)) return false;
  return code === undefined || error.code === code;
}
