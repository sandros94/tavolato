/**
 * All known error codes thrown by tavolato.
 *
 *   ERR_SCHEMA_* — problems with the schema handed to `defineSchema`
 *   ERR_ROW_*    — problems with a row handed to `append` / `appendAll`
 *   ERR_WRITER_* — problems with how the writer itself is being driven
 *   ERR_READ_*   — problems with the bytes handed to `readParquet` / `readSchema`
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
  // File reading
  | "ERR_READ_MALFORMED" // the bytes are not a well-formed Parquet file
  | "ERR_READ_UNSUPPORTED" // well-formed Parquet, but outside the subset tavolato writes
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
 * @internal
 */
export function unsupported(found: string, column?: string): TavolatoError {
  return new TavolatoError(
    `Cannot read ${found}: tavolato only reads the files it writes — flat schemas of string, f64, i64, bool and timestamp columns, PLAIN encoded, UNCOMPRESSED, in v1 data pages`,
    "ERR_READ_UNSUPPORTED",
    column,
  );
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
