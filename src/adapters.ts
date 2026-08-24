import { decodeUtf8, utf8 } from "./internal/bytes.ts";
import { describe, malformed, TavolatoError } from "./error.ts";
import type { Annotation, JsonValue, LogicalAdapter, PhysicalKind, TimeUnitName } from "./types.ts";

/*
 * ---------------------------------------------------------------------------
 * Logical column types
 *
 * A Parquet column carries its layout (the physical type) and its meaning (the
 * annotation) separately, and the second one does not determine a JavaScript
 * type. `DECIMAL(38, 4)` is sixteen bytes of two's complement: a `string`, a
 * `bigint` and somebody's arbitrary-precision object are all defensible
 * readings of it, and picking one for you would be exactly the overreach
 * tavolato refuses everywhere else.
 *
 * So it refuses here too — and an adapter is how you answer. It is not an
 * escape from the scope promise, it is the same principle as the typed
 * refusals: tavolato names what it found and hands the decision back.
 *
 * The ones below are the mappings the format itself makes obvious, each chosen
 * so that a value survives the round trip unchanged: a `number` where it is
 * lossless, a `bigint` for the 64-bit widths, a `string` where a JavaScript
 * number would lie, and a `Date` only where the mapping is exact.
 * ---------------------------------------------------------------------------
 */

const PHYSICAL_KINDS: ReadonlySet<string> = new Set<PhysicalKind>([
  "bool",
  "i32",
  "i64",
  "f32",
  "f64",
  "bytes",
  "fixed",
]);

/** Every annotation an adapter may stamp: the whole model minus `unknown`. */
const WRITABLE_ANNOTATIONS: ReadonlySet<string> = new Set<Annotation["kind"]>([
  "none",
  "string",
  "json",
  "bson",
  "enum",
  "uuid",
  "date",
  "float16",
  "decimal",
  "time",
  "timestamp",
  "integer",
]);

const TIME_UNITS: ReadonlySet<string> = new Set<TimeUnitName>(["millis", "micros", "nanos"]);

/** Digits a Parquet `DECIMAL` can carry, which is what the annotation may declare. */
const MAX_DECIMAL_PRECISION = 38;

function invalid(message: string): TavolatoError {
  return new TavolatoError(message, "ERR_SCHEMA_COLUMN_INVALID");
}

/** A value the adapter was handed, or handed back, that it cannot work with. */
function reject(message: string): never {
  throw new TavolatoError(message, "ERR_ROW_VALUE_INVALID");
}

/**
 * What is wrong with `annotation`, or `undefined` if nothing is.
 *
 * `unknown` is the one member an adapter may not produce: it stands for an
 * annotation *this version has no name for*, and there is no way to write one
 * back out.
 */
function annotationProblem(annotation: unknown): string | undefined {
  if (typeof annotation !== "object" || annotation === null) {
    return `annotate() must return an annotation, received ${describe(annotation)}`;
  }
  const value = annotation as Annotation;
  if (!WRITABLE_ANNOTATIONS.has(value.kind)) {
    return `annotate() returned the annotation kind ${describe(value.kind)}, which cannot be written`;
  }
  if (value.kind === "decimal") {
    // The same bounds `decimal()` holds itself to, and for the same reason:
    // Parquet defines `DECIMAL` only within them, so an annotation outside is
    // one no reader can act on. Refusing it here is the schema gate keeping its
    // promise — a schema that cannot produce a file says so before a row is
    // appended, rather than in somebody else's query engine.
    const { precision, scale } = value;
    return Number.isSafeInteger(precision) &&
      precision >= 1 &&
      precision <= MAX_DECIMAL_PRECISION &&
      Number.isSafeInteger(scale) &&
      scale >= 0 &&
      scale <= precision
      ? undefined
      : `annotate() returned a decimal annotation of precision ${describe(precision)} and scale ${describe(scale)}; precision must be an integer from 1 to ${MAX_DECIMAL_PRECISION} and scale an integer from 0 to the precision`;
  }
  if (value.kind === "time" || value.kind === "timestamp") {
    return TIME_UNITS.has(value.unit) && typeof value.isAdjustedToUTC === "boolean"
      ? undefined
      : `annotate() returned a ${value.kind} annotation without a unit and a UTC flag`;
  }
  if (value.kind === "integer") {
    return [8, 16, 32, 64].includes(value.bitWidth) && typeof value.isSigned === "boolean"
      ? undefined
      : "annotate() returned an integer annotation without a bit width of 8, 16, 32 or 64";
  }
  return undefined;
}

/**
 * What is wrong with `spec` as a column type, or `undefined` if nothing is.
 *
 * Shared by everything that accepts an adapter — `defineColumnType`,
 * `defineSchema` and `ReadOptions.types` — so that an object which never went
 * through `defineColumnType` is still held to the same shape rather than
 * failing later as a `TypeError` from somewhere inside a page.
 *
 * @internal
 */
export function adapterProblem(spec: unknown): string | undefined {
  if (typeof spec !== "object" || spec === null) {
    return `expects a column type such as decimal({ precision: 10, scale: 2 }), received ${describe(spec)}`;
  }
  const adapter = spec as LogicalAdapter<unknown, unknown>;
  if (typeof adapter.name !== "string" || adapter.name === "") {
    return `has no name; a column type needs one so that errors can say which it is`;
  }
  if (!PHYSICAL_KINDS.has(adapter.physical)) {
    return `${adapter.name} declares the physical type ${describe(adapter.physical)}, which is not one of ${[...PHYSICAL_KINDS].join(", ")}`;
  }
  if (adapter.physical === "fixed") {
    const width = adapter.typeLength;
    if (typeof width !== "number" || !Number.isSafeInteger(width) || width < 1) {
      return `${adapter.name} is stored as a FIXED_LEN_BYTE_ARRAY and must declare a positive integer typeLength, received ${describe(width)}`;
    }
  } else if (adapter.typeLength !== undefined) {
    return `${adapter.name} declares a typeLength but is not stored as a FIXED_LEN_BYTE_ARRAY`;
  }
  for (const method of ["matches", "annotate", "read", "write"] as const) {
    if (typeof adapter[method] !== "function") {
      return `${adapter.name} has no ${method}() function`;
    }
  }
  let annotation: unknown;
  try {
    annotation = adapter.annotate();
  } catch (cause) {
    return `${adapter.name} threw from annotate(): ${cause instanceof Error ? cause.message : describe(cause)}`;
  }
  const problem = annotationProblem(annotation);
  return problem === undefined ? undefined : `${adapter.name} ${problem}`;
}

/**
 * Declares a logical column type: the pair of pure functions that turn one
 * physical value into the JavaScript value you want, and back.
 *
 * The spec is validated here — a physical kind that exists, a `typeLength`
 * exactly where `"fixed"` needs one, four callable halves, an annotation that
 * can actually be written — and frozen, so a column type is a value you can
 * hand around rather than a mutable configuration object.
 *
 * The result goes in two places: a schema (`{ type: price }`), where it decides
 * how a column is written, and `ReadOptions.types`, where it claims columns it
 * recognises on the way back in. The same object is fine for both.
 *
 * A `bytes` or `fixed` `write()` must return a **fresh** `Uint8Array` every
 * time. The writer holds what it is handed by reference until the row group is
 * flushed, so a reused scratch buffer would rewrite the rows already buffered
 * with the newest row's bytes.
 *
 * @example
 * const centi = defineColumnType({
 *   name: "centi",
 *   physical: "i64",
 *   matches: (annotation) =>
 *     annotation.kind === "decimal" && annotation.precision === 18 && annotation.scale === 2,
 *   annotate: () => ({ kind: "decimal", precision: 18, scale: 2 }),
 *   read: (raw) => Number(raw as bigint) / 100,
 *   write: (value: number) => BigInt(Math.round(value * 100)),
 * });
 *
 * @throws {TavolatoError} `ERR_SCHEMA_COLUMN_INVALID` when the spec is not one.
 */
export function defineColumnType<TIn, TOut>(
  spec: LogicalAdapter<TIn, TOut>,
): LogicalAdapter<TIn, TOut> {
  const problem = adapterProblem(spec);
  if (problem !== undefined) throw invalid(`defineColumnType ${problem}`);
  return Object.freeze(spec);
}

/*
 * ---------------------------------------------------------------------------
 * The in-box column types
 * ---------------------------------------------------------------------------
 */

const MILLIS_PER_DAY = 86_400_000;

/**
 * `DATE` ⇄ `Date`, stored as days since the Unix epoch in an `INT32`.
 *
 * A Parquet `DATE` has no time of day at all, so writing one requires a `Date`
 * that is exactly UTC midnight. Truncating a timestamp for you would be the
 * library quietly discarding hours it was handed; a typed error hands that
 * decision back, and `new Date(Date.UTC(y, m, d))` is how you make one.
 */
export function date(): LogicalAdapter<Date, Date> {
  return defineColumnType<Date, Date>({
    name: "date",
    physical: "i32",
    matches: (annotation) => annotation.kind === "date",
    annotate: () => ({ kind: "date" }),
    read: (raw) => new Date((raw as number) * MILLIS_PER_DAY),
    write: (value) => {
      if (!(value instanceof Date)) reject(`date expects a Date, received ${describe(value)}`);
      const millis = value.getTime();
      if (Number.isNaN(millis)) reject("date expects a valid Date");
      if (millis % MILLIS_PER_DAY !== 0) {
        reject(
          `date expects a Date at exactly UTC midnight, received ${value.toISOString()}; a Parquet DATE has no time of day`,
        );
      }
      return millis / MILLIS_PER_DAY;
    },
  });
}

/** Options for {@link decimal}. */
export interface DecimalOptions {
  /** Total number of significant digits, 1 to 38. */
  precision: number;
  /** Digits after the point, 0 to `precision`. Defaults to `0`. */
  scale?: number;
}

/**
 * `DECIMAL` ⇄ `string`, in the canonical form with exactly `scale` digits
 * after the point: `"12.3400"` for `{ precision: 9, scale: 4 }`.
 *
 * A string because that is the only JavaScript type that can hold the value:
 * a `number` starts lying at 2^53, and a `bigint` would drop the point. The
 * canonical form is *exact* — what you write is what you read — which is why
 * it is also strict on the way in: `"12.34"` in a `scale: 4` column, a leading
 * zero, or a `-0.00` are all refused rather than reinterpreted.
 *
 * The physical type follows precision, exactly as DuckDB's writer chooses it:
 * `INT32` up to 9 digits, `INT64` up to 18, and a 16-byte two's complement
 * `FIXED_LEN_BYTE_ARRAY` up to 38.
 */
export function decimal(options: DecimalOptions): LogicalAdapter<string, string> {
  const { precision, scale = 0 } = options;
  if (!Number.isSafeInteger(precision) || precision < 1 || precision > MAX_DECIMAL_PRECISION) {
    throw invalid(
      `decimal precision must be an integer from 1 to ${MAX_DECIMAL_PRECISION}, received ${describe(precision)}`,
    );
  }
  if (!Number.isSafeInteger(scale) || scale < 0 || scale > precision) {
    throw invalid(
      `decimal scale must be an integer from 0 to the precision, received ${describe(scale)}`,
    );
  }

  const physical: PhysicalKind = precision <= 9 ? "i32" : precision <= 18 ? "i64" : "fixed";
  const limit = 10n ** BigInt(precision);
  // No redundant leading zero, and exactly `scale` digits after the point:
  // one spelling per value, so every accepted string reads back as itself.
  const pattern = new RegExp(`^-?(0|[1-9]\\d*)${scale === 0 ? "" : `\\.\\d{${scale}}`}$`);

  return defineColumnType<string, string>({
    name: `decimal(${precision}, ${scale})`,
    physical,
    ...(physical === "fixed" ? { typeLength: 16 } : {}),
    matches: (annotation) =>
      annotation.kind === "decimal" &&
      annotation.precision === precision &&
      annotation.scale === scale,
    annotate: () => ({ kind: "decimal", precision, scale }),
    read: (raw) => {
      const unscaled =
        physical === "fixed"
          ? fromTwosComplement(raw as Uint8Array)
          : physical === "i64"
            ? (raw as bigint)
            : BigInt(raw as number);
      return toDecimalString(unscaled, scale);
    },
    write: (value) => {
      if (typeof value !== "string") {
        reject(`decimal expects a string, received ${describe(value)}`);
      }
      if (!pattern.test(value)) {
        reject(
          `decimal(${precision}, ${scale}) expects a canonical decimal string with exactly ${scale} digit${
            scale === 1 ? "" : "s"
          } after the point, received ${describe(value)}`,
        );
      }
      const unscaled = BigInt(value.replace(".", ""));
      if (unscaled === 0n && value.startsWith("-")) {
        reject(`decimal has no negative zero, received ${describe(value)}`);
      }
      if (unscaled <= -limit || unscaled >= limit) {
        reject(`decimal(${precision}, ${scale}) cannot hold ${describe(value)}`);
      }
      return physical === "fixed"
        ? toTwosComplement(unscaled, 16)
        : physical === "i64"
          ? unscaled
          : Number(unscaled);
    },
  });
}

/** Renders an unscaled integer as the canonical decimal string for `scale`. */
function toDecimalString(unscaled: bigint, scale: number): string {
  const negative = unscaled < 0n;
  const digits = (negative ? -unscaled : unscaled).toString().padStart(scale + 1, "0");
  const point = digits.length - scale;
  return `${negative ? "-" : ""}${digits.slice(0, point)}${
    scale === 0 ? "" : `.${digits.slice(point)}`
  }`;
}

/** Big-endian two's complement, which is how Parquet stores a fixed-width decimal. */
function toTwosComplement(value: bigint, length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  let rest = BigInt.asUintN(length * 8, value);
  for (let index = length - 1; index >= 0; index--) {
    bytes[index] = Number(rest & 0xffn);
    rest >>= 8n;
  }
  return bytes;
}

function fromTwosComplement(bytes: Uint8Array): bigint {
  let result = 0n;
  for (const byte of bytes) result = (result << 8n) | BigInt(byte);
  return BigInt.asIntN(bytes.length * 8, result);
}

const UUID_PATTERN = /^[\da-f]{8}-[\da-f]{4}-[\da-f]{4}-[\da-f]{4}-[\da-f]{12}$/;

/**
 * `UUID` ⇄ `string`, in the canonical lowercase 8-4-4-4-12 form, stored as a
 * 16-byte `FIXED_LEN_BYTE_ARRAY`.
 *
 * Only that form is accepted: the hyphens and the case are not part of the
 * value, so allowing a second spelling would mean handing back a string that
 * is not the one you wrote. `crypto.randomUUID()` already produces it.
 */
export function uuid(): LogicalAdapter<string, string> {
  return defineColumnType<string, string>({
    name: "uuid",
    physical: "fixed",
    typeLength: 16,
    matches: (annotation) => annotation.kind === "uuid",
    annotate: () => ({ kind: "uuid" }),
    read: (raw) => {
      let hex = "";
      for (const byte of raw as Uint8Array) hex += byte.toString(16).padStart(2, "0");
      return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    },
    write: (value) => {
      if (typeof value !== "string" || !UUID_PATTERN.test(value)) {
        reject(
          `uuid expects a canonical lowercase UUID such as "b3f2c1a0-1111-4222-8333-444455556666", received ${describe(value)}`,
        );
      }
      const hex = value.replaceAll("-", "");
      const bytes = new Uint8Array(16);
      for (let index = 0; index < 16; index++) {
        bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
      }
      return bytes;
    },
  });
}

/** Options for {@link time} and {@link timestamp}. */
export interface TimeOptions<TUnit extends TimeUnitName = TimeUnitName> {
  /** Resolution of the stored count. */
  unit: TUnit;
}

/**
 * Which JavaScript type a resolution is carried in: a `number` where the count
 * fits an `INT32` and a double holds it exactly, a `bigint` where it does not.
 */
export type TimeValue<TUnit extends TimeUnitName> = TUnit extends "millis" ? number : bigint;

/** The number of units in a day, per resolution; a `TIME` is a count since midnight. */
const UNITS_PER_DAY: Readonly<Record<TimeUnitName, bigint>> = {
  millis: 86_400_000n,
  micros: 86_400_000_000n,
  nanos: 86_400_000_000_000n,
};

function assertUnit(unit: TimeUnitName, what: string): void {
  if (!TIME_UNITS.has(unit)) {
    throw invalid(`${what} unit must be millis, micros or nanos, received ${describe(unit)}`);
  }
}

/**
 * `TIME` ⇄ a count since midnight: a `number` of milliseconds in an `INT32`,
 * or a `bigint` of microseconds or nanoseconds in an `INT64`.
 *
 * A bare count rather than a `Date`, because a time of day is not an instant:
 * there is no date to put it on, and every `Date` this could produce would
 * carry one that was invented here.
 *
 * The domain is `[0, one day)` — **strictly less than one day**, as Arrow and
 * parquet-mr read it. A full day's worth of units is not a time of day but the
 * next midnight; DuckDB renders it as `24:00:00`, which is nobody's clock.
 *
 * The annotation is written as a wall-clock time (`isAdjustedToUTC=false`,
 * which is what DuckDB writes for `TIME`), and read whichever way the flag
 * points: the count is the same number either way, exactly as it is for the
 * built-in `timestamp` type.
 */
export function time<TUnit extends TimeUnitName>(
  options: TimeOptions<TUnit>,
): LogicalAdapter<TimeValue<TUnit>, TimeValue<TUnit>> {
  const { unit } = options;
  assertUnit(unit, "time");
  const perDay = UNITS_PER_DAY[unit];
  const annotate = (): Annotation => ({ kind: "time", unit, isAdjustedToUTC: false });
  const matches = (annotation: Annotation): boolean =>
    annotation.kind === "time" && annotation.unit === unit;

  // One cast, at the one place the unit stops being a type and becomes a value:
  // which of the two shapes is built is exactly what `TimeValue` computes.
  const adapter =
    unit === "millis"
      ? defineColumnType<number, number>({
          name: "time(millis)",
          physical: "i32",
          matches,
          annotate,
          read: (raw) => raw as number,
          write: (value) => {
            if (!Number.isSafeInteger(value) || value < 0 || value >= Number(perDay)) {
              reject(
                `time(millis) expects an integer number of milliseconds since midnight, below ${perDay}, received ${describe(value)}`,
              );
            }
            return value;
          },
        })
      : defineColumnType<bigint, bigint>({
          name: `time(${unit})`,
          physical: "i64",
          matches,
          annotate,
          read: (raw) => raw as bigint,
          write: (value) => {
            if (typeof value !== "bigint" || value < 0n || value >= perDay) {
              reject(
                `time(${unit}) expects a bigint count of ${unit} since midnight, below ${perDay}, received ${describe(value)}`,
              );
            }
            return value;
          },
        });
  return adapter as LogicalAdapter<TimeValue<TUnit>, TimeValue<TUnit>>;
}

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

/**
 * `TIMESTAMP` ⇄ `bigint`, a count since the Unix epoch in an `INT64`.
 *
 * The built-in `timestamp` column type is milliseconds as a `Date`, which is
 * exactly what a `Date` holds. Microseconds and nanoseconds are not: a `Date`
 * would round them, so this hands back the count itself and loses nothing.
 * Milliseconds are offered here too, for the same reason in reverse — a
 * `bigint` when you want the raw count rather than a `Date`.
 *
 * Written as an instant (`isAdjustedToUTC=true`), and read whichever way the
 * flag points, since the count does not move: that is what lets it read both
 * DuckDB's `TIMESTAMP` and its `TIMESTAMPTZ`, which differ only in the flag.
 */
export function timestamp(options: TimeOptions): LogicalAdapter<bigint, bigint> {
  const { unit } = options;
  assertUnit(unit, "timestamp");
  return defineColumnType<bigint, bigint>({
    name: `timestamp(${unit})`,
    physical: "i64",
    matches: (annotation) => annotation.kind === "timestamp" && annotation.unit === unit,
    annotate: () => ({ kind: "timestamp", unit, isAdjustedToUTC: true }),
    read: (raw) => raw as bigint,
    write: (value) => {
      if (typeof value !== "bigint" || value < INT64_MIN || value > INT64_MAX) {
        reject(
          `timestamp(${unit}) expects a bigint within the signed 64-bit range, received ${describe(value)}`,
        );
      }
      return value;
    },
  });
}

/**
 * `FLOAT16` ⇄ `number`, IEEE 754 half precision in a 2-byte
 * `FIXED_LEN_BYTE_ARRAY`.
 *
 * Writing rounds to half precision once — the one place a value changes on the
 * way in, and it has to, because half precision is what the column *is*.
 * Everything after that is exact: what is read back is the stored value, and
 * writing that value again produces the same two bytes.
 */
export function float16(): LogicalAdapter<number, number> {
  return defineColumnType<number, number>({
    name: "float16",
    physical: "fixed",
    typeLength: 2,
    matches: (annotation) => annotation.kind === "float16",
    annotate: () => ({ kind: "float16" }),
    read: (raw) => {
      const bytes = raw as Uint8Array;
      return numberFromHalf(bytes[0] | (bytes[1] << 8));
    },
    write: (value) => {
      if (typeof value !== "number") {
        reject(`float16 expects a number, received ${describe(value)}`);
      }
      const bits = halfFromNumber(value);
      return new Uint8Array([bits & 0xff, bits >>> 8]);
    },
  });
}

/** Scratch view used to read a double's exponent field without a `Math.log2` round trip. */
const scratch = /* @__PURE__ */ new DataView(new ArrayBuffer(8));

/** The unbiased exponent of a finite, non-zero magnitude. */
function exponentOf(magnitude: number): number {
  scratch.setFloat64(0, magnitude);
  return ((scratch.getUint32(0) >>> 20) & 0x7ff) - 1023;
}

/** Rounds to the nearest integer, ties to even — the rounding IEEE 754 prescribes. */
function roundTiesToEven(value: number): number {
  const whole = Math.floor(value);
  const rest = value - whole;
  return rest > 0.5 || (rest === 0.5 && whole % 2 === 1) ? whole + 1 : whole;
}

/**
 * Encodes a double as the sixteen bits of an IEEE 754 half.
 *
 * Rounded from the double directly rather than through a single, which would
 * round twice and, for the handful of values that sit on a boundary both
 * times, land one step away from the correct half.
 */
function halfFromNumber(value: number): number {
  if (Number.isNaN(value)) return 0x7e00; // one quiet NaN; JavaScript has no others to preserve
  const sign = value < 0 || Object.is(value, -0) ? 0x8000 : 0;
  const magnitude = Math.abs(value);
  if (magnitude === Number.POSITIVE_INFINITY) return sign | 0x7c00;
  if (magnitude === 0) return sign;

  // Subnormal halves all share the smallest normal exponent's quantum, so one
  // formula covers both: significand = magnitude / 2^(exponent - 10).
  let exponent = Math.max(exponentOf(magnitude), -14);
  let significand = roundTiesToEven(magnitude / 2 ** (exponent - 10));
  if (significand === 2048) {
    // Rounding carried into the next binade.
    significand = 1024;
    exponent += 1;
  }
  if (exponent > 15) return sign | 0x7c00; // past the largest half: infinity
  return sign | (significand < 1024 ? significand : ((exponent + 15) << 10) | (significand - 1024));
}

/** The inverse of {@link halfFromNumber}; every half is exactly a double. */
function numberFromHalf(bits: number): number {
  const sign = (bits & 0x8000) === 0 ? 1 : -1;
  const exponent = (bits >>> 10) & 0x1f;
  const significand = bits & 0x3ff;
  if (exponent === 0) return sign * significand * 2 ** -24; // zero and the subnormals
  if (exponent === 31) return significand === 0 ? sign * Number.POSITIVE_INFINITY : Number.NaN;
  return sign * (significand + 1024) * 2 ** (exponent - 25);
}

/** The integer widths Parquet's `IntType` names. */
export type IntegerWidth = 8 | 16 | 32 | 64;

/** Options for {@link integer}. */
export interface IntegerOptions<TWidth extends IntegerWidth = IntegerWidth> {
  /** Width of the stored integer. */
  bitWidth: TWidth;
  /** Whether the values are signed. Defaults to `true`. */
  signed?: boolean;
}

/** A 64-bit width is a `bigint`; every narrower one is a `number`. */
export type IntegerValue<TWidth extends IntegerWidth> = TWidth extends 64 ? bigint : number;

/**
 * `INTEGER(bitWidth, signed)` ⇄ `number` for the widths a double holds
 * exactly (8, 16 and 32, all stored in an `INT32`), and `bigint` for 64 bits.
 *
 * The narrow widths are a *domain*, not a layout: Parquet stores them all in
 * an `INT32`, and the annotation is what says how much of it is meant. Values
 * are range-checked **on the way in**, so nothing written through this can put
 * 300 in an `INTEGER(8, true)` column. Reading is another matter: a value some
 * other writer stored outside the annotated range comes back as it is stored,
 * because tavolato reports what a file holds rather than what it should have.
 *
 * This claims annotated columns only. An unannotated `INT32` or `INT64` is
 * already the built-in `i32` and `i64`, and those keep it.
 */
export function integer<TWidth extends IntegerWidth>(
  options: IntegerOptions<TWidth>,
): LogicalAdapter<IntegerValue<TWidth>, IntegerValue<TWidth>> {
  const { bitWidth, signed = true } = options;
  if (![8, 16, 32, 64].includes(bitWidth)) {
    throw invalid(`integer bitWidth must be 8, 16, 32 or 64, received ${describe(bitWidth)}`);
  }
  const name = `${signed ? "" : "u"}int${bitWidth}`;
  const annotate = (): Annotation => ({ kind: "integer", bitWidth, isSigned: signed });
  const matches = (annotation: Annotation): boolean =>
    annotation.kind === "integer" &&
    annotation.bitWidth === bitWidth &&
    annotation.isSigned === signed;

  const low = signed ? -(2n ** BigInt(bitWidth - 1)) : 0n;
  const high = signed ? 2n ** BigInt(bitWidth - 1) - 1n : 2n ** BigInt(bitWidth) - 1n;

  // As in `time`, the one cast is where the width stops being a type.
  const adapter =
    bitWidth === 64
      ? defineColumnType<bigint, bigint>({
          name,
          physical: "i64",
          matches,
          annotate,
          // An unsigned 64-bit column is the same bits in a signed INT64, which
          // is the whole reason the annotation exists.
          read: (raw) => (signed ? (raw as bigint) : BigInt.asUintN(64, raw as bigint)),
          write: (value) => {
            if (typeof value !== "bigint" || value < low || value > high) {
              reject(
                `${name} expects a bigint from ${low} to ${high}, received ${describe(value)}`,
              );
            }
            return BigInt.asIntN(64, value);
          },
        })
      : defineColumnType<number, number>({
          name,
          physical: "i32",
          matches,
          annotate,
          // Only a 32-bit unsigned column wraps: 8 and 16 bit values are
          // positive in an INT32 already.
          read: (raw) => (signed || bitWidth !== 32 ? (raw as number) : (raw as number) >>> 0),
          write: (value) => {
            if (!Number.isSafeInteger(value) || value < Number(low) || value > Number(high)) {
              reject(
                `${name} expects an integer from ${low} to ${high}, received ${describe(value)}`,
              );
            }
            return value | 0;
          },
        });
  return adapter as LogicalAdapter<IntegerValue<TWidth>, IntegerValue<TWidth>>;
}

/*
 * ---------------------------------------------------------------------------
 * The JSON seam
 *
 * A `json` column is stored as a JSON *string* in a `BYTE_ARRAY` annotated
 * `JSON` — that is the wire format, and it is the same one every other engine
 * writes — while the JavaScript value is the structure. The serializing is
 * `JSON.stringify`'s and the parsing is `JSON.parse`'s, so **the round-trip
 * semantics are JSON's, not tavolato's**: see {@link jsonTextOf}.
 *
 * The two halves below are shared by the built-in `json` column type and by
 * {@link json}, which is what keeps a hand-configured reviver an *override* of
 * one behaviour rather than a second one.
 * ---------------------------------------------------------------------------
 */

/**
 * A `JSON.parse` reviver or a `JSON.stringify` replacer. One signature serves
 * both, which is why {@link JsonOptions} spells it once.
 */
export type JsonHook = (key: string, value: unknown) => unknown;

/** The three keys that can reach `Object.prototype` when a parsed object is used. */
const isDangerousKey = (key: string): boolean =>
  key === "__proto__" || key === "prototype" || key === "constructor";

/**
 * The reviver a `json` column is parsed with unless you supply your own: it
 * drops `__proto__`, `prototype` and `constructor` keys from every object in
 * the document.
 *
 * **Two different layers, and it is worth being exact about which is which.** A
 * *column* named `__proto__` round-trips faithfully — the reader defines that
 * property rather than assigning it, so a file may carry a column by that name
 * and get it back untouched. A dangerous *key inside a json value* is a
 * different thing entirely: it is somebody's document, parsed into an object
 * your program will then use, and this reviver drops it.
 *
 * Exported so that a custom reviver can compose with it rather than replace it:
 *
 * @example
 * import { json, jsonReviver } from "tavolato";
 *
 * const dated = json({
 *   reviver: (key, value) => {
 *     const safe = jsonReviver(key, value);
 *     return typeof safe === "string" && ISO.test(safe) ? new Date(safe) : safe;
 *   },
 * });
 */
export const jsonReviver: JsonHook = (key, value) => (isDangerousKey(key) ? undefined : value);

/**
 * Serializes one `json` column value, naming the two ways `JSON.stringify` has
 * of not producing a document.
 *
 * **The JSON round-trip semantics are JSON's, not tavolato's.** Everything
 * `JSON.stringify` does quietly, it still does here: `NaN` and `±Infinity`
 * become `null`, an `undefined` property vanishes along with a function or a
 * symbol one, a `Date` becomes its ISO string and stays a string on the way
 * back, a `Map` becomes `{}`, and a `toJSON()` method is honoured. tavolato
 * refuses to invent a second JSON with different rules; what it does do is
 * turn the two cases `JSON.stringify` cannot express at all into typed errors
 * rather than a `TypeError` or a stored `undefined`.
 *
 * `column` names the column when the caller knows it — the built-in path does,
 * an adapter does not and has its column added when the writer wraps this.
 *
 * @internal
 */
export function jsonTextOf(value: unknown, column?: string, replacer?: JsonHook): string {
  let text: string | undefined;
  try {
    text = JSON.stringify(value, replacer);
  } catch (cause) {
    // A bigint anywhere in the document is the case that gets here: JSON has no
    // spelling for one, and picking a string or a lossy number on somebody's
    // behalf is exactly the guess tavolato refuses everywhere else.
    throw new TavolatoError(
      `A json value must be something JSON.stringify accepts, and ${describe(value)} is not: ${
        cause instanceof Error ? cause.message : describe(cause)
      }`,
      "ERR_ROW_VALUE_INVALID",
      column,
      cause,
    );
  }
  if (text === undefined) {
    // `undefined`, a function or a symbol at the *top* level. Inside an object
    // these vanish; as the whole value there is nothing left to store.
    throw new TavolatoError(
      `A json value must serialize to a JSON document, and ${describe(value)} serializes to nothing at all`,
      "ERR_ROW_VALUE_INVALID",
      column,
    );
  }
  return text;
}

/**
 * Parses one `json` column value, with {@link jsonReviver} unless the caller
 * brought its own.
 *
 * A JSON-annotated column is only *claimed* to hold JSON. Any file may say so
 * about any bytes, so a parse failure is the file being malformed rather than
 * anything the caller did.
 *
 * @internal
 */
export function jsonValueOf(
  text: string,
  column?: string,
  reviver: JsonHook = jsonReviver,
): unknown {
  try {
    return JSON.parse(text, reviver) as unknown;
  } catch (cause) {
    throw malformed(
      `A JSON-annotated column holds ${describe(text.length > 64 ? `${text.slice(0, 64)}…` : text)}, which is not valid JSON`,
      column,
      cause,
    );
  }
}

/** Options for {@link json}. */
export interface JsonOptions {
  /**
   * Reviver handed to `JSON.parse`. **Replaces** {@link jsonReviver} rather
   * than running after it: bringing your own means owning it, dangerous keys
   * included. Composing is one call — `jsonReviver` is exported for exactly
   * that.
   */
  reviver?: JsonHook;
  /** Replacer handed to `JSON.stringify`. */
  replacer?: JsonHook;
}

/**
 * `JSON` ⇄ the parsed document, with a reviver and a replacer of your own.
 *
 * The built-in `json` column type already parses and serializes; this is the
 * same column with the two hooks opened up, the way `ofetch` lets a caller
 * take over response parsing without giving up the default. It claims the
 * `JSON` annotation exactly as the built-in does, and writes the identical
 * bytes — the difference is only which functions run either side of them.
 *
 * Registered in `ReadOptions.types` it wins over the built-in, because adapters
 * are consulted first; in a schema it is chosen by declaring it as the column's
 * `type`.
 *
 * `TValue` is what your hooks map to and from, and defaults to
 * {@link JsonValue}. Name it when your documents have a shape — that is also
 * the way out of `JsonValue`'s index-signature strictness, which an `interface`
 * cannot satisfy.
 *
 * @example
 * const schema = defineSchema({ payload: { type: json<Payload>() } });
 *
 * @example
 * // Keeps the dangerous keys the default reviver drops:
 * const raw = json({ reviver: (_key, value) => value });
 *
 * @throws {TavolatoError} `ERR_SCHEMA_COLUMN_INVALID` when a hook is not a function.
 */
/*
 * Two overloads rather than one default type argument: a defaulted parameter
 * does not survive `defineSchema`'s `const` inference, and a `json()` column
 * would come back out of `ReadRowOf` as `any` instead of a `JsonValue`. The
 * concrete signature is the one an unparameterized call picks; supplying a type
 * argument skips it on arity and lands on the generic one.
 */
export function json(options?: JsonOptions): LogicalAdapter<JsonValue, JsonValue>;
export function json<TValue>(options?: JsonOptions): LogicalAdapter<TValue, TValue>;
export function json<TValue>(options: JsonOptions = {}): LogicalAdapter<TValue, TValue> {
  const { reviver = jsonReviver, replacer } = options;
  if (typeof reviver !== "function") {
    throw invalid(`json reviver must be a function, received ${describe(reviver)}`);
  }
  if (replacer !== undefined && typeof replacer !== "function") {
    throw invalid(`json replacer must be a function, received ${describe(replacer)}`);
  }

  return defineColumnType<TValue, TValue>({
    name: "json",
    physical: "bytes",
    matches: (annotation) => annotation.kind === "json",
    annotate: () => ({ kind: "json" }),
    read: (raw) => jsonValueOf(decodeUtf8(raw as Uint8Array), undefined, reviver) as TValue,
    write: (value) => utf8.encode(jsonTextOf(value, undefined, replacer)),
  });
}
