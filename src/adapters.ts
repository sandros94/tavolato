import { decodeUtf8, encodeUtf8Exact, utf8 } from "./internal/bytes.ts";
import {
  decimalFixedLength,
  decimalPhysicalCanHold,
  logicalTypePhysicalProblem,
} from "./internal/logical.ts";
import {
  adapterUnsupported,
  assertOptionsObject,
  describe,
  malformed,
  TavolatoError,
} from "./error.ts";
import { JSON_NULL } from "./json-null.ts";
import type {
  Annotation,
  JsonDocument,
  LogicalAdapter,
  PhysicalKind,
  TimeUnitName,
} from "./types.ts";

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

const MAX_ANNOTATION_INTEGER = 2 ** 31 - 1;

function invalid(message: string): TavolatoError {
  return new TavolatoError(message, "ERR_SCHEMA_COLUMN_INVALID");
}

/** A value the adapter was handed, or handed back, that it cannot work with. */
function reject(message: string): never {
  throw new TavolatoError(message, "ERR_ROW_VALUE_INVALID");
}

/**
 * Validates `annotation` and copies the exact scalar values inspected.
 *
 * `unknown` is the one member an adapter may not produce: it stands for an
 * annotation *this version has no name for*, and there is no way to write one
 * back out.
 */
function inspectAnnotation(annotation: unknown): Annotation | string {
  if (typeof annotation !== "object" || annotation === null) {
    return `annotate() must return an annotation, received ${describe(annotation)}`;
  }
  const value = annotation as { readonly kind?: unknown };
  const kind = value.kind;
  if (typeof kind !== "string" || !WRITABLE_ANNOTATIONS.has(kind)) {
    return `annotate() returned the annotation kind ${describe(kind)}, which cannot be written`;
  }
  if (kind === "decimal") {
    const decimal = annotation as { readonly precision?: unknown; readonly scale?: unknown };
    const precision = decimal.precision;
    const scale = decimal.scale;
    return typeof precision === "number" &&
      Number.isSafeInteger(precision) &&
      precision >= 1 &&
      precision <= MAX_ANNOTATION_INTEGER &&
      typeof scale === "number" &&
      Number.isSafeInteger(scale) &&
      scale >= 0 &&
      scale <= precision
      ? Object.freeze({ kind, precision, scale })
      : `annotate() returned a decimal annotation of precision ${describe(precision)} and scale ${describe(scale)}; precision must be a positive i32 and scale an integer from 0 to the precision`;
  }
  if (kind === "time" || kind === "timestamp") {
    const timed = annotation as {
      readonly unit?: unknown;
      readonly isAdjustedToUTC?: unknown;
    };
    const unit = timed.unit;
    const isAdjustedToUTC = timed.isAdjustedToUTC;
    return typeof unit === "string" && TIME_UNITS.has(unit) && typeof isAdjustedToUTC === "boolean"
      ? Object.freeze({ kind, unit: unit as TimeUnitName, isAdjustedToUTC })
      : `annotate() returned a ${kind} annotation without a unit and a UTC flag`;
  }
  if (kind === "integer") {
    const integer = annotation as { readonly bitWidth?: unknown; readonly isSigned?: unknown };
    const bitWidth = integer.bitWidth;
    const isSigned = integer.isSigned;
    return typeof bitWidth === "number" &&
      [8, 16, 32, 64].includes(bitWidth) &&
      typeof isSigned === "boolean"
      ? Object.freeze({ kind, bitWidth: bitWidth as 8 | 16 | 32 | 64, isSigned })
      : "annotate() returned an integer annotation without a bit width of 8, 16, 32 or 64";
  }
  return Object.freeze({ kind }) as Annotation;
}

/** Adapter metadata read once and held stable for one validation boundary. */
export interface AdapterInspection {
  readonly adapter: LogicalAdapter<unknown, unknown>;
  readonly name: string;
  readonly physical: PhysicalKind;
  readonly typeLength: number | undefined;
  readonly annotation: Annotation;
}

/**
 * Inspects every structural and format-bearing adapter property once.
 *
 * The string result is suitable for the accepting boundary's typed error;
 * otherwise the returned metadata is the exact snapshot that boundary
 * validated and may safely carry forward.
 *
 * @internal
 */
export function inspectAdapter(spec: unknown): AdapterInspection | string {
  if (typeof spec !== "object" || spec === null) {
    return `expects a column type such as decimal({ precision: 10, scale: 2 }), received ${describe(spec)}`;
  }
  const adapter = spec as LogicalAdapter<unknown, unknown>;
  const name = adapter.name;
  if (typeof name !== "string" || name === "") {
    return `has no name; a column type needs one so that errors can say which it is`;
  }
  const physical = adapter.physical;
  if (!PHYSICAL_KINDS.has(physical)) {
    return `${name} declares the physical type ${describe(physical)}, which is not one of ${[
      ...PHYSICAL_KINDS,
    ].join(", ")}`;
  }
  const typeLength = adapter.typeLength;
  if (physical === "fixed") {
    if (typeof typeLength !== "number" || !Number.isSafeInteger(typeLength) || typeLength < 1) {
      return `${name} is stored as a FIXED_LEN_BYTE_ARRAY and must declare a positive integer typeLength, received ${describe(typeLength)}`;
    }
  } else if (typeLength !== undefined) {
    return `${name} declares a typeLength but is not stored as a FIXED_LEN_BYTE_ARRAY`;
  }
  const matches: unknown = Reflect.get(adapter, "matches");
  const annotate: unknown = Reflect.get(adapter, "annotate");
  const read: unknown = Reflect.get(adapter, "read");
  const write: unknown = Reflect.get(adapter, "write");
  for (const [method, implementation] of [
    ["matches", matches],
    ["annotate", annotate],
    ["read", read],
    ["write", write],
  ] as const) {
    if (typeof implementation !== "function") return `${name} has no ${method}() function`;
  }
  const acceptsPhysical: unknown = Reflect.get(adapter, "acceptsPhysical");
  if (acceptsPhysical !== undefined && typeof acceptsPhysical !== "function") {
    return `${name} has a non-function acceptsPhysical property`;
  }
  let annotation: unknown;
  try {
    annotation = Reflect.apply(annotate as (...args: never[]) => unknown, adapter, []);
  } catch (cause) {
    return `${name} threw from annotate(): ${cause instanceof Error ? cause.message : describe(cause)}`;
  }
  const inspectedAnnotation = inspectAnnotation(annotation);
  if (typeof inspectedAnnotation === "string") return `${name} ${inspectedAnnotation}`;
  const physicalProblem = logicalTypePhysicalProblem(inspectedAnnotation, physical, typeLength);
  if (physicalProblem !== undefined) return `${name} ${physicalProblem}`;
  return {
    adapter,
    name,
    physical,
    typeLength,
    annotation: inspectedAnnotation,
  };
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
  const inspected = inspectAdapter(spec);
  return typeof inspected === "string" ? inspected : undefined;
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
const MIN_INT32 = -(2 ** 31);
const MAX_INT32 = 2 ** 31 - 1;
const MIN_DATE_DAYS = -100_000_000;
const MAX_DATE_DAYS = 100_000_000;

/** JavaScript representation used by {@link date}. */
export type DateRepresentation = "date" | "number";

/** Options for {@link date}. */
export interface DateOptions<TAs extends DateRepresentation = DateRepresentation> {
  /** `Date`, or a signed count of days since the Unix epoch. Defaults to `"date"`. */
  readonly as?: TAs;
}

/** Value read and written by a {@link date} adapter. */
export type DateValue<TAs extends DateRepresentation> = TAs extends "number" ? number : Date;

/**
 * `DATE` ⇄ `Date` or a day count, stored as days since the Unix epoch in an
 * `INT32`.
 *
 * The default `"date"` representation requires exactly UTC midnight and
 * covers the ±100,000,000 days JavaScript can represent. `"number"` preserves
 * the complete signed `INT32` domain Parquet permits. Each representation is
 * stable: values never change type according to their magnitude.
 */
export function date<TAs extends DateRepresentation = "date">(
  options: DateOptions<TAs> = {},
): LogicalAdapter<DateValue<TAs>, DateValue<TAs>> {
  assertOptionsObject(options, "date options", "ERR_SCHEMA_COLUMN_INVALID");
  const { as = "date" } = options;
  if (as !== "date" && as !== "number") {
    throw invalid(`date as must be "date" or "number", received ${describe(as)}`);
  }

  const common = {
    name: "date",
    physical: "i32",
    matches: (annotation: Annotation) => annotation.kind === "date",
    annotate: (): Annotation => ({ kind: "date" }),
  } as const;
  const adapter =
    as === "number"
      ? defineColumnType<number, number>({
          ...common,
          read: (raw) => raw as number,
          write: (value) => {
            if (!Number.isSafeInteger(value) || value < MIN_INT32 || value > MAX_INT32) {
              reject(
                `date as number expects a signed 32-bit integer from ${MIN_INT32} to ${MAX_INT32}, received ${describe(value)}`,
              );
            }
            return value;
          },
        })
      : defineColumnType<Date, Date>({
          ...common,
          read: (raw) => {
            const days = raw as number;
            if (days < MIN_DATE_DAYS || days > MAX_DATE_DAYS) {
              throw adapterUnsupported(
                `a DATE holding ${days} days since the Unix epoch, outside JavaScript Date's range of ${MIN_DATE_DAYS} to ${MAX_DATE_DAYS} days`,
                `register date({ as: "number" }) in ReadOptions.types to read the count itself`,
              );
            }
            return new Date(days * MILLIS_PER_DAY);
          },
          write: (value) => {
            if (!(value instanceof Date))
              reject(`date expects a Date, received ${describe(value)}`);
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
  return adapter as LogicalAdapter<DateValue<TAs>, DateValue<TAs>>;
}

/** Options for {@link decimal}. */
export interface DecimalOptions {
  /** Total significant digits, from 1 through Parquet's signed-i32 maximum. */
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
 * Writes use `INT32` up to 9 digits, `INT64` up to 18, then the smallest
 * `FIXED_LEN_BYTE_ARRAY` that can carry the precision. The same adapter reads
 * every physical layout Parquet permits for the declared precision, including
 * `BYTE_ARRAY` and wider fixed arrays.
 */
export function decimal(options: DecimalOptions): LogicalAdapter<string, string> {
  assertOptionsObject(options, "decimal options", "ERR_SCHEMA_COLUMN_INVALID");
  const { precision, scale = 0 } = options;
  if (!Number.isSafeInteger(precision) || precision < 1 || precision > MAX_ANNOTATION_INTEGER) {
    throw invalid(
      `decimal precision must be an integer from 1 to ${MAX_ANNOTATION_INTEGER}, received ${describe(precision)}`,
    );
  }
  if (!Number.isSafeInteger(scale) || scale < 0 || scale > precision) {
    throw invalid(
      `decimal scale must be an integer from 0 to the precision, received ${describe(scale)}`,
    );
  }

  const physical: PhysicalKind = precision <= 9 ? "i32" : precision <= 18 ? "i64" : "fixed";
  const typeLength = physical === "fixed" ? decimalFixedLength(precision) : undefined;

  return defineColumnType<string, string>({
    name: `decimal(${precision}, ${scale})`,
    physical,
    ...(typeLength === undefined ? {} : { typeLength }),
    acceptsPhysical: (found, foundTypeLength) =>
      decimalPhysicalCanHold(precision, found, foundTypeLength),
    matches: (annotation) =>
      annotation.kind === "decimal" &&
      annotation.precision === precision &&
      annotation.scale === scale,
    annotate: () => ({ kind: "decimal", precision, scale }),
    read: (raw) => {
      if (raw instanceof Uint8Array && raw.length === 0) {
        throw malformed("DECIMAL BYTE_ARRAY values must contain at least one byte");
      }
      const unscaled =
        typeof raw === "number"
          ? BigInt(raw)
          : typeof raw === "bigint"
            ? raw
            : fromTwosComplement(raw as Uint8Array);
      return toDecimalString(unscaled, precision, scale);
    },
    write: (value) => {
      if (typeof value !== "string") {
        reject(`decimal expects a string, received ${describe(value)}`);
      }
      const unscaled = decimalUnscaled(value, precision, scale);
      return physical === "fixed"
        ? toTwosComplement(unscaled, typeLength as number)
        : physical === "i64"
          ? unscaled
          : Number(unscaled);
    },
  });
}

/** Parses one canonical decimal only after validating its shape and precision. */
function decimalUnscaled(value: string, precision: number, scale: number): bigint {
  const signLength = value.startsWith("-") ? 1 : 0;
  const point = scale === 0 ? value.length : value.length - scale - 1;
  const integerDigits = point - signLength;
  const canonical = (): never =>
    reject(
      `decimal(${precision}, ${scale}) expects a canonical decimal string with exactly ${scale} digit${
        scale === 1 ? "" : "s"
      } after the point, received ${describe(value)}`,
    );

  if (
    integerDigits < 1 ||
    (scale > 0 && value[point] !== ".") ||
    (integerDigits > 1 && value[signLength] === "0")
  ) {
    canonical();
  }

  let significantDigits = 0;
  let nonzero = false;
  for (let index = signLength; index < value.length; index++) {
    if (index === point && scale > 0) continue;
    const digit = value.charCodeAt(index) - 48;
    if (digit < 0 || digit > 9) canonical();
    if (digit !== 0) nonzero = true;
    if (nonzero) significantDigits++;
  }
  if (signLength === 1 && !nonzero) {
    reject(`decimal has no negative zero, received ${describe(value)}`);
  }
  if (significantDigits > precision) {
    reject(`decimal(${precision}, ${scale}) cannot hold ${describe(value)}`);
  }

  const encoded = scale === 0 ? value : `${value.slice(0, point)}${value.slice(point + 1)}`;
  return BigInt(encoded);
}

/** Validates and renders an unscaled integer as the canonical decimal string. */
function toDecimalString(unscaled: bigint, precision: number, scale: number): string {
  const negative = unscaled < 0n;
  const magnitude = (negative ? -unscaled : unscaled).toString();
  if (magnitude.length > precision) {
    throw malformed(`DECIMAL value exceeds its declared precision ${precision}`);
  }
  const digits = magnitude.padStart(scale + 1, "0");
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
  /** Whether the value is adjusted to UTC, exactly as declared by Parquet. */
  isAdjustedToUTC: boolean;
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

function assertUTCFlag(isAdjustedToUTC: boolean, what: string): void {
  if (typeof isAdjustedToUTC !== "boolean") {
    throw invalid(
      `${what} isAdjustedToUTC must be true or false, received ${describe(isAdjustedToUTC)}`,
    );
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
 * `isAdjustedToUTC` is preserved exactly: `false` is a local time and `true`
 * is normalized to UTC. An adapter claims only the annotation it declares.
 */
export function time<TUnit extends TimeUnitName>(
  options: TimeOptions<TUnit>,
): LogicalAdapter<TimeValue<TUnit>, TimeValue<TUnit>> {
  assertOptionsObject(options, "time options", "ERR_SCHEMA_COLUMN_INVALID");
  const { unit, isAdjustedToUTC } = options;
  assertUnit(unit, "time");
  assertUTCFlag(isAdjustedToUTC, "time");
  const perDay = UNITS_PER_DAY[unit];
  const annotate = (): Annotation => ({ kind: "time", unit, isAdjustedToUTC });
  const matches = (annotation: Annotation): boolean =>
    annotation.kind === "time" &&
    annotation.unit === unit &&
    annotation.isAdjustedToUTC === isAdjustedToUTC;

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
 * `TIMESTAMP` ⇄ `bigint`, a count from the 1970-01-01 reference in an `INT64`.
 *
 * The built-in `timestamp` column type is milliseconds as a `Date`, which is
 * exactly what a `Date` holds. Microseconds and nanoseconds are not: a `Date`
 * would round them, so this hands back the count itself and loses nothing.
 * Milliseconds are offered here too, for the same reason in reverse — a
 * `bigint` when you want the raw count rather than a `Date`.
 *
 * `isAdjustedToUTC` is preserved exactly: `false` is a local date-time and
 * `true` is an instant normalized to UTC. An adapter claims only the annotation
 * it declares.
 */
export function timestamp(options: TimeOptions): LogicalAdapter<bigint, bigint> {
  assertOptionsObject(options, "timestamp options", "ERR_SCHEMA_COLUMN_INVALID");
  const { unit, isAdjustedToUTC } = options;
  assertUnit(unit, "timestamp");
  assertUTCFlag(isAdjustedToUTC, "timestamp");
  return defineColumnType<bigint, bigint>({
    name: `timestamp(${unit})`,
    physical: "i64",
    matches: (annotation) =>
      annotation.kind === "timestamp" &&
      annotation.unit === unit &&
      annotation.isAdjustedToUTC === isAdjustedToUTC,
    annotate: () => ({ kind: "timestamp", unit, isAdjustedToUTC }),
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

/** JavaScript representation used by {@link float16}. */
export type Float16Representation = "number" | "bits";

/** Options for {@link float16}. */
export interface Float16Options {
  /** Rounded numeric value, or the exact unsigned 16-bit encoding. Defaults to `"number"`. */
  readonly as?: Float16Representation;
}

/**
 * `FLOAT16` ⇄ its numeric value or exact bit pattern, stored in a 2-byte
 * `FIXED_LEN_BYTE_ARRAY`.
 *
 * The default `"number"` representation rounds to half precision on write and
 * reads the stored numeric value. `"bits"` instead accepts and returns an
 * unsigned integer from 0 through 65,535, preserving every encoding exactly —
 * including NaN payloads and signs and both zero encodings.
 */
export function float16(options: Float16Options = {}): LogicalAdapter<number, number> {
  assertOptionsObject(options, "float16 options", "ERR_SCHEMA_COLUMN_INVALID");
  const { as = "number" } = options;
  if (as !== "number" && as !== "bits") {
    throw invalid(`float16 as must be "number" or "bits", received ${describe(as)}`);
  }

  const common = {
    name: "float16",
    physical: "fixed",
    typeLength: 2,
    matches: (annotation: Annotation) => annotation.kind === "float16",
    annotate: (): Annotation => ({ kind: "float16" }),
  } as const;
  return as === "bits"
    ? defineColumnType<number, number>({
        ...common,
        read: (raw) => halfBitsFromBytes(raw as Uint8Array),
        write: (value) => {
          if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff) {
            reject(
              `float16 as bits expects an unsigned 16-bit integer from 0 to 65535, received ${describe(value)}`,
            );
          }
          return halfBytesFromBits(value);
        },
      })
    : defineColumnType<number, number>({
        ...common,
        read: (raw) => numberFromHalf(halfBitsFromBytes(raw as Uint8Array)),
        write: (value) => {
          if (typeof value !== "number") {
            reject(`float16 expects a number, received ${describe(value)}`);
          }
          return halfBytesFromBits(halfFromNumber(value));
        },
      });
}

/** Reads Parquet's little-endian binary16 bytes as their unsigned bit pattern. */
function halfBitsFromBytes(bytes: Uint8Array): number {
  return bytes[0] | (bytes[1] << 8);
}

/** Writes an unsigned binary16 bit pattern in Parquet's little-endian order. */
function halfBytesFromBits(bits: number): Uint8Array {
  return new Uint8Array([bits & 0xff, bits >>> 8]);
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
  assertOptionsObject(options, "integer options", "ERR_SCHEMA_COLUMN_INVALID");
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
 * writes. JavaScript may see either the parsed value or that exact source text.
 * Value-mode serializing is `JSON.stringify`'s and parsing is `JSON.parse`'s,
 * so **its round-trip semantics are JSON's, not tavolato's**: see
 * {@link jsonTextOf}.
 *
 * The two halves below are shared by the built-in `json` column type and by
 * {@link json}, so parsing, sanitization and serialization have one contract.
 * ---------------------------------------------------------------------------
 */

/**
 * A `JSON.parse` reviver or a `JSON.stringify` replacer. One signature serves
 * both, which is why {@link JsonOptions} spells it once.
 */
export type JsonHook = (key: string, value: unknown) => unknown;

/** JavaScript representation used by {@link json}. */
export type JsonRepresentation = "value" | "text";

/** Policy for dangerous own keys in a parsed value-mode JSON document. */
export type JsonDangerousKeys = "drop" | "preserve";

/** The three keys that can reach `Object.prototype` when a parsed object is used. */
const DANGEROUS_JSON_KEYS = ["__proto__", "prototype", "constructor"] as const;

const isDangerousKey = (key: string): boolean =>
  DANGEROUS_JSON_KEYS.some((dangerous) => key === dangerous);

/**
 * An idempotent `JSON.parse` reviver that drops `__proto__`, `prototype` and
 * `constructor` keys from every object in a document.
 *
 * **Two different layers, and it is worth being exact about which is which.** A
 * *column* named `__proto__` round-trips faithfully — the reader defines that
 * property rather than assigning it, so a file may carry a column by that name
 * and get it back untouched. A dangerous *key inside a json value* is a
 * different thing entirely: it is somebody's document, parsed into an object
 * your program will then use, and this reviver drops it.
 *
 * Value-mode {@link json} sanitizes the final graph after any custom reviver,
 * so callers do not need to compose this hook for safety. It remains exported
 * for direct `JSON.parse` use and explicit composition.
 *
 * @example
 * import { jsonReviver } from "tavolato";
 *
 * const safe = JSON.parse(source, jsonReviver);
 */
export const jsonReviver: JsonHook = (key, value) => (isDangerousKey(key) ? undefined : value);

/** True for values whose own data properties may lead to more graph nodes. */
function isObject(value: unknown): value is object {
  return (typeof value === "object" && value !== null) || typeof value === "function";
}

/**
 * Removes dangerous own keys from a materialized graph without reading a
 * property value through ordinary access. Revivers may introduce cycles,
 * accessors, proxies and locked descriptors, so traversal is iterative and a
 * reflective operation that reports incomplete removal is a failure rather
 * than a partial guard. A custom reviver is trusted executable code: hostile
 * proxy traps, prototypes, accessors, exotic internals and later mutation stay
 * on that caller-controlled side of the boundary.
 */
function dropDangerousKeys(value: unknown): void {
  if (!isObject(value)) return;
  const seen = new WeakSet<object>();
  const pending: object[] = [value];
  while (pending.length > 0) {
    const current = pending.pop() as object;
    if (seen.has(current)) continue;
    seen.add(current);

    // Do not trust enumeration to disclose these names: a Proxy may legally
    // omit a configurable own property from ownKeys. Probe, delete and verify
    // each known key independently before enumeration is used for traversal.
    for (const key of DANGEROUS_JSON_KEYS) {
      Object.hasOwn(current, key);
      if (!Reflect.deleteProperty(current, key) || Object.hasOwn(current, key)) {
        throw new TypeError(`Cannot remove dangerous JSON key ${JSON.stringify(key)}`);
      }
    }

    for (const key of Reflect.ownKeys(current)) {
      if (typeof key === "string" && isDangerousKey(key)) continue;
      const descriptor = Reflect.getOwnPropertyDescriptor(current, key);
      if (descriptor !== undefined && "value" in descriptor && isObject(descriptor.value)) {
        pending.push(descriptor.value);
      }
    }
  }
}

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
  if (value === JSON_NULL) return "null";
  const checkedReplacer: JsonHook = function (this: unknown, key, found) {
    if (found === JSON_NULL) {
      throw new TavolatoError(
        "JSON_NULL is only valid as the complete top-level json document",
        "ERR_ROW_VALUE_INVALID",
        column,
      );
    }
    const replaced = replacer === undefined ? found : Reflect.apply(replacer, this, [key, found]);
    if (replaced === JSON_NULL) {
      throw new TavolatoError(
        "A json replacer cannot introduce JSON_NULL below the document boundary",
        "ERR_ROW_VALUE_INVALID",
        column,
      );
    }
    return replaced;
  };
  let text: string | undefined;
  try {
    text = JSON.stringify(value, checkedReplacer);
  } catch (cause) {
    // Bigints and nested JSON_NULL sentinels are the in-box cases that get
    // here; caller hooks and cyclic documents may throw too. None may escape
    // as an untyped platform or callback error.
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
 * Parses one `json` column value, then applies its dangerous-key policy after
 * any caller reviver has completed.
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
  reviver?: JsonHook,
  dangerousKeys: JsonDangerousKeys = "drop",
): unknown {
  let value: unknown;
  try {
    value = JSON.parse(text, reviver) as unknown;
  } catch (cause) {
    throw invalidJsonSource(text, column, cause);
  }
  if (dangerousKeys === "drop") {
    try {
      dropDangerousKeys(value);
    } catch (cause) {
      throw malformed(
        "A JSON-annotated column produced a value whose dangerous keys could not be removed",
        column,
        cause,
      );
    }
  }
  return value === null ? JSON_NULL : value;
}

/** One malformed-file contract for every representation of JSON source. */
function invalidJsonSource(
  text: string,
  column: string | undefined,
  cause: unknown,
): TavolatoError {
  return malformed(
    `A JSON-annotated column holds ${describe(text.length > 64 ? `${text.slice(0, 64)}…` : text)}, which is not valid JSON`,
    column,
    cause,
  );
}

/** Options for the parsed-value representation of {@link json}. */
export interface JsonValueOptions {
  /** Parse to and serialize from a JavaScript value. Defaults to `"value"`. */
  readonly as?: "value";
  /**
   * Dangerous own keys are removed after parsing and reviving by default.
   * Native JSON receives the strong guarantee. A custom reviver is trusted
   * code; sanitization covers its reflectively exposed graph and direct known
   * keys while preserving identity, prototypes, cycles, safe accessors and
   * exotic values whose dangerous own keys are removable. Otherwise reading
   * fails with `ERR_READ_MALFORMED`; use `"preserve"` only when downstream
   * code is prepared to retain every key and trust exotic internals.
   */
  readonly dangerousKeys?: JsonDangerousKeys;
  /** Reviver handed to `JSON.parse`; sanitization runs after it returns. */
  readonly reviver?: JsonHook;
  /** Replacer handed to `JSON.stringify`. */
  readonly replacer?: JsonHook;
}

/** Options for the exact-text representation of {@link json}. */
export interface JsonTextOptions {
  /** Preserve the complete JSON document as source text. */
  readonly as: "text";
  /** Text is not materialized into an object graph. */
  readonly dangerousKeys?: never;
  /** Text is not parsed into a value, so a reviver has no meaning. */
  readonly reviver?: never;
  /** Text is not serialized from a value, so a replacer has no meaning. */
  readonly replacer?: never;
}

/** Options for {@link json}, discriminated by its JavaScript representation. */
export type JsonOptions = JsonValueOptions | JsonTextOptions;

/** Validates a JSON source string without replacing the source it spells. */
function jsonSourceOf(text: string, column?: string): string {
  try {
    JSON.parse(text);
    return text;
  } catch (cause) {
    throw invalidJsonSource(text, column, cause);
  }
}

/** Validates exact JSON source and returns the bytes that preserve it. */
function jsonSourceBytesOf(value: unknown): Uint8Array {
  if (typeof value !== "string") {
    reject(`json as text expects a string, received ${describe(value)}`);
  }
  try {
    JSON.parse(value);
  } catch (cause) {
    throw new TavolatoError(
      `json as text expects a complete valid JSON document, received ${describe(value.length > 64 ? `${value.slice(0, 64)}…` : value)}`,
      "ERR_ROW_VALUE_INVALID",
      undefined,
      cause,
    );
  }
  const bytes = encodeUtf8Exact(value);
  if (bytes === undefined) {
    throw new TavolatoError(
      "json as text expects source that has an exact UTF-8 representation",
      "ERR_ROW_VALUE_INVALID",
    );
  }
  return bytes;
}

/**
 * `JSON` ⇄ a parsed value or its exact source text.
 *
 * The built-in `json` column type already parses and serializes; this factory
 * makes that representation explicit, opens its two hooks, or exposes the
 * validated source text instead. Every representation claims the same `JSON`
 * annotation and writes the same wire format.
 *
 * Registered in `ReadOptions.types` it wins over the built-in, because adapters
 * are consulted first; in a schema it is chosen by declaring it as the column's
 * `type`.
 *
 * The default `"value"` representation parses on read and serializes on write.
 * `"text"` validates both directions but preserves whitespace, key order and
 * every other byte of valid source rather than normalizing it.
 *
 * In value mode, `TValue` is what your hooks map to and from, and defaults to
 * {@link JsonDocument}. Name it when your documents have a shape — that is also
 * the way out of its recursive index-signature strictness, which an `interface`
 * cannot satisfy.
 *
 * @example
 * const schema = defineSchema({ payload: { type: json<Payload>() } });
 *
 * @example
 * // Explicitly retains dangerous own keys in the parsed graph:
 * const raw = json({ dangerousKeys: "preserve" });
 *
 * @throws {TavolatoError} `ERR_SCHEMA_COLUMN_INVALID` for an unknown
 * representation or dangerous-key policy, an invalid hook, or a value-mode
 * option supplied in text mode.
 */
/*
 * Overloads rather than one default type argument: a defaulted parameter
 * does not survive `defineSchema`'s `const` inference, and a `json()` column
 * would come back out of `ReadRowOf` as `any` instead of a `JsonDocument`. The
 * concrete signature is the one an unparameterized call picks; supplying a type
 * argument skips it on arity and lands on the generic one.
 */
export function json(options: JsonTextOptions): LogicalAdapter<string, string>;
export function json(options?: JsonValueOptions): LogicalAdapter<JsonDocument, JsonDocument>;
export function json<TValue>(options?: JsonValueOptions): LogicalAdapter<TValue, TValue>;
export function json(
  options: JsonOptions,
): LogicalAdapter<JsonDocument | string, JsonDocument | string>;
export function json<TValue>(
  options: JsonOptions = {},
): LogicalAdapter<TValue, TValue> | LogicalAdapter<string, string> {
  assertOptionsObject(options, "json options", "ERR_SCHEMA_COLUMN_INVALID");
  const { as = "value" } = options;
  if (as !== "value" && as !== "text") {
    throw invalid(`json as must be "value" or "text", received ${describe(as)}`);
  }
  if (as === "text") {
    if (options.dangerousKeys !== undefined) {
      throw invalid("json as text does not accept a dangerousKeys policy");
    }
    if (options.reviver !== undefined) {
      throw invalid("json as text does not accept a reviver");
    }
    if (options.replacer !== undefined) {
      throw invalid("json as text does not accept a replacer");
    }
    return defineColumnType<string, string>({
      name: "json",
      physical: "bytes",
      matches: (annotation) => annotation.kind === "json",
      annotate: () => ({ kind: "json" }),
      read: (raw) => jsonSourceOf(decodeUtf8(raw as Uint8Array)),
      write: jsonSourceBytesOf,
    });
  }

  const { dangerousKeys = "drop", reviver, replacer } = options;
  if (dangerousKeys !== "drop" && dangerousKeys !== "preserve") {
    throw invalid(
      `json dangerousKeys must be "drop" or "preserve", received ${describe(dangerousKeys)}`,
    );
  }
  if (reviver !== undefined && typeof reviver !== "function") {
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
    read: (raw) =>
      jsonValueOf(decodeUtf8(raw as Uint8Array), undefined, reviver, dangerousKeys) as TValue,
    write: (value) => utf8.encode(jsonTextOf(value, undefined, replacer)),
  });
}
