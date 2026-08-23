import { ByteWriter, utf8 } from "./internal/bytes.ts";
import { type ColumnValues, encodeRleBitPackedHybrid, writePlain } from "./internal/encoding.ts";
import {
  type ColumnChunkMeta,
  encodeDataPageHeader,
  encodeFileMetadata,
  MAGIC,
  MAX_DEFINITION_LEVEL_BIT_WIDTH,
  type RowGroupMeta,
} from "./internal/format.ts";
import { TavolatoError } from "./error.ts";
import type { ParquetSchema, Row, SchemaColumn, SchemaDefinition, WriterOptions } from "./types.ts";

const DEFAULT_ROW_GROUP_SIZE = 10_000;
const DEFAULT_CREATED_BY = "tavolato";

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

/**
 * `PageHeader` stores page sizes as signed 32-bit integers, so a page body may
 * never reach 2^31 bytes. Chunks are cut into a new row group well before that
 * boundary; the 64 KiB of slack costs nothing and keeps the header math clear
 * of the edge.
 */
const MAX_PAGE_BYTES = 0x7f_ff_00_00;

/** `DataPageHeader.num_values` is an i32, so a row group cannot hold more rows. */
const MAX_ROWS_PER_GROUP = 0x7f_ff_ff_ff;

/** A validated value on its way into a column buffer; `null` means "no value". */
type StagedValue =
  | { readonly kind: "bytes"; readonly value: Uint8Array }
  | { readonly kind: "f64"; readonly value: number }
  | { readonly kind: "i64"; readonly value: bigint }
  | { readonly kind: "bool"; readonly value: boolean }
  | null;

interface ColumnState {
  readonly column: SchemaColumn;
  readonly values: ColumnValues;
  /** Definition levels for the current row group; unused for required columns. */
  readonly levels: number[];
  nullCount: number;
  /** PLAIN-encoded size of the buffered BYTE_ARRAY values; other kinds derive theirs from the item count. */
  byteArraySize: number;
}

function createColumnState(column: SchemaColumn): ColumnState {
  let values: ColumnValues;
  switch (column.type) {
    case "string": {
      values = { kind: "bytes", items: [] };
      break;
    }
    case "f64": {
      values = { kind: "f64", items: [] };
      break;
    }
    case "bool": {
      values = { kind: "bool", items: [] };
      break;
    }
    default: {
      // i64 and timestamp both land on INT64.
      values = { kind: "i64", items: [] };
    }
  }
  return { column, values, levels: [], nullCount: 0, byteArraySize: 0 };
}

function resetColumnState(state: ColumnState): void {
  state.values.items.length = 0;
  state.levels.length = 0;
  state.nullCount = 0;
  state.byteArraySize = 0;
}

/** Renders an offending value for an error message, without ever stringifying an object. */
function describe(value: unknown): string {
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

function invalid(column: SchemaColumn, value: unknown, expected: string): TavolatoError {
  return new TavolatoError(
    `Column "${column.name}" of type ${column.type} expects ${expected}, received ${describe(
      value,
    )}`,
    "ERR_ROW_VALUE_INVALID",
    column.name,
  );
}

function toInt64(column: SchemaColumn, value: unknown): bigint {
  let result: bigint;
  if (typeof value === "bigint") {
    result = value;
  } else if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw invalid(column, value, "a bigint or a safe integer number");
    }
    result = BigInt(value);
  } else {
    throw invalid(column, value, "a bigint or a safe integer number");
  }
  if (result < INT64_MIN || result > INT64_MAX) {
    throw invalid(column, value, "a value within the signed 64-bit range");
  }
  return result;
}

function toEpochMillis(column: SchemaColumn, value: unknown): bigint {
  if (value instanceof Date) {
    const millis = value.getTime();
    if (Number.isNaN(millis)) throw invalid(column, value, "a valid Date");
    return BigInt(millis);
  }
  if (typeof value === "number") {
    if (!Number.isSafeInteger(value)) {
      throw invalid(column, value, "a Date or integer epoch milliseconds");
    }
    return BigInt(value);
  }
  throw invalid(column, value, "a Date or integer epoch milliseconds");
}

function stage(column: SchemaColumn, value: unknown, present: boolean): StagedValue {
  if (value === null || value === undefined) {
    if (!column.optional) {
      throw new TavolatoError(
        `Column "${column.name}" is required but the row ${
          present ? "sets it to null or undefined" : "does not provide it"
        }`,
        "ERR_ROW_VALUE_MISSING",
        column.name,
      );
    }
    return null;
  }
  switch (column.type) {
    case "string": {
      if (typeof value !== "string") throw invalid(column, value, "a string");
      return { kind: "bytes", value: utf8.encode(value) };
    }
    case "f64": {
      if (typeof value !== "number") throw invalid(column, value, "a number");
      return { kind: "f64", value };
    }
    case "bool": {
      if (typeof value !== "boolean") throw invalid(column, value, "a boolean");
      return { kind: "bool", value };
    }
    case "i64": {
      return { kind: "i64", value: toInt64(column, value) };
    }
    default: {
      return { kind: "i64", value: toEpochMillis(column, value) };
    }
  }
}

function commit(state: ColumnState, staged: StagedValue): void {
  if (staged === null) {
    state.levels.push(0);
    state.nullCount++;
    return;
  }
  if (state.column.optional) state.levels.push(1);
  if (staged.kind === "bytes") state.byteArraySize += 4 + staged.value.length;
  // `staged.kind` is derived from `state.column.type`, which also decided the
  // buffer's kind, so the two always agree. TypeScript cannot express that
  // correlation across two independent unions, hence the single cast.
  (state.values.items as unknown[]).push(staged.value);
}

/**
 * Upper bound, in bytes, of the page body (level stream plus PLAIN values) the
 * column chunk would need if `staged` were committed on top of what is already
 * buffered. Values are counted exactly; the level stream is bounded by its
 * worst case, an all-bit-packed run set (one byte per group of eight levels
 * plus one header byte per 63 groups).
 */
function projectedPageSize(state: ColumnState, staged: StagedValue): number {
  const { values } = state;
  const valueCount = values.items.length + (staged === null ? 0 : 1);
  let bytes: number;
  switch (values.kind) {
    case "bytes": {
      bytes =
        state.byteArraySize +
        (staged !== null && staged.kind === "bytes" ? 4 + staged.value.length : 0);
      break;
    }
    case "bool": {
      bytes = Math.ceil(valueCount / 8);
      break;
    }
    default: {
      bytes = valueCount * 8;
    }
  }
  if (state.column.optional) {
    const levelCount = state.levels.length + 1;
    bytes += 4 + Math.ceil(levelCount / 8) + Math.ceil(levelCount / 504) + 1;
  }
  return bytes;
}

/**
 * Buffers rows and emits a complete Parquet file.
 *
 * Rows accumulate in memory until `rowGroupSize` is reached, at which point a
 * row group is serialized into the output buffer and the column buffers are
 * released. `finish()` flushes whatever is left, appends the footer, and
 * returns the file; the writer is unusable afterwards.
 *
 * Instances are created through {@link createWriter}.
 */
export class ParquetWriter<TDefinition extends SchemaDefinition = SchemaDefinition> {
  /** The schema this writer was built from. */
  readonly schema: ParquetSchema<TDefinition>;

  readonly #states: readonly ColumnState[];
  readonly #staged: StagedValue[];
  readonly #names: ReadonlySet<string>;
  readonly #rowGroupSize: number;
  readonly #createdBy: string;

  #out: ByteWriter;
  #rowGroups: RowGroupMeta[] = [];
  #bufferedRows = 0;
  #rowCount = 0;
  #finished = false;

  constructor(schema: ParquetSchema<TDefinition>, options: WriterOptions = {}) {
    const rowGroupSize = options.rowGroupSize ?? DEFAULT_ROW_GROUP_SIZE;
    if (!Number.isSafeInteger(rowGroupSize) || rowGroupSize < 1) {
      throw new TavolatoError(
        `rowGroupSize must be a positive integer, received ${describe(options.rowGroupSize)}`,
        "ERR_WRITER_OPTION_INVALID",
      );
    }
    const createdBy = options.createdBy ?? DEFAULT_CREATED_BY;
    if (typeof createdBy !== "string") {
      throw new TavolatoError(
        `createdBy must be a string, received ${describe(options.createdBy)}`,
        "ERR_WRITER_OPTION_INVALID",
      );
    }

    this.schema = schema;
    this.#rowGroupSize = Math.min(rowGroupSize, MAX_ROWS_PER_GROUP);
    this.#createdBy = createdBy;
    this.#states = schema.columns.map((column) => createColumnState(column));
    this.#staged = Array.from<StagedValue>({ length: schema.columns.length }).fill(null);
    this.#names = new Set(schema.columns.map((column) => column.name));
    this.#out = new ByteWriter(4096);
    this.#out.raw(MAGIC);
  }

  /** Number of rows appended so far. */
  get rowCount(): number {
    return this.#rowCount;
  }

  /** Whether `finish()` has already been called. */
  get finished(): boolean {
    return this.#finished;
  }

  /**
   * Validates a row against the schema and buffers it.
   *
   * The row is validated in full before anything is buffered, so a rejected
   * row leaves the writer exactly as it was.
   *
   * @throws {TavolatoError} `ERR_WRITER_FINISHED`, `ERR_ROW_NOT_AN_OBJECT`,
   * `ERR_ROW_UNKNOWN_COLUMN`, `ERR_ROW_VALUE_MISSING` or `ERR_ROW_VALUE_INVALID`.
   */
  append(row: Row<TDefinition>): void {
    if (this.#finished) {
      throw new TavolatoError("Writer has already been finished", "ERR_WRITER_FINISHED");
    }
    if (typeof row !== "object" || row === null || Array.isArray(row)) {
      throw new TavolatoError(
        `A row must be a plain object, received ${describe(row)}`,
        "ERR_ROW_NOT_AN_OBJECT",
      );
    }

    const record = row as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      if (!this.#names.has(key)) {
        throw new TavolatoError(
          `Row has column "${key}" which is not part of the schema`,
          "ERR_ROW_UNKNOWN_COLUMN",
          key,
        );
      }
    }

    // Stage first, commit second: a throw halfway through validation must not
    // leave the column buffers holding a partial row.
    const staged = this.#staged;
    for (let index = 0; index < this.#states.length; index++) {
      const column = this.#states[index].column;
      staged[index] = stage(column, record[column.name], Object.hasOwn(record, column.name));
    }

    // Page sizes are i32 fields, so no column chunk may grow past
    // `MAX_PAGE_BYTES`. A row that would push a chunk over the boundary closes
    // the buffered row group first and starts a fresh one; a single value that
    // cannot fit a page even on its own is refused outright (the 16 bytes of
    // headroom cover the value's length prefix and a one-row level stream).
    let mustFlush = false;
    for (let index = 0; index < this.#states.length; index++) {
      const state = this.#states[index];
      const value = staged[index];
      if (value !== null && value.kind === "bytes" && value.value.length + 16 > MAX_PAGE_BYTES) {
        throw new TavolatoError(
          `Column "${state.column.name}" received a ${value.value.length} byte value, which cannot fit a Parquet data page`,
          "ERR_ROW_VALUE_INVALID",
          state.column.name,
        );
      }
      if (projectedPageSize(state, value) > MAX_PAGE_BYTES) mustFlush = true;
    }
    if (mustFlush) this.#flushRowGroup();

    for (let index = 0; index < this.#states.length; index++) {
      commit(this.#states[index], staged[index]);
    }

    this.#bufferedRows++;
    this.#rowCount++;
    if (this.#bufferedRows >= this.#rowGroupSize) this.#flushRowGroup();
  }

  /** Appends every row of an iterable, in order. */
  appendAll(rows: Iterable<Row<TDefinition>>): void {
    for (const row of rows) this.append(row);
  }

  /**
   * Flushes the pending row group, appends the footer, and returns the finished
   * file. The writer releases its buffers and refuses further use.
   *
   * A writer that never saw a row still produces a valid file: schema present,
   * zero row groups, `num_rows = 0`.
   *
   * @throws {TavolatoError} `ERR_WRITER_FINISHED` when called twice.
   */
  finish(): Uint8Array {
    if (this.#finished) {
      throw new TavolatoError("Writer has already been finished", "ERR_WRITER_FINISHED");
    }
    this.#flushRowGroup();

    const footer = encodeFileMetadata(
      this.schema.columns,
      this.#rowGroups,
      this.#rowCount,
      this.#createdBy,
    );
    this.#out.raw(footer);
    this.#out.u32(footer.length);
    this.#out.raw(MAGIC);

    const bytes = this.#out.toBytes();
    this.#finished = true;
    this.#out = new ByteWriter(16);
    this.#rowGroups = [];
    for (const state of this.#states) resetColumnState(state);
    return bytes;
  }

  #flushRowGroup(): void {
    if (this.#bufferedRows === 0) return;
    const numRows = this.#bufferedRows;
    const fileOffset = this.#out.length;
    const columns: ColumnChunkMeta[] = [];
    let totalByteSize = 0;

    for (const state of this.#states) {
      const dataPageOffset = this.#out.length;
      const page = new ByteWriter(1024);
      if (state.column.optional) {
        // Data page v1 prefixes RLE level data with its length as 4 bytes LE.
        const levels = encodeRleBitPackedHybrid(state.levels, MAX_DEFINITION_LEVEL_BIT_WIDTH);
        page.u32(levels.length);
        page.raw(levels);
      }
      writePlain(page, state.values);
      const body = page.toBytes();
      const header = encodeDataPageHeader(body.length, numRows);

      this.#out.raw(header);
      this.#out.raw(body);

      const totalSize = header.length + body.length;
      totalByteSize += totalSize;
      columns.push({
        name: state.column.name,
        type: state.column.type,
        optional: state.column.optional,
        numValues: numRows,
        nullCount: state.nullCount,
        dataPageOffset,
        totalSize,
      });
      resetColumnState(state);
    }

    this.#rowGroups.push({ columns, numRows, totalByteSize, fileOffset });
    this.#bufferedRows = 0;
  }
}

/**
 * Creates a writer for a schema produced by `defineSchema`.
 *
 * @example
 * const schema = defineSchema({ at: { type: "timestamp" }, n: { type: "i64" } });
 * const writer = createWriter(schema, { rowGroupSize: 50_000 });
 * writer.append({ at: Date.now(), n: 42n });
 * const bytes = writer.finish();
 */
export function createWriter<TDefinition extends SchemaDefinition>(
  schema: ParquetSchema<TDefinition>,
  options?: WriterOptions,
): ParquetWriter<TDefinition> {
  return new ParquetWriter(schema, options);
}
