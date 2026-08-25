import { ByteWriter, utf8 } from "./internal/bytes.ts";
import { chain, chainEach, isThenable } from "./internal/chain.ts";
import { type ColumnValues, encodeRleBitPackedHybrid, writePlain } from "./internal/encoding.ts";
import {
  CODEC_IDS,
  codecId,
  type ColumnChunkMeta,
  type ColumnSnapshot,
  columnTypeName,
  CompressionCodec,
  encodeDataPageHeader,
  encodeFileMetadata,
  MAGIC,
  MAX_DEFINITION_LEVEL_BIT_WIDTH,
  type RowGroupMeta,
} from "./internal/format.ts";
import { jsonTextOf } from "./adapters.ts";
import { describe, TavolatoError } from "./error.ts";
import { validateParquetSchema } from "./schema.ts";
import type {
  AnyLogicalAdapter,
  ParquetSchema,
  Row,
  SchemaColumn,
  SchemaDefinition,
  WriterCodec,
  WriterOptions,
} from "./types.ts";

const DEFAULT_ROW_GROUP_SIZE = 10_000;
const DEFAULT_CREATED_BY = "tavolato";

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const INT32_MIN = -(2 ** 31);
const INT32_MAX = 2 ** 31 - 1;

/** The instant furthest from the epoch a `Date` can hold, in milliseconds. */
const MAX_DATE_MILLIS = 8_640_000_000_000_000;

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
  | { readonly kind: "fixed"; readonly value: Uint8Array }
  | { readonly kind: "f64"; readonly value: number }
  | { readonly kind: "f32"; readonly value: number }
  | { readonly kind: "i64"; readonly value: bigint }
  | { readonly kind: "i32"; readonly value: number }
  | { readonly kind: "bool"; readonly value: boolean }
  | null;

interface ColumnState {
  readonly column: SchemaColumn;
  /**
   * What the file will say this column is, read off the column type once. The
   * buffer below is shaped from it and the footer is written from it, so the
   * two cannot come to disagree.
   */
  readonly snapshot: ColumnSnapshot;
  readonly values: ColumnValues;
  /** Definition levels for the current row group; unused for required columns. */
  readonly levels: number[];
  nullCount: number;
  /** PLAIN-encoded size of the buffered BYTE_ARRAY values; other kinds derive theirs from the item count. */
  byteArraySize: number;
}

function createColumnState(column: SchemaColumn, snapshot: ColumnSnapshot): ColumnState {
  const { physical } = snapshot;
  const values: ColumnValues =
    physical === "fixed"
      ? { kind: physical, typeLength: snapshot.typeLength ?? 0, items: [] }
      : { kind: physical, items: [] };
  return { column, snapshot, values, levels: [], nullCount: 0, byteArraySize: 0 };
}

function resetColumnState(state: ColumnState): void {
  state.values.items.length = 0;
  state.levels.length = 0;
  state.nullCount = 0;
  state.byteArraySize = 0;
}

function invalid(column: SchemaColumn, value: unknown, expected: string): TavolatoError {
  return new TavolatoError(
    `Column "${column.name}" of type ${columnTypeName(column.type)} expects ${expected}, received ${describe(
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
    // A `timestamp` column is read back as a `Date`, and a `Date` runs out
    // long before an INT64 does. A count past that boundary could only ever be
    // read as an Invalid Date, so it is refused here rather than stored.
    if (value < -MAX_DATE_MILLIS || value > MAX_DATE_MILLIS) {
      throw invalid(column, value, "epoch milliseconds within the range a Date can represent");
    }
    return BigInt(value);
  }
  throw invalid(column, value, "a Date or integer epoch milliseconds");
}

function toInt32(column: SchemaColumn, value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw invalid(column, value, "an integer number");
  }
  if (value < INT32_MIN || value > INT32_MAX) {
    throw invalid(column, value, "a value within the signed 32-bit range");
  }
  return value;
}

function stage(state: ColumnState, value: unknown, present: boolean): StagedValue {
  const { column } = state;
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
  const { type } = column;
  // An adapter column runs the caller's transform first, then holds what comes
  // back to the physical type it promised. Nulls never get this far.
  if (typeof type !== "string") return stageRaw(state, type, adapt(column, type, value));
  switch (type) {
    case "string": {
      if (typeof value !== "string") throw invalid(column, value, "a string");
      return { kind: "bytes", value: utf8.encode(value) };
    }
    case "json": {
      // A `json` column holds the *structure*, and the stored form is the JSON
      // string that Parquet's JSON annotation describes. Which means the
      // semantics of the round trip are `JSON.stringify`'s and `JSON.parse`'s
      // rather than tavolato's — `jsonTextOf` is where that is written down.
      //
      // A top-level `null` never reaches here: it was handled above, with every
      // other column type's, as the column being null.
      return { kind: "bytes", value: utf8.encode(jsonTextOf(value, column.name)) };
    }
    case "f64": {
      if (typeof value !== "number") throw invalid(column, value, "a number");
      return { kind: "f64", value };
    }
    case "f32": {
      // Stored at single precision, so this is the one built-in type whose
      // value is rounded on the way in — once, here, and never again.
      if (typeof value !== "number") throw invalid(column, value, "a number");
      return { kind: "f32", value };
    }
    case "bool": {
      if (typeof value !== "boolean") throw invalid(column, value, "a boolean");
      return { kind: "bool", value };
    }
    case "i64": {
      return { kind: "i64", value: toInt64(column, value) };
    }
    case "i32": {
      return { kind: "i32", value: toInt32(column, value) };
    }
    default: {
      return { kind: "i64", value: toEpochMillis(column, value) };
    }
  }
}

/** Runs an adapter's `write`, turning anything it throws into a typed error. */
function adapt(column: SchemaColumn, adapter: AnyLogicalAdapter, value: unknown): unknown {
  try {
    return adapter.write(value);
  } catch (cause) {
    throw new TavolatoError(
      `Column "${column.name}" of type ${adapter.name} rejected a value: ${
        cause instanceof Error ? cause.message : describe(cause)
      }`,
      "ERR_ROW_VALUE_INVALID",
      column.name,
      cause,
    );
  }
}

/**
 * Checks that what an adapter handed back is the physical value it said it
 * would produce, and stages it.
 *
 * The same courtesy the codec hooks get: a hook is a caller's function, and
 * the type it promises is not the type it delivers. Writing an unchecked
 * `undefined` into a `DataView` would silently store a zero instead.
 *
 * The check is against the *buffer* the value is about to land in, resolved
 * once when the column state was built, rather than against what the adapter
 * says its physical type is now — the same reason the codec's id is resolved in
 * the constructor.
 */
function stageRaw(state: ColumnState, adapter: AnyLogicalAdapter, raw: unknown): StagedValue {
  const { column, values } = state;
  const returned = (expected: string): TavolatoError =>
    new TavolatoError(
      `Column "${column.name}" of type ${adapter.name} returned ${describe(raw)} where ${expected} was expected`,
      "ERR_ROW_VALUE_INVALID",
      column.name,
    );

  switch (values.kind) {
    case "bytes": {
      if (!(raw instanceof Uint8Array)) throw returned("bytes");
      return { kind: "bytes", value: raw };
    }
    case "fixed": {
      if (!(raw instanceof Uint8Array) || raw.length !== values.typeLength) {
        throw returned(`exactly ${values.typeLength} bytes`);
      }
      return { kind: "fixed", value: raw };
    }
    case "bool": {
      if (typeof raw !== "boolean") throw returned("a boolean");
      return { kind: "bool", value: raw };
    }
    case "f64":
    case "f32": {
      if (typeof raw !== "number") throw returned("a number");
      return { kind: values.kind, value: raw };
    }
    case "i32": {
      if (typeof raw !== "number" || !Number.isInteger(raw) || raw < INT32_MIN || raw > INT32_MAX) {
        throw returned("a signed 32-bit integer");
      }
      return { kind: "i32", value: raw };
    }
    default: {
      if (typeof raw !== "bigint" || raw < INT64_MIN || raw > INT64_MAX) {
        throw returned("a signed 64-bit integer");
      }
      return { kind: "i64", value: raw };
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
    case "fixed": {
      bytes = valueCount * values.typeLength;
      break;
    }
    case "bool": {
      bytes = Math.ceil(valueCount / 8);
      break;
    }
    case "i32":
    case "f32": {
      bytes = valueCount * 4;
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
 * One column's page body, detached from the column buffer and waiting to be
 * compressed and written.
 */
interface PendingPage {
  readonly column: ColumnSnapshot;
  readonly body: Uint8Array;
  readonly nullCount: number;
}

/**
 * Buffers rows and emits a complete Parquet file.
 *
 * Rows accumulate in memory until `rowGroupSize` is reached, at which point a
 * row group is serialized into the output buffer and the column buffers are
 * released. `finish()` flushes whatever is left, appends the footer, and
 * returns the file; the writer is unusable afterwards.
 *
 * With a `codec`, `append` and `finish` return a promise exactly when the codec
 * hands them one — never otherwise. See {@link ParquetWriter.append} for the
 * one rule that comes with it.
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
  readonly #codec: WriterCodec | undefined;
  readonly #codecId: number;

  #out: ByteWriter;
  #rowGroups: RowGroupMeta[] = [];
  #bufferedRows = 0;
  #rowCount = 0;
  #finished = false;
  /** Set for the whole of an in-progress `append` / `finish`; see `#guard`. */
  #busy = false;
  /** Set once a codec failure has left the output mid-row-group and unrecoverable. */
  #failure: TavolatoError | undefined;

  constructor(schema: ParquetSchema<TDefinition>, options: WriterOptions = {}) {
    const columns = validateParquetSchema(schema);
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
    const codec = options.codec;
    let codecStamp: number = CompressionCodec.UNCOMPRESSED;
    if (codec !== undefined) {
      if (typeof codec !== "object" || codec === null || typeof codec.compress !== "function") {
        throw new TavolatoError(
          `codec must be an object such as { name: "GZIP", compress }, received ${describe(
            options.codec,
          )}`,
          "ERR_WRITER_OPTION_INVALID",
        );
      }
      // Resolved once, in the constructor: a name that stands for no codec
      // would otherwise only surface as a nonsense id in the finished file.
      const id = typeof codec.name === "string" ? codecId(codec.name) : undefined;
      if (id === undefined) {
        throw new TavolatoError(
          `codec.name must be one of ${Object.keys(CODEC_IDS).join(", ")}, received ${describe(
            codec.name,
          )}`,
          "ERR_WRITER_OPTION_INVALID",
        );
      }
      codecStamp = id;
    }

    this.schema = schema;
    this.#rowGroupSize = Math.min(rowGroupSize, MAX_ROWS_PER_GROUP);
    this.#createdBy = createdBy;
    this.#codec = codec;
    this.#codecId = codecStamp;
    this.#states = columns.map(({ column, snapshot }) => createColumnState(column, snapshot));
    this.#staged = Array.from<StagedValue>({ length: columns.length }).fill(null);
    this.#names = new Set(columns.map(({ column }) => column.name));
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
   * Returns nothing on the ordinary path. It returns a **promise** only when
   * appending this row closed a row group *and* the configured codec compressed
   * it asynchronously; that promise must be awaited before the writer is
   * touched again, or the next call throws `ERR_WRITER_BUSY`. Without a codec,
   * or with a synchronous one, nothing here ever defers.
   *
   * @throws {TavolatoError} `ERR_WRITER_FINISHED`, `ERR_WRITER_BUSY`,
   * `ERR_WRITER_CODEC_FAILED`, `ERR_ROW_NOT_AN_OBJECT`,
   * `ERR_ROW_UNKNOWN_COLUMN`, `ERR_ROW_VALUE_MISSING` or `ERR_ROW_VALUE_INVALID`.
   */
  append(row: Row<TDefinition>): void | Promise<void> {
    return this.#guard(() => this.#appendRow(row));
  }

  #appendRow(row: Row<TDefinition>): void | Promise<void> {
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
      const state = this.#states[index];
      const { name } = state.column;
      // Only read the key the row actually has. `record.__proto__` on a row
      // that carries no such column is `Object.prototype` rather than
      // `undefined` — a column a caller left out would otherwise arrive as a
      // value, and be refused for being the wrong type instead of for being
      // missing. `Object.hasOwn` is the question that has always been asked
      // here; this only stops the answer being ignored.
      const present = Object.hasOwn(record, name);
      staged[index] = stage(state, present ? record[name] : undefined, present);
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

    // The flush below detaches the buffered rows synchronously even when the
    // codec defers, so committing this row on top of the freshly emptied
    // buffers is safe while the previous group is still being compressed.
    let pending = mustFlush ? this.#flushRowGroup() : undefined;

    for (let index = 0; index < this.#states.length; index++) {
      commit(this.#states[index], staged[index]);
    }

    this.#bufferedRows++;
    this.#rowCount++;
    if (this.#bufferedRows >= this.#rowGroupSize) {
      // Row groups are written in order, so a second flush in the same call
      // (only possible at `rowGroupSize: 1`) waits for the first.
      pending = chain(pending, () => this.#flushRowGroup());
    }
    return pending;
  }

  /**
   * Appends every row of an iterable, in order.
   *
   * Returns a promise under the same rule as {@link ParquetWriter.append}: only
   * when a codec deferred, and from then on the remaining rows are appended in
   * its continuation. The iterable is pulled lazily either way.
   *
   * The re-entry guard is **per row**, by design: an append that interleaves
   * with an unawaited one lands whole or draws `ERR_WRITER_BUSY`, never half a
   * row and never a torn file.
   */
  appendAll(rows: Iterable<Row<TDefinition>>): void | Promise<void> {
    const iterator = rows[Symbol.iterator]();
    let closed = false;
    const fail = (failure: unknown): never => {
      if (!closed) {
        closed = true;
        try {
          iterator.return?.();
        } catch {
          // IteratorClose preserves an existing throw completion over return()'s.
        }
      }
      throw failure;
    };
    const run = (): void | Promise<void> => {
      for (let next = iterator.next(); next.done !== true; next = iterator.next()) {
        const row = next.value;
        try {
          const pending = this.append(row);
          if (isThenable(pending)) return Promise.resolve(pending).then(run, fail);
        } catch (failure) {
          return fail(failure);
        }
      }
    };
    return run();
  }

  /**
   * Flushes the pending row group, appends the footer, and returns the finished
   * file. The writer releases its buffers and refuses further use.
   *
   * A writer that never saw a row still produces a valid file: schema present,
   * zero row groups, `num_rows = 0`.
   *
   * Returns a promise only when the codec compresses the final row group
   * asynchronously.
   *
   * @throws {TavolatoError} `ERR_WRITER_FINISHED` when called twice,
   * `ERR_WRITER_BUSY` while an earlier call is still in flight.
   */
  finish(): Uint8Array | Promise<Uint8Array> {
    // The footer is written inside the guard, not after it: a writer that has
    // flushed its last page but not yet stamped `num_rows` is still torn, and
    // `#complete` must be unable to run twice.
    return this.#guard(() => chain(this.#flushRowGroup(), () => this.#complete()));
  }

  /**
   * Runs one public operation with the writer held against re-entry for the
   * operation's *whole* lifetime — the synchronous stretch on the stack
   * included, so a codec that calls back in is refused rather than allowed to
   * rewrite offsets that have already been handed out.
   *
   * Nothing is marked before {@link ParquetWriter.append} has decided the call
   * is legal, so a refused call leaves the writer exactly as it found it; and
   * the mark is only cleared once the operation has fully settled, which is
   * what closes the gap between a last page landing and a footer being written.
   */
  #guard<T>(run: () => T | Promise<T>): T | Promise<T> {
    this.#assertUsable();
    this.#busy = true;
    let pending: T | Promise<T>;
    try {
      pending = run();
    } catch (error) {
      this.#busy = false;
      throw error;
    }
    if (!isThenable(pending)) {
      this.#busy = false;
      return pending;
    }
    return pending.then(
      (value) => {
        this.#busy = false;
        return value;
      },
      (error: unknown) => {
        this.#busy = false;
        throw error;
      },
    );
  }

  /** Refuses a writer that is finished, mid-operation, or wrecked by a codec. */
  #assertUsable(): void {
    if (this.#finished) {
      throw new TavolatoError("Writer has already been finished", "ERR_WRITER_FINISHED");
    }
    if (this.#failure !== undefined) throw this.#failure;
    if (this.#busy) {
      throw new TavolatoError(
        "The writer is in the middle of an operation: await the promise the previous call returned, and do not call back into the writer from a codec",
        "ERR_WRITER_BUSY",
      );
    }
  }

  /** Writes the footer and hands over the file. */
  #complete(): Uint8Array {
    const footer = encodeFileMetadata(
      this.#states.map((state) => state.snapshot),
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

  #flushRowGroup(): void | Promise<void> {
    if (this.#bufferedRows === 0) return;
    const numRows = this.#bufferedRows;
    this.#bufferedRows = 0;

    // Every page body is built, and every column buffer released, before a
    // single byte is compressed: whatever the codec does with its time, it
    // holds no rows hostage.
    const pages: PendingPage[] = [];
    for (const state of this.#states) {
      const page = new ByteWriter(1024);
      if (state.column.optional) {
        // Data page v1 prefixes RLE level data with its length as 4 bytes LE.
        const levels = encodeRleBitPackedHybrid(state.levels, MAX_DEFINITION_LEVEL_BIT_WIDTH);
        page.u32(levels.length);
        page.raw(levels);
      }
      writePlain(page, state.values);
      pages.push({ column: state.snapshot, body: page.toBytes(), nullCount: state.nullCount });
      resetColumnState(state);
    }

    const fileOffset = this.#out.length;
    const columns: ColumnChunkMeta[] = [];
    let totalByteSize = 0;
    let totalCompressedSize = 0;

    // Compressed one page at a time, in order: a chunk's offset is only known
    // once the chunk before it has landed, which is why there is no second pass
    // and no offset to patch afterwards.
    return chain(
      chainEach(pages.length, (index) => {
        const page = pages[index];
        const dataPageOffset = this.#out.length;
        return chain(this.#compress(page.body), (body) => {
          const header = encodeDataPageHeader(page.body.length, body.length, numRows);
          this.#out.raw(header);
          this.#out.raw(body);

          totalByteSize += header.length + page.body.length;
          totalCompressedSize += header.length + body.length;
          columns.push({
            name: page.column.name,
            physical: page.column.physical,
            optional: page.column.optional,
            codec: this.#codecId,
            numValues: numRows,
            nullCount: page.nullCount,
            dataPageOffset,
            totalUncompressedSize: header.length + page.body.length,
            totalCompressedSize: header.length + body.length,
          });
        });
      }),
      () => {
        this.#rowGroups.push({
          columns,
          numRows,
          totalByteSize,
          totalCompressedSize,
          fileOffset,
        });
      },
    );
  }

  /** Hands one page body to the codec, or returns it untouched when there is none. */
  #compress(body: Uint8Array): Uint8Array | PromiseLike<Uint8Array> {
    const codec = this.#codec;
    if (codec === undefined) return body;

    let compressed: Uint8Array | Promise<Uint8Array>;
    try {
      compressed = codec.compress(body);
    } catch (cause) {
      throw this.#codecFailed(`The ${codec.name} codec threw while compressing a page`, cause);
    }
    if (!isThenable(compressed)) return this.#accept(codec.name, compressed);

    // `chain` only forwards fulfilment, so a rejection is turned into a typed
    // error here, at the one place that knows which codec was asked.
    const settled = compressed.then(
      (result) => this.#accept(codec.name, result),
      (cause: unknown) => {
        throw this.#codecFailed(`The ${codec.name} codec rejected while compressing a page`, cause);
      },
    );
    // A thenable is only a thenable by duck typing. One whose `then` returns
    // nothing to chain on — the callback style that predates promises — would
    // otherwise smuggle that `undefined` onward in place of the page bytes.
    if (!isThenable(settled)) {
      throw this.#codecFailed(
        `The ${codec.name} codec returned a thenable whose then() produced ${describe(
          settled,
        )} rather than a promise`,
      );
    }
    return settled;
  }

  /**
   * Checks that what the codec handed back can actually be written as a page.
   * `unknown` rather than `Uint8Array`: the hook is a caller's function, and the
   * type it promises is not the type it delivers.
   */
  #accept(name: string, result: unknown): Uint8Array {
    if (!(result instanceof Uint8Array)) {
      throw this.#codecFailed(
        `The ${name} codec returned ${describe(result)} instead of a Uint8Array`,
      );
    }
    if (result.length > MAX_PAGE_BYTES) {
      throw this.#codecFailed(
        `The ${name} codec returned a ${result.length} byte page, which cannot fit a Parquet data page`,
      );
    }
    return result;
  }

  /**
   * Records a codec failure and returns the error to throw. The rows of the
   * row group being flushed are already gone and the output sits mid-group, so
   * the writer is poisoned rather than left to produce a file missing rows it
   * accepted.
   */
  #codecFailed(what: string, cause?: unknown): TavolatoError {
    const error = new TavolatoError(
      `${what}; the writer cannot recover and is now unusable`,
      "ERR_WRITER_CODEC_FAILED",
      undefined,
      cause,
    );
    this.#failure = error;
    return error;
  }
}

/**
 * A {@link ParquetWriter} built without a codec, and typed for it.
 *
 * Only a codec can make this writer defer, so one that was never given a codec
 * never does — and its callers should not have to pretend otherwise. This is
 * the same surface, with the maybe-promises resolved to what they can actually
 * be. `createWriter` hands it back whenever no `codec` option is passed.
 */
export interface SyncParquetWriter<TDefinition extends SchemaDefinition = SchemaDefinition> {
  /** The schema this writer was built from. */
  readonly schema: ParquetSchema<TDefinition>;
  /** Number of rows appended so far. */
  readonly rowCount: number;
  /** Whether `finish()` has already been called. */
  readonly finished: boolean;
  /** See {@link ParquetWriter.append}. */
  append(row: Row<TDefinition>): void;
  /** See {@link ParquetWriter.appendAll}. */
  appendAll(rows: Iterable<Row<TDefinition>>): void;
  /** See {@link ParquetWriter.finish}. */
  finish(): Uint8Array;
}

/**
 * Creates a writer for a schema produced by `defineSchema`.
 *
 * Without a `codec` the result is a {@link SyncParquetWriter}, whose `append`
 * and `finish` return outright; with one it is a {@link ParquetWriter}, whose
 * do so only when the codec itself is synchronous.
 *
 * @example
 * const schema = defineSchema({ at: { type: "timestamp" }, n: { type: "i64" } });
 * const writer = createWriter(schema, { rowGroupSize: 50_000 });
 * writer.append({ at: Date.now(), n: 42n });
 * const bytes = writer.finish();
 *
 * @example
 * // Compressed, with whatever the runtime already has:
 * import { gzipSync } from "node:zlib";
 * const writer = createWriter(schema, { codec: { name: "GZIP", compress: gzipSync } });
 *
 * @throws {TavolatoError} `ERR_SCHEMA_EMPTY` or
 * `ERR_SCHEMA_COLUMN_INVALID` when a structurally supplied schema is malformed.
 */
export function createWriter<TDefinition extends SchemaDefinition>(
  schema: ParquetSchema<TDefinition>,
  options?: WriterOptions & { codec?: undefined },
): SyncParquetWriter<TDefinition>;
export function createWriter<TDefinition extends SchemaDefinition>(
  schema: ParquetSchema<TDefinition>,
  options: WriterOptions,
): ParquetWriter<TDefinition>;
export function createWriter<TDefinition extends SchemaDefinition>(
  schema: ParquetSchema<TDefinition>,
  options?: WriterOptions,
): SyncParquetWriter<TDefinition> | ParquetWriter<TDefinition> {
  return new ParquetWriter(schema, options);
}
