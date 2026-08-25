import type { JsonNull } from "./json-null.ts";

/**
 * The column types tavolato writes.
 *
 * | tavolato    | Parquet physical | Parquet logical                               |
 * | ----------- | ---------------- | --------------------------------------------- |
 * | `string`    | `BYTE_ARRAY`     | `STRING` (`UTF8`)                             |
 * | `json`      | `BYTE_ARRAY`     | `JSON`                                        |
 * | `f64`       | `DOUBLE`         | —                                             |
 * | `f32`       | `FLOAT`          | —                                             |
 * | `i64`       | `INT64`          | —                                             |
 * | `i32`       | `INT32`          | —                                             |
 * | `bool`      | `BOOLEAN`        | —                                             |
 * | `timestamp` | `INT64`          | `TIMESTAMP(UTC, MILLIS)` (`TIMESTAMP_MILLIS`) |
 *
 * The *shape* is frozen — one flat level of scalar columns, forever — but the
 * list itself grows: a minor version may add a member, as `json` was added
 * next to `string`. Switch over it with a `default` arm rather than an
 * exhaustive one, or a new member turns into a type error on upgrade.
 *
 * These are the types that own a *bare* physical type, the one a file carries
 * with no annotation at all. Everything a column can additionally *mean* —
 * a date, a decimal, a UUID — is an annotation, and annotations are claimed by
 * a {@link LogicalAdapter} rather than by a member of this union.
 */
export type ColumnType = "string" | "json" | "f64" | "f32" | "i64" | "i32" | "bool" | "timestamp";

/**
 * Anything `JSON.stringify` produces and `JSON.parse` gives back: the value a
 * `json` column holds on both sides.
 *
 * A `json` column is stored as a JSON *string* in a `BYTE_ARRAY` annotated
 * `JSON` — that is the wire format and it does not move — but the JavaScript
 * value is the structure, serialized on the way in and parsed on the way out.
 *
 * `undefined` is deliberately absent: it is not a JSON value. At the top level
 * of a column it means the column is null, which is what `optional` is for;
 * inside an object it simply vanishes, exactly as `JSON.stringify` drops it.
 */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

/**
 * A complete document accepted by a value-mode `json` column.
 *
 * Nested `null` remains an ordinary JSON value. At the top level, JavaScript
 * `null` means a Parquet null, so the JSON document literal uses
 * {@link JsonNull} instead.
 */
export type JsonDocument = Exclude<JsonValue, null> | JsonNull;

/*
 * ---------------------------------------------------------------------------
 * Annotations
 *
 * Parquet stores a column twice over: as a *physical* type, which says how the
 * bytes are laid out, and as an annotation, which says what they mean. The
 * annotation comes in two spellings — the modern `LogicalType` union and the
 * deprecated `ConvertedType` enum — and tavolato reads both into the one model
 * below.
 *
 * That model mirrors `parquet.thrift` rather than tavolato's own scope: the
 * format froze these members, so decoding all of them costs nothing, and
 * refusing one *by name* is worth everything.
 * ---------------------------------------------------------------------------
 */

/** The resolutions Parquet's `TimeUnit` union names. */
export type TimeUnitName = "millis" | "micros" | "nanos";

/**
 * What a column's annotation says, once `LogicalType` and `ConvertedType` have
 * been reconciled (the modern spelling wins where a file carries both).
 *
 * Only a handful of these have a built-in column type. The rest are exactly
 * what a {@link LogicalAdapter} exists to claim — and `unknown` is the
 * forward-compatible escape hatch: an annotation a later Parquet release adds
 * decodes to its field id rather than throwing, and reaches `matches` like
 * any other.
 */
export type Annotation =
  | { readonly kind: "none" }
  | { readonly kind: "string" }
  | { readonly kind: "json" }
  | { readonly kind: "bson" }
  | { readonly kind: "enum" }
  | { readonly kind: "uuid" }
  | { readonly kind: "date" }
  | { readonly kind: "float16" }
  | { readonly kind: "decimal"; readonly precision: number; readonly scale: number }
  | {
      readonly kind: "time" | "timestamp";
      readonly unit: TimeUnitName;
      readonly isAdjustedToUTC: boolean;
    }
  | { readonly kind: "integer"; readonly bitWidth: 8 | 16 | 32 | 64; readonly isSigned: boolean }
  /** An annotation this version has no name for; `id` is its `LogicalType` field id. */
  | { readonly kind: "unknown"; readonly id: number };

/**
 * How a column's values are physically stored, and therefore which raw
 * JavaScript value a {@link LogicalAdapter} is handed and has to hand back.
 *
 * | kind    | Parquet physical       | raw value                             |
 * | ------- | ---------------------- | ------------------------------------- |
 * | `bool`  | `BOOLEAN`              | `boolean`                             |
 * | `i32`   | `INT32`                | `number`                              |
 * | `i64`   | `INT64`                | `bigint`                              |
 * | `f32`   | `FLOAT`                | `number`                              |
 * | `f64`   | `DOUBLE`               | `number`                              |
 * | `bytes` | `BYTE_ARRAY`           | `Uint8Array`, any length              |
 * | `fixed` | `FIXED_LEN_BYTE_ARRAY` | `Uint8Array` of exactly `typeLength`  |
 *
 * `INT96` is deliberately absent: the format deprecated it, so tavolato
 * refuses it outright rather than offering a hook for it.
 */
export type PhysicalKind = "bool" | "i32" | "i64" | "f32" | "f64" | "bytes" | "fixed";

/**
 * A logical column type you supply: the two functions that turn one physical
 * value into the JavaScript value you want, and back.
 *
 * An adapter is you resolving an ambiguity tavolato refuses to guess at. A
 * `DECIMAL(38, 4)` column is sixteen bytes of two's complement; whether those
 * should become a `string`, a `bigint` or somebody's arbitrary-precision object
 * is a question about *your* program, and answering it for you would be the
 * same overreach as silently reading an `INT96`. So tavolato names what it
 * found and stops — and an adapter is how you answer.
 *
 * Both halves are **synchronous**: an adapter is a pure value transform, and
 * the one place tavolato defers is the codec seam.
 *
 * Nulls never reach an adapter. `optional` columns are handled by the
 * definition-level machinery on both sides, so `read` and `write` only ever
 * see values that are present.
 *
 * Build one with {@link defineColumnType}, which validates the shape and
 * freezes it.
 */
export interface LogicalAdapter<TIn, TOut> {
  /** Used in error messages and wherever a schema is displayed. */
  readonly name: string;
  /** Which physical type this adapter writes. */
  readonly physical: PhysicalKind;
  /** Byte width this adapter writes, required for `"fixed"` and rejected otherwise. */
  readonly typeLength?: number;
  /**
   * Whether this adapter can read a physical layout other than the one it
   * writes. When absent, reads require `physical` and `typeLength` to match the
   * write layout exactly. Exact matches are accepted without calling this hook;
   * the file's annotation-to-physical contract is validated before alternatives
   * reach it.
   */
  acceptsPhysical?(physical: PhysicalKind, typeLength: number | undefined): boolean;
  /**
   * Whether this adapter claims a column carrying `annotation`. The physical
   * layout is accepted before this is called, so it only has the annotation
   * left to judge.
   */
  matches(annotation: Annotation, physical: PhysicalKind): boolean;
  /** The annotation to stamp on a column written through this adapter. */
  annotate(): Annotation;
  /** Turns one raw physical value into the value a caller reads. */
  read(raw: unknown): TOut;
  /** Turns one value a caller wrote into a raw physical value. */
  write(value: TIn): unknown;
}

/**
 * Any adapter, whatever it maps between.
 *
 * The two `any`s are confined to this one alias on purpose: a column
 * definition has to accept *some* adapter without knowing which, and the
 * alternative — a generic parameter threaded through `ParquetSchema`, `Row`,
 * `ParquetWriter` and every public signature that touches them — would be paid
 * for by every caller, adapters or not. Inference is recovered where it
 * matters, in {@link ColumnInput} and {@link ColumnOutput}, which read the
 * adapter's own type arguments back off the schema definition.
 */
// oxlint-disable-next-line no-explicit-any
export type AnyLogicalAdapter = LogicalAdapter<any, any>;

/** Declaration of a single column. */
export interface ColumnDefinition<
  TType extends ColumnType | AnyLogicalAdapter = ColumnType | AnyLogicalAdapter,
> {
  /** Column type: a built-in name, or an adapter built with `defineColumnType`. */
  type: TType;
  /** When `true` the column is nullable (Parquet `OPTIONAL`). Defaults to `false`. */
  optional?: boolean;
}

/** A flat map of column name to column declaration. */
export type SchemaDefinition = Record<string, ColumnDefinition>;

/** A normalized column, in the order it appears in the file. */
export interface SchemaColumn {
  readonly name: string;
  readonly type: ColumnType | AnyLogicalAdapter;
  readonly optional: boolean;
  /**
   * Byte width of a `FIXED_LEN_BYTE_ARRAY` column; absent for every other
   * physical type. On the way in it comes from the adapter, on the way out
   * from the file, and the two have to agree for the adapter to claim it.
   */
  readonly typeLength?: number;
}

/**
 * A validated schema. Produced by `defineSchema`, consumed by `createWriter`.
 * The `definition` property only carries the input type through for row-type
 * inference; it is not used at runtime.
 */
export interface ParquetSchema<TDefinition extends SchemaDefinition = SchemaDefinition> {
  readonly columns: readonly SchemaColumn[];
  readonly definition: TDefinition;
}

/** The JavaScript values accepted for a given column type. */
export type ColumnInput<TType extends ColumnType | AnyLogicalAdapter> =
  TType extends LogicalAdapter<infer TIn, unknown>
    ? TIn
    : TType extends "json"
      ? JsonDocument
      : TType extends "string"
        ? string
        : TType extends "f64" | "f32" | "i32"
          ? number
          : TType extends "i64"
            ? bigint | number
            : TType extends "bool"
              ? boolean
              : TType extends "timestamp"
                ? Date | number
                : never;

type OptionalColumns<TDefinition extends SchemaDefinition> = {
  [K in keyof TDefinition]: TDefinition[K] extends { optional: true } ? K : never;
}[keyof TDefinition];

type RequiredColumns<TDefinition extends SchemaDefinition> = Exclude<
  keyof TDefinition,
  OptionalColumns<TDefinition>
>;

/**
 * The row shape accepted by a writer built from `TDefinition`: required columns
 * are mandatory, optional ones may be omitted, `null` or `undefined`.
 */
export type Row<TDefinition extends SchemaDefinition> = {
  [K in RequiredColumns<TDefinition>]: ColumnInput<TDefinition[K]["type"]>;
} & {
  [K in OptionalColumns<TDefinition>]?: ColumnInput<TDefinition[K]["type"]> | null;
};

/**
 * The JavaScript values the reader produces for a given column type. The
 * mapping is the narrow half of {@link ColumnInput}: where the writer accepts
 * either of two inputs, the reader always returns the same one.
 *
 * | tavolato    | read back as |
 * | ----------- | ------------ |
 * | `string`    | `string`     |
 * | `json`      | `JsonDocument` |
 * | `f64`       | `number`     |
 * | `f32`       | `number`     |
 * | `i64`       | `bigint`     |
 * | `i32`       | `number`     |
 * | `bool`      | `boolean`    |
 * | `timestamp` | `Date`       |
 *
 * For an adapter column it is whatever that adapter's `read` returns.
 */
export type ColumnOutput<TType extends ColumnType | AnyLogicalAdapter> =
  TType extends LogicalAdapter<never, infer TOut>
    ? TOut
    : TType extends "json"
      ? JsonDocument
      : TType extends "string"
        ? string
        : TType extends "f64" | "f32" | "i32"
          ? number
          : TType extends "i64"
            ? bigint
            : TType extends "bool"
              ? boolean
              : TType extends "timestamp"
                ? Date
                : never;

/**
 * Any value the reader can produce for a **built-in** column type; `null` for a
 * null in an optional column.
 *
 * {@link JsonValue} carries the nested scalars, objects and arrays a `json`
 * column parses. {@link JsonNull} represents the top-level document literal;
 * ordinary `null` remains the absence of an optional Parquet value.
 *
 * `Uint8Array` is reserved for a future raw-binary column type and is listed
 * here so that adding one is not a breaking change to every `switch` over a
 * read value. Nothing returns it today — an unannotated `BYTE_ARRAY` or
 * `FIXED_LEN_BYTE_ARRAY` is refused rather than handed back as bytes.
 *
 * A column read through a {@link LogicalAdapter} yields whatever that adapter's
 * `read` returns, which the in-box adapters keep inside this union. One of your
 * own is free not to; {@link ReadRowOf} is where its real type is recovered.
 */
export type ReadValue = JsonValue | JsonNull | bigint | Date | Uint8Array;

/**
 * One row produced by the reader, keyed by column name in file order.
 *
 * With `ReadOptions.columns` the keys are the projected columns only, still in
 * the file's order rather than the order they were asked for.
 */
export type ReadRow = Record<string, ReadValue>;

/**
 * The read-side twin of {@link Row}: what a row of a file written from
 * `TDefinition` holds. `readParquet` cannot know a file's schema at compile
 * time, so this is the type to assert onto its rows when you do.
 *
 * Every column is present as a key; optional ones may be `null`.
 */
export type ReadRowOf<TDefinition extends SchemaDefinition> = {
  [K in RequiredColumns<TDefinition>]: ColumnOutput<TDefinition[K]["type"]>;
} & {
  [K in OptionalColumns<TDefinition>]: ColumnOutput<TDefinition[K]["type"]> | null;
};

/** What `readParquet` returns: the file's schema and every row it holds. */
export interface ParquetFile {
  /**
   * Derived from the file, in the same shape `defineSchema` produces — and
   * narrowed to `ReadOptions.columns` when a projection was asked for, so that
   * the schema always describes exactly the rows beside it.
   */
  readonly schema: ParquetSchema;
  /** Every row, in row group order and, within a group, in file order. */
  readonly rows: ReadRow[];
}

/**
 * What `readRowGroups` returns: the footer, read eagerly, and a lazy walk over
 * the file's row groups.
 *
 * The counts and the schema come from the footer and are there immediately. The
 * rows are not: iterating decodes **one row group per step**, which is what
 * keeps memory to the rows of a single group rather than the rows of the file.
 *
 * `TStep` is what a step yields — the rows array itself for a read that cannot
 * defer, and `ReadRow[] | Promise<ReadRow[]>` where a codec can. See
 * {@link SyncParquetRowGroups}.
 */
export interface ParquetRowGroups<TStep = ReadRow[] | Promise<ReadRow[]>> {
  /**
   * Derived from the file, in the same shape `defineSchema` produces, and
   * narrowed to `ReadOptions.columns` when a projection was asked for.
   */
  readonly schema: ParquetSchema;
  /**
   * Total rows the footer declares, across every group. A projection narrows
   * the columns, never the rows, so this is the whole file's count either way.
   */
  readonly rowCount: number;
  /** Number of row groups in the file. */
  readonly groupCount: number;
  /**
   * Decodes one row group per step, in file order.
   *
   * State lives in the iterator rather than in this object, so each call starts
   * again at the first group and two walks may run at the same time.
   */
  [Symbol.iterator](): IterableIterator<TStep>;
}

/**
 * A {@link ParquetRowGroups} read without codecs, and typed for it.
 *
 * Only a codec can make a read defer, so a read that registers none never
 * does — and its steps are plain rows arrays rather than maybe-promises.
 * `readRowGroups` hands this back whenever no `codecs` option is passed, which
 * mirrors what `readParquet` does with `ParquetFile`.
 */
export type SyncParquetRowGroups = ParquetRowGroups<ReadRow[]>;

/*
 * ---------------------------------------------------------------------------
 * Compression
 *
 * A Parquet v1 data page is compressed as one opaque body — the RLE definition
 * levels live *inside* it — so a codec is nothing but a byte transform. That is
 * the whole contract, and it is why tavolato can offer compression without
 * shipping a single compressor: you bring the one your runtime already has.
 * ---------------------------------------------------------------------------
 */

/**
 * The codecs Parquet's `CompressionCodec` enum names, minus `UNCOMPRESSED`
 * (which is the absence of a codec, not one you register).
 */
export type CodecName = "SNAPPY" | "GZIP" | "LZO" | "BROTLI" | "LZ4" | "ZSTD" | "LZ4_RAW";

/**
 * A page codec. Both halves are optional so that a single object can be handed
 * to the writer, to the reader, or to both; see {@link WriterCodec} and
 * {@link ReaderCodec} for the half each side insists on.
 *
 * Either function may be synchronous or asynchronous. A synchronous one keeps
 * the whole call synchronous — `append`, `finish` and `readParquet` only return
 * a promise when the codec hands them one.
 *
 * Bodies are the codec's own standard container: a GZIP page is an RFC 1952
 * member (so `gunzip`, not a raw inflate), a `ZSTD` page an RFC 8878 frame, a
 * `SNAPPY` page a raw block, and `LZ4_RAW` a raw LZ4 block — `LZ4` is the older
 * Hadoop framing that predates it.
 */
export interface Codec {
  /** Compresses one page body. */
  compress?(page: Uint8Array): Uint8Array | Promise<Uint8Array>;
  /**
   * Decompresses one page body back to exactly `uncompressedSize` bytes; the
   * size comes from the page header and is checked against what you return.
   */
  decompress?(page: Uint8Array, uncompressedSize: number): Uint8Array | Promise<Uint8Array>;
}

/** A {@link Codec} that can compress: what `WriterOptions.codec` needs. */
export interface WriterCodec extends Codec {
  /** Which codec the bytes are, stamped into every column chunk's metadata. */
  name: CodecName;
  compress(page: Uint8Array): Uint8Array | Promise<Uint8Array>;
}

/** A {@link Codec} that can decompress: what a `ReadOptions.codecs` entry needs. */
export interface ReaderCodec extends Codec {
  decompress(page: Uint8Array, uncompressedSize: number): Uint8Array | Promise<Uint8Array>;
}

/** Options accepted by `createWriter`. */
export interface WriterOptions {
  /**
   * Maximum number of rows buffered before a row group is flushed.
   * Defaults to `10_000`.
   */
  rowGroupSize?: number;
  /**
   * Value stored in the footer's `created_by` field. Defaults to `"tavolato"`.
   */
  createdBy?: string;
  /**
   * Compresses every data page body and stamps `name` into the file. Without
   * one the output is `UNCOMPRESSED`, byte for byte what tavolato has always
   * written.
   */
  codec?: WriterCodec;
}

/** Options accepted by `readParquet`. */
export interface ReadOptions {
  /**
   * Decompressors, by the codec they handle. A column chunk compressed with a
   * codec that is not registered here is refused by name, as it always was;
   * registering one is how you opt into reading it.
   */
  codecs?: Partial<Record<CodecName, ReaderCodec>>;
  /**
   * Logical column types, tried **in order**: the first adapter whose physical
   * type, byte width and `matches` all agree claims the column, and its object
   * becomes that column's `type` in the schema the reader returns.
   *
   * Adapters are consulted before the built-in types, so an annotated column is
   * yours to claim. The in-box adapters never claim an unannotated one, which
   * is what leaves the bare physical types to `i32`, `i64`, `f32`, `f64` and
   * `bool`.
   */
  types?: readonly AnyLogicalAdapter[];
  /**
   * Column projection: the names to read, and the only ones the rows and the
   * returned schema will carry.
   *
   * A column chunk is independently seekable, so an unselected column is not
   * decoded, not decompressed, and not even *looked at* — the reader skips
   * straight past its pages. That is the whole point, and it is why projection
   * lifts refusals rather than merely hiding columns: a file with an `INT96`
   * column, a dictionary-encoded one, or one annotated in a way nothing claims
   * is readable as long as those columns are projected away. The same goes for
   * codecs — an unselected chunk's codec never has to be registered.
   *
   * What projection does **not** lift is the shape of the schema itself. A
   * nested or repeated field means the file is not one flat level of columns,
   * and the mapping from schema to column chunks that projection walks is gone
   * with it; those are still refused whole-file.
   *
   * Column order is the **file's**, not the order asked for, in the rows and in
   * the schema alike — a projection is a set, and a deterministic order is
   * worth more than honouring an accident of argument order.
   *
   * Every name has to be a column the file declares, listed once: an unknown
   * name, a duplicate, or an empty list is `ERR_READ_OPTION_INVALID` rather
   * than a read that quietly does less than it was asked to.
   *
   * Honoured by `readParquet` and `readRowGroups`. `readSchema` ignores it:
   * projection is a property of a *read*, and inspecting what a file holds is
   * exactly the case where the answer should not have been narrowed first.
   */
  columns?: readonly string[];
}
