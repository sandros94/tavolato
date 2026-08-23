/**
 * The column types tavolato writes.
 *
 * | tavolato    | Parquet physical | Parquet logical                          |
 * | ----------- | ---------------- | ---------------------------------------- |
 * | `string`    | `BYTE_ARRAY`     | `STRING` (`UTF8`)                        |
 * | `json`      | `BYTE_ARRAY`     | `JSON`                                   |
 * | `f64`       | `DOUBLE`         | —                                        |
 * | `i64`       | `INT64`          | —                                        |
 * | `bool`      | `BOOLEAN`        | —                                        |
 * | `timestamp` | `INT64`          | `TIMESTAMP(UTC, MILLIS)` (`TIMESTAMP_MILLIS`) |
 *
 * The *shape* is frozen — one flat level of scalar columns, forever — but the
 * list itself grows: a minor version may add a member, as `json` was added
 * next to `string`. Switch over it with a `default` arm rather than an
 * exhaustive one, or a new member turns into a type error on upgrade.
 */
export type ColumnType = "string" | "json" | "f64" | "i64" | "bool" | "timestamp";

/** Declaration of a single column. */
export interface ColumnDefinition<TType extends ColumnType = ColumnType> {
  /** Column type. */
  type: TType;
  /** When `true` the column is nullable (Parquet `OPTIONAL`). Defaults to `false`. */
  optional?: boolean;
}

/** A flat map of column name to column declaration. */
export type SchemaDefinition = Record<string, ColumnDefinition>;

/** A normalized column, in the order it appears in the file. */
export interface SchemaColumn {
  readonly name: string;
  readonly type: ColumnType;
  readonly optional: boolean;
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
export type ColumnInput<TType extends ColumnType> = TType extends "string" | "json"
  ? string
  : TType extends "f64"
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
 * | `json`      | `string`     |
 * | `f64`       | `number`     |
 * | `i64`       | `bigint`     |
 * | `bool`      | `boolean`    |
 * | `timestamp` | `Date`       |
 */
export type ColumnOutput<TType extends ColumnType> = TType extends "string" | "json"
  ? string
  : TType extends "f64"
    ? number
    : TType extends "i64"
      ? bigint
      : TType extends "bool"
        ? boolean
        : TType extends "timestamp"
          ? Date
          : never;

/**
 * Any value the reader can produce; `null` for a null in an optional column.
 *
 * `Uint8Array` is reserved for a future raw-binary column type and is listed
 * here so that adding one is not a breaking change to every `switch` over a
 * read value. Nothing returns it today.
 */
export type ReadValue = string | number | bigint | boolean | Date | Uint8Array | null;

/** One row produced by the reader, keyed by column name in file order. */
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
  /** Derived from the file, in the same shape `defineSchema` produces. */
  readonly schema: ParquetSchema;
  /** Every row, in row group order and, within a group, in file order. */
  readonly rows: ReadRow[];
}

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
}
