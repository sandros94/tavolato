/**
 * The five column types tavolato writes. This list is frozen by design — see
 * the scope freeze in the README.
 *
 * | tavolato    | Parquet physical | Parquet logical                          |
 * | ----------- | ---------------- | ---------------------------------------- |
 * | `string`    | `BYTE_ARRAY`     | `STRING` (`UTF8`)                        |
 * | `f64`       | `DOUBLE`         | —                                        |
 * | `i64`       | `INT64`          | —                                        |
 * | `bool`      | `BOOLEAN`        | —                                        |
 * | `timestamp` | `INT64`          | `TIMESTAMP(UTC, MILLIS)` (`TIMESTAMP_MILLIS`) |
 */
export type ColumnType = "string" | "f64" | "i64" | "bool" | "timestamp";

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
export type ColumnInput<TType extends ColumnType> = TType extends "string"
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
 * | `f64`       | `number`     |
 * | `i64`       | `bigint`     |
 * | `bool`      | `boolean`    |
 * | `timestamp` | `Date`       |
 */
export type ColumnOutput<TType extends ColumnType> = TType extends "string"
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

/** Any value the reader can produce; `null` for a null in an optional column. */
export type ReadValue = string | number | bigint | boolean | Date | null;

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
}
