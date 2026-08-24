import { ByteReader } from "./bytes.ts";
import { CompactReader, CompactWriter, type ThriftField, ThriftType } from "./thrift.ts";
import { malformed } from "../error.ts";
import type {
  Annotation,
  AnyLogicalAdapter,
  CodecName,
  ColumnType,
  PhysicalKind,
  TimeUnitName,
} from "../types.ts";

/**
 * The `parquet.thrift` enums, as tavolato names them.
 *
 * The physical types are the whole enum: `INT96` included, because refusing it
 * by name is the only thing tavolato ever does with it.
 *
 * @see https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
 */
export const PhysicalType: {
  readonly BOOLEAN: 0;
  readonly INT32: 1;
  readonly INT64: 2;
  readonly INT96: 3;
  readonly FLOAT: 4;
  readonly DOUBLE: 5;
  readonly BYTE_ARRAY: 6;
  readonly FIXED_LEN_BYTE_ARRAY: 7;
} = {
  BOOLEAN: 0,
  INT32: 1,
  INT64: 2,
  INT96: 3,
  FLOAT: 4,
  DOUBLE: 5,
  BYTE_ARRAY: 6,
  FIXED_LEN_BYTE_ARRAY: 7,
} as const;

/**
 * The deprecated `ConvertedType` enum, in full.
 *
 * It is deprecated, not gone: every writer still emits it beside the modern
 * `LogicalType` so that readers predating the union keep working, and DuckDB
 * emits *only* this one for `INT_32`, `DATE` and the unsigned integers. Both
 * spellings therefore have to be understood, and both are written.
 */
export const ConvertedType: {
  readonly UTF8: 0;
  readonly MAP: 1;
  readonly MAP_KEY_VALUE: 2;
  readonly LIST: 3;
  readonly ENUM: 4;
  readonly DECIMAL: 5;
  readonly DATE: 6;
  readonly TIME_MILLIS: 7;
  readonly TIME_MICROS: 8;
  readonly TIMESTAMP_MILLIS: 9;
  readonly TIMESTAMP_MICROS: 10;
  readonly UINT_8: 11;
  readonly UINT_16: 12;
  readonly UINT_32: 13;
  readonly UINT_64: 14;
  readonly INT_8: 15;
  readonly INT_16: 16;
  readonly INT_32: 17;
  readonly INT_64: 18;
  readonly JSON: 19;
  readonly BSON: 20;
  readonly INTERVAL: 21;
} = {
  UTF8: 0,
  MAP: 1,
  MAP_KEY_VALUE: 2,
  LIST: 3,
  ENUM: 4,
  DECIMAL: 5,
  DATE: 6,
  TIME_MILLIS: 7,
  TIME_MICROS: 8,
  TIMESTAMP_MILLIS: 9,
  TIMESTAMP_MICROS: 10,
  UINT_8: 11,
  UINT_16: 12,
  UINT_32: 13,
  UINT_64: 14,
  INT_8: 15,
  INT_16: 16,
  INT_32: 17,
  INT_64: 18,
  JSON: 19,
  BSON: 20,
  INTERVAL: 21,
} as const;

export const FieldRepetitionType: {
  readonly REQUIRED: 0;
  readonly OPTIONAL: 1;
  readonly REPEATED: 2;
} = { REQUIRED: 0, OPTIONAL: 1, REPEATED: 2 } as const;

export const Encoding: { readonly PLAIN: 0; readonly RLE: 3 } = { PLAIN: 0, RLE: 3 } as const;

/**
 * The whole `CompressionCodec` enum. tavolato compresses nothing itself, but it
 * has to name every codec: to stamp the one a caller's hook produced, and to
 * look one up when a file it did not write asks for it.
 */
export const CompressionCodec: {
  readonly UNCOMPRESSED: 0;
  readonly SNAPPY: 1;
  readonly GZIP: 2;
  readonly LZO: 3;
  readonly BROTLI: 4;
  readonly LZ4: 5;
  readonly ZSTD: 6;
  readonly LZ4_RAW: 7;
} = {
  UNCOMPRESSED: 0,
  SNAPPY: 1,
  GZIP: 2,
  LZO: 3,
  BROTLI: 4,
  LZ4: 5,
  ZSTD: 6,
  LZ4_RAW: 7,
} as const;

/** Every codec a caller may register, by name — `UNCOMPRESSED` deliberately absent. */
export const CODEC_IDS: Readonly<Record<CodecName, number>> = {
  SNAPPY: CompressionCodec.SNAPPY,
  GZIP: CompressionCodec.GZIP,
  LZO: CompressionCodec.LZO,
  BROTLI: CompressionCodec.BROTLI,
  LZ4: CompressionCodec.LZ4,
  ZSTD: CompressionCodec.ZSTD,
  LZ4_RAW: CompressionCodec.LZ4_RAW,
};

/** The `CompressionCodec` id `name` stands for, or `undefined` if it stands for none. */
export function codecId(name: string): number | undefined {
  return Object.hasOwn(CODEC_IDS, name) ? CODEC_IDS[name as CodecName] : undefined;
}

/**
 * The name a caller would have registered `id` under, or `undefined` for
 * `UNCOMPRESSED` and for an id no released Parquet version defines.
 */
export function registrableCodec(id: number): CodecName | undefined {
  // CODEC_NAMES is the reject-message table below; every entry past index 0 is
  // a name `CodecName` also holds, which is what makes the cast sound.
  return id === CompressionCodec.UNCOMPRESSED
    ? undefined
    : (CODEC_NAMES[id] as CodecName | undefined);
}

export const PageType: { readonly DATA_PAGE: 0 } = { DATA_PAGE: 0 } as const;

/**
 * The `LogicalType` union field ids.
 *
 * Field 9 is reserved for the `INTERVAL` the union never gained; the nested
 * types (`MAP`, `LIST`) are here to be *named* in a refusal, never to be read.
 */
export const LogicalTypeId: {
  readonly STRING: 1;
  readonly MAP: 2;
  readonly LIST: 3;
  readonly ENUM: 4;
  readonly DECIMAL: 5;
  readonly DATE: 6;
  readonly TIME: 7;
  readonly TIMESTAMP: 8;
  readonly INTERVAL: 9;
  readonly INTEGER: 10;
  readonly NULL: 11;
  readonly JSON: 12;
  readonly BSON: 13;
  readonly UUID: 14;
  readonly FLOAT16: 15;
  readonly VARIANT: 16;
  readonly GEOMETRY: 17;
  readonly GEOGRAPHY: 18;
  readonly FILE: 19;
} = {
  STRING: 1,
  MAP: 2,
  LIST: 3,
  ENUM: 4,
  DECIMAL: 5,
  DATE: 6,
  TIME: 7,
  TIMESTAMP: 8,
  INTERVAL: 9,
  INTEGER: 10,
  NULL: 11,
  JSON: 12,
  BSON: 13,
  UUID: 14,
  FLOAT16: 15,
  VARIANT: 16,
  GEOMETRY: 17,
  GEOGRAPHY: 18,
  FILE: 19,
} as const;

/**
 * The field id that stands for "an annotation this version cannot name".
 *
 * Zero is not a legal Thrift field id, so it can never collide with a real
 * member of the union — which is what makes it the right marker for a
 * `ConvertedType` outside the enum, or a `LogicalType` struct carrying no
 * member at all.
 */
export const UNNAMED_ANNOTATION = 0 as const;

/** `TimeUnit` union field ids. */
export const TimeUnit: { readonly MILLIS: 1; readonly MICROS: 2; readonly NANOS: 3 } = {
  MILLIS: 1,
  MICROS: 2,
  NANOS: 3,
} as const;

/** The `TimeUnit` union field id for each resolution, and back again. */
const TIME_UNIT_IDS: Readonly<Record<TimeUnitName, number>> = {
  millis: TimeUnit.MILLIS,
  micros: TimeUnit.MICROS,
  nanos: TimeUnit.NANOS,
};

const TIME_UNIT_KINDS: readonly (TimeUnitName | undefined)[] = [
  undefined,
  "millis",
  "micros",
  "nanos",
];

/** `PAR1`, the four magic bytes that open and close every Parquet file. */
export const MAGIC: Uint8Array = /* @__PURE__ */ new Uint8Array([0x50, 0x41, 0x52, 0x31]);

/**
 * A flat schema has exactly one optional level per column, so nullable columns
 * always use definition levels of width 1 and non-nullable columns write none
 * at all.
 */
export const MAX_DEFINITION_LEVEL_BIT_WIDTH: number = 1;

/** The `Type` id each physical kind is stored as, and back again. */
const PHYSICAL_IDS: Readonly<Record<PhysicalKind, number>> = {
  bool: PhysicalType.BOOLEAN,
  i32: PhysicalType.INT32,
  i64: PhysicalType.INT64,
  f32: PhysicalType.FLOAT,
  f64: PhysicalType.DOUBLE,
  bytes: PhysicalType.BYTE_ARRAY,
  fixed: PhysicalType.FIXED_LEN_BYTE_ARRAY,
};

/** `INT96` has no kind: it is deprecated in the format and permanently refused. */
const PHYSICAL_KINDS: readonly (PhysicalKind | undefined)[] = [
  "bool",
  "i32",
  "i64",
  undefined,
  "f32",
  "f64",
  "bytes",
  "fixed",
];

export function physicalTypeId(kind: PhysicalKind): number {
  return PHYSICAL_IDS[kind];
}

/** The kind a `Type` id stands for, or `undefined` for `INT96` and for no type at all. */
export function physicalKindOf(id: number): PhysicalKind | undefined {
  return PHYSICAL_KINDS[id];
}

const NO_ANNOTATION: Annotation = { kind: "none" };

/** How each built-in column type is stored, and what it is annotated with. */
const BUILTINS: Readonly<
  Record<ColumnType, { readonly physical: PhysicalKind; readonly annotation: Annotation }>
> = {
  string: { physical: "bytes", annotation: { kind: "string" } },
  json: { physical: "bytes", annotation: { kind: "json" } },
  f64: { physical: "f64", annotation: NO_ANNOTATION },
  f32: { physical: "f32", annotation: NO_ANNOTATION },
  i64: { physical: "i64", annotation: NO_ANNOTATION },
  i32: { physical: "i32", annotation: NO_ANNOTATION },
  bool: { physical: "bool", annotation: NO_ANNOTATION },
  timestamp: {
    physical: "i64",
    annotation: { kind: "timestamp", unit: "millis", isAdjustedToUTC: true },
  },
};

/** Which physical type — and therefore which value buffer — a column uses. */
export function columnPhysical(type: ColumnType | AnyLogicalAdapter): PhysicalKind {
  return typeof type === "string" ? BUILTINS[type].physical : type.physical;
}

/** What a column is annotated with on the way into a file. */
export function columnAnnotation(type: ColumnType | AnyLogicalAdapter): Annotation {
  return typeof type === "string" ? BUILTINS[type].annotation : type.annotate();
}

/** A column's `type_length`, which only a `FIXED_LEN_BYTE_ARRAY` has. */
export function columnTypeLength(type: ColumnType | AnyLogicalAdapter): number | undefined {
  return typeof type === "string" || type.physical !== "fixed" ? undefined : type.typeLength;
}

/** What to call a column type in an error message: the name, or the adapter's. */
export function columnTypeName(type: ColumnType | AnyLogicalAdapter): string {
  return typeof type === "string" ? type : type.name;
}

/** Everything the footer needs to know about one column chunk. */
export interface ColumnChunkMeta {
  readonly name: string;
  /** Taken from the same {@link ColumnSnapshot} the schema element is written from. */
  readonly physical: PhysicalKind;
  readonly optional: boolean;
  /** `CompressionCodec` id the page bodies were written with. */
  readonly codec: number;
  readonly numValues: number;
  readonly nullCount: number;
  /** Absolute file offset of the column chunk's first (and only) page header. */
  readonly dataPageOffset: number;
  /** Page headers plus page bodies as they would be uncompressed, in bytes. */
  readonly totalUncompressedSize: number;
  /** Page headers plus page bodies as they sit in the file, in bytes. */
  readonly totalCompressedSize: number;
}

/** Everything the footer needs to know about one row group. */
export interface RowGroupMeta {
  readonly columns: readonly ColumnChunkMeta[];
  readonly numRows: number;
  /** `total_byte_size`, which the format defines as the *uncompressed* size. */
  readonly totalByteSize: number;
  readonly totalCompressedSize: number;
  readonly fileOffset: number;
}

/**
 * Serializes a v1 `PageHeader` wrapping a `DataPageHeader`.
 *
 * Both sizes are the body's, header excluded, and they are equal for every page
 * written without a codec.
 *
 * The optional `crc` field is deliberately not written: page checksums are
 * optional in the format, and omitting them keeps the core free of any
 * hashing dependency.
 */
export function encodeDataPageHeader(
  uncompressedSize: number,
  compressedSize: number,
  numValues: number,
): Uint8Array {
  const writer = new CompactWriter();
  writer.structBegin();
  writer.fieldI32(1, PageType.DATA_PAGE);
  writer.fieldI32(2, uncompressedSize);
  writer.fieldI32(3, compressedSize);
  writer.fieldStructBegin(5); // data_page_header
  writer.fieldI32(1, numValues);
  writer.fieldI32(2, Encoding.PLAIN);
  writer.fieldI32(3, Encoding.RLE); // definition_level_encoding
  writer.fieldI32(4, Encoding.RLE); // repetition_level_encoding
  writer.structEnd();
  writer.structEnd();
  return writer.toBytes();
}

/**
 * The `ConvertedType` an annotation is also spelled as, or `undefined` where
 * the deprecated enum has no word for it (`UUID`, `FLOAT16`, and anything in
 * nanoseconds).
 *
 * The legacy spelling defines `TIME_*` and `TIMESTAMP_*` as UTC-normalised,
 * which an annotation that is not adjusted to UTC contradicts — and it is
 * still written, because that is what every other writer does (DuckDB's own
 * `TIME` is a `TIME_MICROS` with `isAdjustedToUTC=0`) and because dropping it
 * would hide the column from readers that only know the old enum.
 */
function convertedTypeOf(annotation: Annotation): number | undefined {
  switch (annotation.kind) {
    case "string": {
      return ConvertedType.UTF8;
    }
    case "json": {
      return ConvertedType.JSON;
    }
    case "bson": {
      return ConvertedType.BSON;
    }
    case "enum": {
      return ConvertedType.ENUM;
    }
    case "decimal": {
      return ConvertedType.DECIMAL;
    }
    case "date": {
      return ConvertedType.DATE;
    }
    case "time": {
      return annotation.unit === "millis"
        ? ConvertedType.TIME_MILLIS
        : annotation.unit === "micros"
          ? ConvertedType.TIME_MICROS
          : undefined;
    }
    case "timestamp": {
      return annotation.unit === "millis"
        ? ConvertedType.TIMESTAMP_MILLIS
        : annotation.unit === "micros"
          ? ConvertedType.TIMESTAMP_MICROS
          : undefined;
    }
    case "integer": {
      const width = INTEGER_WIDTH_ORDER.indexOf(annotation.bitWidth);
      return (annotation.isSigned ? ConvertedType.INT_8 : ConvertedType.UINT_8) + width;
    }
    default: {
      // none, uuid, float16 and unknown have no deprecated spelling.
      return undefined;
    }
  }
}

/** `UINT_8 … UINT_64` and `INT_8 … INT_64` both run in this order. */
const INTEGER_WIDTH_ORDER: readonly (8 | 16 | 32 | 64)[] = [8, 16, 32, 64];

/** Writes the `TimeUnit` union inside a `TimeType` / `TimestampType`. */
function writeTimeUnit(writer: CompactWriter, unit: TimeUnitName): void {
  writer.fieldStructBegin(2); // unit
  writer.fieldStructBegin(TIME_UNIT_IDS[unit]);
  writer.structEnd();
  writer.structEnd();
}

/**
 * Writes `logicalType` (field 10), the modern annotation; the `ConvertedType`
 * beside it stays for readers that predate the union.
 *
 * Every member is an empty struct unless the format gives it parameters, and
 * the parameterised ones are written in `parquet.thrift`'s own field order.
 */
function writeLogicalType(writer: CompactWriter, annotation: Annotation): void {
  switch (annotation.kind) {
    case "string":
    case "enum":
    case "date":
    case "json":
    case "bson":
    case "uuid":
    case "float16": {
      // An empty struct: the annotation is the name, and says nothing more.
      writer.fieldStructBegin(10);
      writer.fieldStructBegin(EMPTY_LOGICAL_TYPE_IDS[annotation.kind]);
      writer.structEnd();
      writer.structEnd();
      break;
    }
    case "decimal": {
      writer.fieldStructBegin(10);
      writer.fieldStructBegin(LogicalTypeId.DECIMAL);
      writer.fieldI32(1, annotation.scale); // DecimalType puts scale first
      writer.fieldI32(2, annotation.precision);
      writer.structEnd();
      writer.structEnd();
      break;
    }
    case "time":
    case "timestamp": {
      writer.fieldStructBegin(10);
      writer.fieldStructBegin(
        annotation.kind === "time" ? LogicalTypeId.TIME : LogicalTypeId.TIMESTAMP,
      );
      writer.fieldBool(1, annotation.isAdjustedToUTC);
      writeTimeUnit(writer, annotation.unit);
      writer.structEnd();
      writer.structEnd();
      break;
    }
    case "integer": {
      writer.fieldStructBegin(10);
      writer.fieldStructBegin(LogicalTypeId.INTEGER);
      writer.fieldI8(1, annotation.bitWidth); // IntType.bitWidth is an i8
      writer.fieldBool(2, annotation.isSigned);
      writer.structEnd();
      writer.structEnd();
      break;
    }
    // No default: `none` carries no annotation at all, and `unknown` never
    // reaches a schema — `defineColumnType` refuses an adapter that returns one.
  }
}

/** The members of the union that are a bare name and nothing else. */
const EMPTY_LOGICAL_TYPE_IDS: Readonly<
  Record<"string" | "enum" | "date" | "json" | "bson" | "uuid" | "float16", number>
> = {
  string: LogicalTypeId.STRING,
  enum: LogicalTypeId.ENUM,
  date: LogicalTypeId.DATE,
  json: LogicalTypeId.JSON,
  bson: LogicalTypeId.BSON,
  uuid: LogicalTypeId.UUID,
  float16: LogicalTypeId.FLOAT16,
};

function writeSchemaElement(writer: CompactWriter, column: ColumnSnapshot): void {
  const { annotation, typeLength } = column;
  writer.structBegin();
  writer.fieldI32(1, physicalTypeId(column.physical));
  if (typeLength !== undefined) writer.fieldI32(2, typeLength);
  writer.fieldI32(3, column.optional ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED);
  writer.fieldString(4, column.name);
  const convertedType = convertedTypeOf(annotation);
  if (convertedType !== undefined) writer.fieldI32(6, convertedType);
  if (annotation.kind === "decimal") {
    // The deprecated decimal spelling keeps its parameters on the element
    // itself, and the format requires them there for a DECIMAL column.
    writer.fieldI32(7, annotation.scale);
    writer.fieldI32(8, annotation.precision);
  }
  writeLogicalType(writer, annotation);
  writer.structEnd();
}

/** The shape {@link snapshotColumn} takes; satisfied by a `SchemaColumn`. */
export interface SchemaColumnLike {
  readonly name: string;
  readonly type: ColumnType | AnyLogicalAdapter;
  readonly optional: boolean;
}

/**
 * A column as the file will state it: everything an adapter answers, read once
 * and never asked again.
 *
 * `physical`, `typeLength` and `annotate()` are all live properties of a
 * caller's object. A writer shapes its buffers from them when it is built and
 * writes its footer from them at `finish()` — so they are read **once**, here,
 * and both halves of the file are built from the same answers. A column type
 * that changes its mind in between can no longer produce a footer describing
 * pages that were never written.
 */
export interface ColumnSnapshot {
  readonly name: string;
  readonly optional: boolean;
  readonly physical: PhysicalKind;
  readonly typeLength: number | undefined;
  readonly annotation: Annotation;
}

/** Takes that snapshot, once, where a write begins. */
export function snapshotColumn(column: SchemaColumnLike): ColumnSnapshot {
  return Object.freeze({
    name: column.name,
    optional: column.optional,
    physical: columnPhysical(column.type),
    typeLength: columnTypeLength(column.type),
    annotation: columnAnnotation(column.type),
  });
}

function writeColumnChunk(writer: CompactWriter, chunk: ColumnChunkMeta): void {
  writer.structBegin();
  // file_offset is deprecated; the format tells writers to store 0 when no
  // ColumnMetaData is written outside the footer, which is our case.
  writer.fieldI64(2, 0n);
  writer.fieldStructBegin(3); // meta_data
  writer.fieldI32(1, physicalTypeId(chunk.physical));
  const encodings = chunk.optional ? [Encoding.RLE, Encoding.PLAIN] : [Encoding.PLAIN];
  writer.fieldListBegin(2, ThriftType.I32, encodings.length);
  for (const encoding of encodings) writer.elementI32(encoding);
  writer.fieldListBegin(3, ThriftType.BINARY, 1); // path_in_schema
  writer.elementString(chunk.name);
  writer.fieldI32(4, chunk.codec);
  writer.fieldI64(5, BigInt(chunk.numValues));
  writer.fieldI64(6, BigInt(chunk.totalUncompressedSize));
  writer.fieldI64(7, BigInt(chunk.totalCompressedSize));
  writer.fieldI64(9, BigInt(chunk.dataPageOffset));
  writer.fieldStructBegin(12); // statistics
  writer.fieldI64(3, BigInt(chunk.nullCount));
  writer.structEnd();
  writer.structEnd();
  writer.structEnd();
}

function writeRowGroup(writer: CompactWriter, group: RowGroupMeta): void {
  writer.structBegin();
  writer.fieldListBegin(1, ThriftType.STRUCT, group.columns.length);
  for (const chunk of group.columns) writeColumnChunk(writer, chunk);
  writer.fieldI64(2, BigInt(group.totalByteSize));
  writer.fieldI64(3, BigInt(group.numRows));
  writer.fieldI64(5, BigInt(group.fileOffset));
  writer.fieldI64(6, BigInt(group.totalCompressedSize));
  writer.structEnd();
}

/** Serializes the `FileMetaData` footer struct. */
export function encodeFileMetadata(
  columns: readonly ColumnSnapshot[],
  rowGroups: readonly RowGroupMeta[],
  numRows: number,
  createdBy: string,
): Uint8Array {
  const writer = new CompactWriter();
  writer.structBegin();
  writer.fieldI32(1, 1); // version — always 1 for maximum reader compatibility

  // The schema is a depth-first flattening of the tree; element 0 is the root
  // group, which carries no type and no repetition_type.
  writer.fieldListBegin(2, ThriftType.STRUCT, columns.length + 1);
  writer.structBegin();
  writer.fieldString(4, "schema");
  writer.fieldI32(5, columns.length); // num_children
  writer.structEnd();
  for (const column of columns) writeSchemaElement(writer, column);

  writer.fieldI64(3, BigInt(numRows));
  writer.fieldListBegin(4, ThriftType.STRUCT, rowGroups.length);
  for (const group of rowGroups) writeRowGroup(writer, group);
  writer.fieldString(6, createdBy);
  writer.structEnd();
  return writer.toBytes();
}

/*
 * ---------------------------------------------------------------------------
 * Decoding
 *
 * The reader only pulls out the fields it needs; every other field is skipped
 * through the Thrift protocol, so a footer written by a newer Parquet release
 * (or by another implementation with more to say) still reads.
 * ---------------------------------------------------------------------------
 */

/**
 * Names for the enum values the reader may have to reject, so errors can say what
 * it found. The deprecated `ConvertedType` has no table of its own: it is
 * decoded into the annotation model, and an annotation names itself.
 */
const PHYSICAL_TYPE_NAMES = [
  "BOOLEAN",
  "INT32",
  "INT64",
  "INT96",
  "FLOAT",
  "DOUBLE",
  "BYTE_ARRAY",
  "FIXED_LEN_BYTE_ARRAY",
];

const LOGICAL_TYPE_NAMES = [
  "",
  "STRING",
  "MAP",
  "LIST",
  "ENUM",
  "DECIMAL",
  "DATE",
  "TIME",
  "TIMESTAMP",
  "INTERVAL", // reserved in the union, and what the deprecated enum called it
  "INTEGER",
  "UNKNOWN",
  "JSON",
  "BSON",
  "UUID",
  "FLOAT16",
  "VARIANT",
  "GEOMETRY",
  "GEOGRAPHY",
  "FILE",
];

const ENCODING_NAMES = [
  "PLAIN",
  "",
  "PLAIN_DICTIONARY",
  "RLE",
  "BIT_PACKED",
  "DELTA_BINARY_PACKED",
  "DELTA_LENGTH_BYTE_ARRAY",
  "DELTA_BYTE_ARRAY",
  "RLE_DICTIONARY",
  "BYTE_STREAM_SPLIT",
];

const CODEC_NAMES = ["UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "BROTLI", "LZ4", "ZSTD", "LZ4_RAW"];

const PAGE_TYPE_NAMES = ["DATA_PAGE", "INDEX_PAGE", "DICTIONARY_PAGE", "DATA_PAGE_V2"];

function nameOf(names: readonly string[], id: number | undefined, kind: string): string {
  const name = id === undefined ? undefined : names[id];
  return name === undefined || name === "" ? `${kind} ${id}` : name;
}

export function physicalTypeName(id: number | undefined): string {
  return nameOf(PHYSICAL_TYPE_NAMES, id, "physical type");
}

export function logicalTypeName(id: number | undefined): string {
  return nameOf(LOGICAL_TYPE_NAMES, id, "logical type");
}

export function encodingName(id: number | undefined): string {
  return nameOf(ENCODING_NAMES, id, "encoding");
}

export function codecName(id: number | undefined): string {
  return nameOf(CODEC_NAMES, id, "codec");
}

export function pageTypeName(id: number | undefined): string {
  return nameOf(PAGE_TYPE_NAMES, id, "page type");
}

/**
 * How an annotation reads in an error message, parameters included: the point
 * of refusing by name is that the name says everything the caller needs to
 * write an adapter for it.
 */
export function annotationName(annotation: Annotation): string {
  switch (annotation.kind) {
    case "decimal": {
      return `DECIMAL(precision=${annotation.precision}, scale=${annotation.scale})`;
    }
    case "time":
    case "timestamp": {
      const name = annotation.kind === "time" ? "TIME" : "TIMESTAMP";
      return `${name}(${annotation.unit.toUpperCase()}, isAdjustedToUTC=${annotation.isAdjustedToUTC})`;
    }
    case "integer": {
      return `INTEGER(${annotation.bitWidth}, ${annotation.isSigned ? "signed" : "unsigned"})`;
    }
    case "unknown": {
      return annotation.id === UNNAMED_ANNOTATION
        ? "an annotation this version has no name for"
        : logicalTypeName(annotation.id);
    }
    default: {
      // Every remaining member is a bare name, and `none` is never named.
      return annotation.kind.toUpperCase();
    }
  }
}

/** One `SchemaElement`, as far as the reader inspects it. */
export interface SchemaElement {
  readonly name: string;
  readonly physical?: number;
  /** `type_length`, the byte width of a `FIXED_LEN_BYTE_ARRAY`. */
  readonly typeLength?: number;
  readonly repetition?: number;
  readonly numChildren: number;
  readonly convertedType?: number;
  /** The deprecated decimal parameters, which live on the element rather than in the annotation. */
  readonly scale?: number;
  readonly precision?: number;
  /** The modern annotation, already reduced to the model; wins over `convertedType`. */
  readonly logical?: Annotation;
}

/**
 * What a `SchemaElement` says its column means.
 *
 * The modern `LogicalType` wins wherever a file carries both spellings, which
 * is what the format prescribes: the deprecated enum cannot express half of
 * what the union can, so where they disagree the union is the truth.
 *
 * Nothing here throws. An annotation this version cannot name decodes to
 * `unknown` and is refused — by name — one layer up, where the column is
 * known and an adapter has had its chance to claim it.
 */
export function annotationOf(element: SchemaElement): Annotation {
  return element.logical ?? convertedAnnotation(element);
}

/** Reads the deprecated `ConvertedType` (plus its element-level parameters) into the model. */
function convertedAnnotation(element: SchemaElement): Annotation {
  const { convertedType } = element;
  switch (convertedType) {
    case undefined: {
      return NO_ANNOTATION;
    }
    case ConvertedType.UTF8: {
      return { kind: "string" };
    }
    case ConvertedType.JSON: {
      return { kind: "json" };
    }
    case ConvertedType.BSON: {
      return { kind: "bson" };
    }
    case ConvertedType.ENUM: {
      return { kind: "enum" };
    }
    case ConvertedType.DATE: {
      return { kind: "date" };
    }
    case ConvertedType.DECIMAL: {
      return { kind: "decimal", precision: element.precision ?? 0, scale: element.scale ?? 0 };
    }
    case ConvertedType.TIME_MILLIS:
    case ConvertedType.TIME_MICROS:
    case ConvertedType.TIMESTAMP_MILLIS:
    case ConvertedType.TIMESTAMP_MICROS: {
      // The deprecated spelling only knows two resolutions, and defines both as
      // UTC-normalised (LogicalTypes.md, backward compatibility).
      const millis =
        convertedType === ConvertedType.TIME_MILLIS ||
        convertedType === ConvertedType.TIMESTAMP_MILLIS;
      return {
        kind: convertedType <= ConvertedType.TIME_MICROS ? "time" : "timestamp",
        unit: millis ? "millis" : "micros",
        isAdjustedToUTC: true,
      };
    }
    case ConvertedType.UINT_8:
    case ConvertedType.UINT_16:
    case ConvertedType.UINT_32:
    case ConvertedType.UINT_64:
    case ConvertedType.INT_8:
    case ConvertedType.INT_16:
    case ConvertedType.INT_32:
    case ConvertedType.INT_64: {
      const isSigned = convertedType >= ConvertedType.INT_8;
      const base = isSigned ? ConvertedType.INT_8 : ConvertedType.UINT_8;
      return { kind: "integer", bitWidth: INTEGER_WIDTH_ORDER[convertedType - base], isSigned };
    }
    case ConvertedType.MAP:
    case ConvertedType.MAP_KEY_VALUE: {
      return { kind: "unknown", id: LogicalTypeId.MAP };
    }
    case ConvertedType.LIST: {
      return { kind: "unknown", id: LogicalTypeId.LIST };
    }
    case ConvertedType.INTERVAL: {
      return { kind: "unknown", id: LogicalTypeId.INTERVAL };
    }
    default: {
      // An id outside the enum: real Parquet has none, and guessing `none`
      // would read a column tavolato cannot vouch for.
      return { kind: "unknown", id: UNNAMED_ANNOTATION };
    }
  }
}

/** One `ColumnChunk` plus its inlined `ColumnMetaData`. */
export interface ColumnChunkInfo {
  readonly path: readonly string[];
  readonly physical?: number;
  readonly codec: number;
  readonly numValues: number;
  readonly dataPageOffset: number;
  readonly dictionaryPageOffset?: number;
  /**
   * Bytes the chunk occupies in the file, page headers included — which is to
   * say where it *ends*: the one thing a reader holding nothing but the footer
   * cannot work out for itself.
   *
   * Only a read that fetches a chunk without the rest of the file around it has
   * any use for that, so this is decoded leniently and is `undefined` wherever
   * the file leaves it out or states something no byte count could be. A local
   * read never looks at it, and a file is never refused over it.
   */
  readonly totalCompressedSize?: number;
}

export interface RowGroupInfo {
  readonly columns: readonly ColumnChunkInfo[];
  readonly numRows: number;
}

export interface FileMetadata {
  readonly schema: readonly SchemaElement[];
  readonly numRows: number;
  readonly rowGroups: readonly RowGroupInfo[];
  readonly createdBy?: string;
}

/** A v1 `PageHeader`, reduced to what the reader checks and needs. */
export interface PageHeaderInfo {
  readonly pageType: number;
  /** Bytes the body occupies in the file. */
  readonly compressedSize: number;
  /** Bytes the body must decompress back to; equal to `compressedSize` when it was never compressed. */
  readonly uncompressedSize: number;
  readonly numValues: number;
  readonly encoding: number;
  readonly definitionLevelEncoding: number;
}

/** Narrows a 64-bit file field to a usable offset or count. */
function toCount(value: bigint, what: string): number {
  if (value < 0n || value > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw malformed(`${what} is ${value}, which cannot be a valid file position or count`);
  }
  return Number(value);
}

/**
 * Runs `read` for each field of a struct and skips every field it does not
 * claim, which is the Thrift rule for unrecognised fields and the reason a
 * richer footer from another writer still parses.
 */
function eachField(reader: CompactReader, read: (field: ThriftField) => boolean): void {
  reader.structBegin();
  for (let field = reader.fieldBegin(); field !== null; field = reader.fieldBegin()) {
    if (!read(field)) reader.skip(field.type);
  }
  reader.structEnd();
}

function decodeTimeUnit(reader: CompactReader): TimeUnitName | undefined {
  let unit: TimeUnitName | undefined;
  eachField(reader, (field) => {
    unit = TIME_UNIT_KINDS[field.id];
    return false; // the union's payload is an empty struct; skipping it reads the stop byte
  });
  return unit;
}

/*
 * Every parameter below is `required` in `parquet.thrift`, and every one of
 * them is claimed only when the field really is the Thrift type the format
 * declares for it.
 *
 * Both halves of that matter. A field of the wrong type carries a payload of a
 * different shape — a `bool`'s value rides in its header and costs no bytes at
 * all, an `i32` is a varint, a `binary` is a length and then bytes — so
 * claiming one without reading it the way it was written leaves the rest of the
 * footer misaligned, and the failure surfaces fields later as a Thrift type
 * that does not exist. And a required parameter that is missing is not a
 * default: `isSigned` decides what half of an integer's range means, and a unit
 * decides a value's order of magnitude. A member the file did not spell out is
 * one this version cannot read, which is exactly what `unknown` says — and it
 * is refused a layer up, where the column has a name.
 */

/** Whether a field header carries a bool, whose value *is* its Thrift type. */
function isBoolField(type: number): boolean {
  return type === ThriftType.BOOLEAN_TRUE || type === ThriftType.BOOLEAN_FALSE;
}

/** `TimeType` and `TimestampType` are the same two fields, in the same order. */
function decodeTimeLike(reader: CompactReader, kind: "time" | "timestamp"): Annotation {
  let isAdjustedToUTC: boolean | undefined;
  let unit: TimeUnitName | undefined;
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        // A bool carries its value in the field header, so there is nothing to
        // read — as long as the field is one.
        if (!isBoolField(field.type)) return false;
        isAdjustedToUTC = reader.bool(field.type);
        return true;
      }
      case 2: {
        if (field.type !== ThriftType.STRUCT) return false;
        unit = decodeTimeUnit(reader);
        return true;
      }
      default: {
        return false;
      }
    }
  });
  // A resolution nobody has named yet is not a `TIME` this version understands,
  // and pretending it is milliseconds would move every value.
  return unit === undefined || isAdjustedToUTC === undefined
    ? { kind: "unknown", id: kind === "time" ? LogicalTypeId.TIME : LogicalTypeId.TIMESTAMP }
    : { kind, unit, isAdjustedToUTC };
}

/** `DecimalType { 1: i32 scale, 2: i32 precision }` — scale first, as the format has it. */
function decodeDecimalType(reader: CompactReader): Annotation {
  let scale: number | undefined;
  let precision: number | undefined;
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        if (field.type !== ThriftType.I32) return false;
        scale = reader.i32();
        return true;
      }
      case 2: {
        if (field.type !== ThriftType.I32) return false;
        precision = reader.i32();
        return true;
      }
      default: {
        return false;
      }
    }
  });
  return scale === undefined || precision === undefined
    ? { kind: "unknown", id: LogicalTypeId.DECIMAL }
    : { kind: "decimal", precision, scale };
}

/** `IntType { 1: i8 bitWidth, 2: bool isSigned }`. */
function decodeIntType(reader: CompactReader): Annotation {
  let bitWidth = 0;
  let isSigned: boolean | undefined;
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        if (field.type !== ThriftType.I8) return false;
        bitWidth = reader.i8();
        return true;
      }
      case 2: {
        if (!isBoolField(field.type)) return false;
        isSigned = reader.bool(field.type);
        return true;
      }
      default: {
        return false;
      }
    }
  });
  const width = INTEGER_WIDTH_ORDER.find((candidate) => candidate === bitWidth);
  return width === undefined || isSigned === undefined
    ? { kind: "unknown", id: LogicalTypeId.INTEGER }
    : { kind: "integer", bitWidth: width, isSigned };
}

/**
 * Reads the `LogicalType` union straight into the annotation model.
 *
 * A member with no parameters is skipped rather than descended into, which
 * consumes its empty struct's stop byte; the parameterised ones are read here
 * and nowhere else. A known but unsupported member, or one a later release
 * adds, decodes to its field id so it can still be named in a refusal and
 * offered to an adapter where its physical contract permits one.
 */
function decodeLogicalType(reader: CompactReader): Annotation {
  let annotation: Annotation = { kind: "unknown", id: UNNAMED_ANNOTATION };
  eachField(reader, (field) => {
    switch (field.id) {
      case LogicalTypeId.STRING: {
        annotation = { kind: "string" };
        return false;
      }
      case LogicalTypeId.ENUM: {
        annotation = { kind: "enum" };
        return false;
      }
      case LogicalTypeId.DATE: {
        annotation = { kind: "date" };
        return false;
      }
      case LogicalTypeId.JSON: {
        annotation = { kind: "json" };
        return false;
      }
      case LogicalTypeId.BSON: {
        annotation = { kind: "bson" };
        return false;
      }
      case LogicalTypeId.UUID: {
        annotation = { kind: "uuid" };
        return false;
      }
      case LogicalTypeId.FLOAT16: {
        annotation = { kind: "float16" };
        return false;
      }
      case LogicalTypeId.DECIMAL: {
        if (field.type !== ThriftType.STRUCT) return false;
        annotation = decodeDecimalType(reader);
        return true;
      }
      case LogicalTypeId.TIME:
      case LogicalTypeId.TIMESTAMP: {
        if (field.type !== ThriftType.STRUCT) return false;
        annotation = decodeTimeLike(reader, field.id === LogicalTypeId.TIME ? "time" : "timestamp");
        return true;
      }
      case LogicalTypeId.INTEGER: {
        if (field.type !== ThriftType.STRUCT) return false;
        annotation = decodeIntType(reader);
        return true;
      }
      default: {
        annotation = { kind: "unknown", id: field.id };
        return false;
      }
    }
  });
  return annotation;
}

/*
 * The rest of the footer is claimed under the same rule as the annotations
 * above: a field is only read as what it is declared to be.
 *
 * The reason is the protocol rather than the format. A compact `i32` is a
 * varint, a `binary` is a length and then bytes, a `struct` ends in a stop
 * byte, a `list` opens with a header — so a field claimed and read the wrong
 * way consumes the wrong number of bytes, and every field after it is parsed
 * from the middle of something. That surfaces as a Thrift type that does not
 * exist, at an offset that means nothing to anybody. Skipping the field
 * instead leaves the stream where it belongs, and leaves the value missing —
 * which the checks downstream already refuse, by name and with the column.
 */

function decodeSchemaElement(reader: CompactReader): SchemaElement {
  let name = "";
  let physical: number | undefined;
  let typeLength: number | undefined;
  let repetition: number | undefined;
  let numChildren = 0;
  let convertedType: number | undefined;
  let scale: number | undefined;
  let precision: number | undefined;
  let logical: Annotation | undefined;
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        if (field.type !== ThriftType.I32) return false;
        physical = reader.i32();
        return true;
      }
      case 2: {
        if (field.type !== ThriftType.I32) return false;
        typeLength = reader.i32();
        return true;
      }
      case 3: {
        if (field.type !== ThriftType.I32) return false;
        repetition = reader.i32();
        return true;
      }
      case 4: {
        if (field.type !== ThriftType.BINARY) return false;
        name = reader.string();
        return true;
      }
      case 5: {
        if (field.type !== ThriftType.I32) return false;
        numChildren = reader.i32();
        return true;
      }
      case 6: {
        if (field.type !== ThriftType.I32) return false;
        convertedType = reader.i32();
        return true;
      }
      case 7: {
        if (field.type !== ThriftType.I32) return false;
        scale = reader.i32();
        return true;
      }
      case 8: {
        if (field.type !== ThriftType.I32) return false;
        precision = reader.i32();
        return true;
      }
      case 10: {
        if (field.type !== ThriftType.STRUCT) return false;
        logical = decodeLogicalType(reader);
        return true;
      }
      default: {
        return false;
      }
    }
  });
  return {
    name,
    physical,
    typeLength,
    repetition,
    numChildren,
    convertedType,
    scale,
    precision,
    logical,
  };
}

function decodeColumnMetaData(reader: CompactReader, chunk: MutableChunk): void {
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        if (field.type !== ThriftType.I32) return false;
        chunk.physical = reader.i32();
        return true;
      }
      case 3: {
        if (field.type !== ThriftType.LIST) return false;
        const { elementType, size } = reader.listBegin();
        if (elementType !== ThriftType.BINARY) {
          // The header is already read, so the elements have to be consumed
          // either way; what they are not is a path.
          reader.skipElements(elementType, size);
          return true;
        }
        const path: string[] = [];
        for (let index = 0; index < size; index++) path.push(reader.string());
        chunk.path = path;
        return true;
      }
      case 4: {
        if (field.type !== ThriftType.I32) return false;
        chunk.codec = reader.i32();
        return true;
      }
      case 5: {
        if (field.type !== ThriftType.I64) return false;
        chunk.numValues = toCount(reader.i64(), "A column chunk's num_values");
        return true;
      }
      case 7: {
        if (field.type !== ThriftType.I64) return false;
        // Read, but never enforced: see `ColumnChunkInfo.totalCompressedSize`.
        // A value that is not a byte count makes the chunk unfetchable on its
        // own, which is a remote read's problem to raise where it needs one —
        // and no reason at all to refuse a file whose pages are right there.
        const size = reader.i64();
        chunk.totalCompressedSize =
          size >= 0n && size <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(size) : undefined;
        return true;
      }
      case 9: {
        if (field.type !== ThriftType.I64) return false;
        chunk.dataPageOffset = toCount(reader.i64(), "A column chunk's data_page_offset");
        return true;
      }
      case 11: {
        if (field.type !== ThriftType.I64) return false;
        chunk.dictionaryPageOffset = toCount(
          reader.i64(),
          "A column chunk's dictionary_page_offset",
        );
        return true;
      }
      default: {
        return false;
      }
    }
  });
}

interface MutableChunk {
  path: string[];
  physical?: number;
  codec: number;
  numValues: number;
  dataPageOffset: number;
  dictionaryPageOffset?: number;
  totalCompressedSize?: number;
}

function decodeColumnChunk(reader: CompactReader): ColumnChunkInfo {
  const chunk: MutableChunk = {
    path: [],
    codec: CompressionCodec.UNCOMPRESSED,
    numValues: 0,
    dataPageOffset: 0,
  };
  eachField(reader, (field) => {
    if (field.id !== 3 || field.type !== ThriftType.STRUCT) return false;
    decodeColumnMetaData(reader, chunk);
    return true;
  });
  return chunk;
}

function decodeRowGroup(reader: CompactReader): RowGroupInfo {
  const columns: ColumnChunkInfo[] = [];
  let numRows = 0;
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        if (field.type !== ThriftType.LIST) return false;
        const { elementType, size } = reader.listBegin();
        if (elementType !== ThriftType.STRUCT) {
          reader.skipElements(elementType, size);
          return true;
        }
        for (let index = 0; index < size; index++) columns.push(decodeColumnChunk(reader));
        return true;
      }
      case 3: {
        if (field.type !== ThriftType.I64) return false;
        numRows = toCount(reader.i64(), "A row group's num_rows");
        return true;
      }
      default: {
        return false;
      }
    }
  });
  return { columns, numRows };
}

/** Parses the `FileMetaData` footer struct. */
export function decodeFileMetadata(bytes: Uint8Array): FileMetadata {
  const reader = new CompactReader(new ByteReader(bytes));
  const schema: SchemaElement[] = [];
  const rowGroups: RowGroupInfo[] = [];
  let numRows = 0;
  let createdBy: string | undefined;

  eachField(reader, (field) => {
    switch (field.id) {
      case 2: {
        if (field.type !== ThriftType.LIST) return false;
        const { elementType, size } = reader.listBegin();
        if (elementType !== ThriftType.STRUCT) {
          reader.skipElements(elementType, size);
          return true;
        }
        for (let index = 0; index < size; index++) schema.push(decodeSchemaElement(reader));
        return true;
      }
      case 3: {
        if (field.type !== ThriftType.I64) return false;
        numRows = toCount(reader.i64(), "The footer's num_rows");
        return true;
      }
      case 4: {
        if (field.type !== ThriftType.LIST) return false;
        const { elementType, size } = reader.listBegin();
        if (elementType !== ThriftType.STRUCT) {
          reader.skipElements(elementType, size);
          return true;
        }
        for (let index = 0; index < size; index++) rowGroups.push(decodeRowGroup(reader));
        return true;
      }
      case 6: {
        if (field.type !== ThriftType.BINARY) return false;
        createdBy = reader.string();
        return true;
      }
      default: {
        return false;
      }
    }
  });

  return { schema, numRows, rowGroups, createdBy };
}

/**
 * Parses a `PageHeader` in place, leaving the reader positioned on the first
 * byte of the page body.
 */
export function decodePageHeader(input: ByteReader): PageHeaderInfo {
  const reader = new CompactReader(input);
  let pageType: number = PageType.DATA_PAGE;
  let uncompressedSize = -1;
  let compressedSize = -1;
  let numValues = 0;
  let encoding: number = Encoding.PLAIN;
  let definitionLevelEncoding: number = Encoding.RLE;

  // Under the same rule as the footer: a field is read only as the type it is
  // declared to be, so a header that gets one wrong describes a page with a
  // value missing rather than sending the reader off into the page body. Every
  // value below is checked before it is acted on — the sizes here, the count
  // and the encodings where the page is read.
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        if (field.type !== ThriftType.I32) return false;
        pageType = reader.i32();
        return true;
      }
      case 2: {
        if (field.type !== ThriftType.I32) return false;
        uncompressedSize = reader.i32();
        return true;
      }
      case 3: {
        if (field.type !== ThriftType.I32) return false;
        compressedSize = reader.i32();
        return true;
      }
      case 5: {
        if (field.type !== ThriftType.STRUCT) return false;
        eachField(reader, (inner) => {
          switch (inner.id) {
            case 1: {
              if (inner.type !== ThriftType.I32) return false;
              numValues = reader.i32();
              return true;
            }
            case 2: {
              if (inner.type !== ThriftType.I32) return false;
              encoding = reader.i32();
              return true;
            }
            case 3: {
              if (inner.type !== ThriftType.I32) return false;
              definitionLevelEncoding = reader.i32();
              return true;
            }
            default: {
              return false;
            }
          }
        });
        return true;
      }
      default: {
        return false;
      }
    }
  });

  // Both sizes are mandatory in the format, and a compressed page is unreadable
  // without the second one: refuse a header that leaves either out rather than
  // guess a length.
  if (compressedSize < 0 || uncompressedSize < 0) {
    throw malformed(`A page header at offset ${input.offset} declares no page size`);
  }
  return {
    pageType,
    compressedSize,
    uncompressedSize,
    numValues,
    encoding,
    definitionLevelEncoding,
  };
}
