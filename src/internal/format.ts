import { ByteReader } from "./bytes.ts";
import { CompactReader, CompactWriter, type ThriftField, ThriftType } from "./thrift.ts";
import { malformed } from "../error.ts";
import type { ColumnType } from "../types.ts";

/**
 * The subset of `parquet.thrift` enums this writer emits.
 *
 * @see https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
 */
export const PhysicalType: {
  readonly BOOLEAN: 0;
  readonly INT64: 2;
  readonly DOUBLE: 5;
  readonly BYTE_ARRAY: 6;
} = { BOOLEAN: 0, INT64: 2, DOUBLE: 5, BYTE_ARRAY: 6 } as const;

/**
 * `INT_64` is not written by tavolato but is accepted on the way in: it
 * annotates an `INT64` with exactly the domain an unannotated one already has,
 * so it carries no information the reader would have to drop.
 */
export const ConvertedType: {
  readonly UTF8: 0;
  readonly TIMESTAMP_MILLIS: 9;
  readonly INT_64: 18;
} = { UTF8: 0, TIMESTAMP_MILLIS: 9, INT_64: 18 } as const;

export const FieldRepetitionType: {
  readonly REQUIRED: 0;
  readonly OPTIONAL: 1;
  readonly REPEATED: 2;
} = { REQUIRED: 0, OPTIONAL: 1, REPEATED: 2 } as const;

export const Encoding: { readonly PLAIN: 0; readonly RLE: 3 } = { PLAIN: 0, RLE: 3 } as const;

export const CompressionCodec: { readonly UNCOMPRESSED: 0 } = { UNCOMPRESSED: 0 } as const;

export const PageType: { readonly DATA_PAGE: 0 } = { DATA_PAGE: 0 } as const;

/** The `LogicalType` union field ids tavolato recognises by name. */
export const LogicalTypeId: { readonly STRING: 1; readonly TIMESTAMP: 8 } = {
  STRING: 1,
  TIMESTAMP: 8,
} as const;

/** `TimeUnit` union field ids. */
export const TimeUnit: { readonly MILLIS: 1 } = { MILLIS: 1 } as const;

/** `PAR1`, the four magic bytes that open and close every Parquet file. */
export const MAGIC: Uint8Array = /* @__PURE__ */ new Uint8Array([0x50, 0x41, 0x52, 0x31]);

/**
 * A flat schema has exactly one optional level per column, so nullable columns
 * always use definition levels of width 1 and non-nullable columns write none
 * at all.
 */
export const MAX_DEFINITION_LEVEL_BIT_WIDTH: number = 1;

/** How each supported column type maps onto Parquet's physical/logical types. */
export interface PhysicalMapping {
  readonly physical: number;
  readonly convertedType?: number;
}

const MAPPINGS: Record<ColumnType, PhysicalMapping> = {
  string: { physical: PhysicalType.BYTE_ARRAY, convertedType: ConvertedType.UTF8 },
  f64: { physical: PhysicalType.DOUBLE },
  i64: { physical: PhysicalType.INT64 },
  bool: { physical: PhysicalType.BOOLEAN },
  timestamp: { physical: PhysicalType.INT64, convertedType: ConvertedType.TIMESTAMP_MILLIS },
};

export function physicalMapping(type: ColumnType): PhysicalMapping {
  return MAPPINGS[type];
}

/** Everything the footer needs to know about one column chunk. */
export interface ColumnChunkMeta {
  readonly name: string;
  readonly type: ColumnType;
  readonly optional: boolean;
  readonly numValues: number;
  readonly nullCount: number;
  /** Absolute file offset of the column chunk's first (and only) page header. */
  readonly dataPageOffset: number;
  /** Page header plus page body, in bytes. */
  readonly totalSize: number;
}

/** Everything the footer needs to know about one row group. */
export interface RowGroupMeta {
  readonly columns: readonly ColumnChunkMeta[];
  readonly numRows: number;
  readonly totalByteSize: number;
  readonly fileOffset: number;
}

/**
 * Serializes a v1 `PageHeader` wrapping a `DataPageHeader`.
 *
 * The optional `crc` field is deliberately not written: page checksums are
 * optional in the format, and omitting them keeps the core free of any
 * hashing dependency.
 */
export function encodeDataPageHeader(pageSize: number, numValues: number): Uint8Array {
  const writer = new CompactWriter();
  writer.structBegin();
  writer.fieldI32(1, PageType.DATA_PAGE);
  writer.fieldI32(2, pageSize); // uncompressed_page_size
  writer.fieldI32(3, pageSize); // compressed_page_size — UNCOMPRESSED, so identical
  writer.fieldStructBegin(5); // data_page_header
  writer.fieldI32(1, numValues);
  writer.fieldI32(2, Encoding.PLAIN);
  writer.fieldI32(3, Encoding.RLE); // definition_level_encoding
  writer.fieldI32(4, Encoding.RLE); // repetition_level_encoding
  writer.structEnd();
  writer.structEnd();
  return writer.toBytes();
}

function writeSchemaElement(writer: CompactWriter, column: SchemaColumnLike): void {
  const mapping = physicalMapping(column.type);
  writer.structBegin();
  writer.fieldI32(1, mapping.physical);
  writer.fieldI32(3, column.optional ? FieldRepetitionType.OPTIONAL : FieldRepetitionType.REQUIRED);
  writer.fieldString(4, column.name);
  if (mapping.convertedType !== undefined) writer.fieldI32(6, mapping.convertedType);

  // logicalType (field 10) — the modern annotation; ConvertedType above stays
  // for readers that predate it.
  if (column.type === "string") {
    writer.fieldStructBegin(10);
    writer.fieldStructBegin(1); // STRING
    writer.structEnd();
    writer.structEnd();
  } else if (column.type === "timestamp") {
    writer.fieldStructBegin(10);
    writer.fieldStructBegin(8); // TIMESTAMP
    // ConvertedType TIMESTAMP_MILLIS is defined as UTC-normalised, so the
    // logical annotation must say so too (LogicalTypes.md, backward compat).
    writer.fieldBool(1, true); // isAdjustedToUTC
    writer.fieldStructBegin(2); // unit
    writer.fieldStructBegin(1); // MILLIS
    writer.structEnd();
    writer.structEnd();
    writer.structEnd();
    writer.structEnd();
  }
  writer.structEnd();
}

/** The shape `writeSchemaElement` needs; satisfied by both schema and chunk metadata. */
export interface SchemaColumnLike {
  readonly name: string;
  readonly type: ColumnType;
  readonly optional: boolean;
}

function writeColumnChunk(writer: CompactWriter, chunk: ColumnChunkMeta): void {
  writer.structBegin();
  // file_offset is deprecated; the format tells writers to store 0 when no
  // ColumnMetaData is written outside the footer, which is our case.
  writer.fieldI64(2, 0n);
  writer.fieldStructBegin(3); // meta_data
  writer.fieldI32(1, physicalMapping(chunk.type).physical);
  const encodings = chunk.optional ? [Encoding.RLE, Encoding.PLAIN] : [Encoding.PLAIN];
  writer.fieldListBegin(2, ThriftType.I32, encodings.length);
  for (const encoding of encodings) writer.elementI32(encoding);
  writer.fieldListBegin(3, ThriftType.BINARY, 1); // path_in_schema
  writer.elementString(chunk.name);
  writer.fieldI32(4, CompressionCodec.UNCOMPRESSED);
  writer.fieldI64(5, BigInt(chunk.numValues));
  writer.fieldI64(6, BigInt(chunk.totalSize)); // total_uncompressed_size
  writer.fieldI64(7, BigInt(chunk.totalSize)); // total_compressed_size
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
  writer.fieldI64(6, BigInt(group.totalByteSize)); // total_compressed_size
  writer.structEnd();
}

/** Serializes the `FileMetaData` footer struct. */
export function encodeFileMetadata(
  columns: readonly SchemaColumnLike[],
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

/** Names for the enum values the reader may have to reject, so errors can say what it found. */
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

const CONVERTED_TYPE_NAMES = [
  "UTF8",
  "MAP",
  "MAP_KEY_VALUE",
  "LIST",
  "ENUM",
  "DECIMAL",
  "DATE",
  "TIME_MILLIS",
  "TIME_MICROS",
  "TIMESTAMP_MILLIS",
  "TIMESTAMP_MICROS",
  "UINT_8",
  "UINT_16",
  "UINT_32",
  "UINT_64",
  "INT_8",
  "INT_16",
  "INT_32",
  "INT_64",
  "JSON",
  "BSON",
  "INTERVAL",
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
  "",
  "INTEGER",
  "UNKNOWN",
  "JSON",
  "BSON",
  "UUID",
  "FLOAT16",
  "VARIANT",
  "GEOMETRY",
  "GEOGRAPHY",
];

const TIME_UNIT_NAMES = ["", "MILLIS", "MICROS", "NANOS"];

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

export function convertedTypeName(id: number | undefined): string {
  return nameOf(CONVERTED_TYPE_NAMES, id, "converted type");
}

export function logicalTypeName(id: number | undefined): string {
  return nameOf(LOGICAL_TYPE_NAMES, id, "logical type");
}

export function timeUnitName(id: number | undefined): string {
  return nameOf(TIME_UNIT_NAMES, id, "time unit");
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

/** A `LogicalType` union, reduced to the two facts the reader acts on. */
export interface LogicalType {
  /** The union's field id; see {@link LogicalTypeId}. */
  readonly id: number;
  /** For `TIMESTAMP`, the `TimeUnit` union field id; see {@link TimeUnit}. */
  readonly unit?: number;
}

/** One `SchemaElement`, as far as the reader inspects it. */
export interface SchemaElement {
  readonly name: string;
  readonly physical?: number;
  readonly repetition?: number;
  readonly numChildren: number;
  readonly convertedType?: number;
  readonly logical?: LogicalType;
}

/** One `ColumnChunk` plus its inlined `ColumnMetaData`. */
export interface ColumnChunkInfo {
  readonly path: readonly string[];
  readonly physical?: number;
  readonly codec: number;
  readonly numValues: number;
  readonly dataPageOffset: number;
  readonly dictionaryPageOffset?: number;
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
  readonly compressedSize: number;
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

function decodeTimeUnit(reader: CompactReader): number | undefined {
  let unit: number | undefined;
  eachField(reader, (field) => {
    unit = field.id;
    return false; // the union's payload is an empty struct; skipping it reads the stop byte
  });
  return unit;
}

function decodeLogicalType(reader: CompactReader): LogicalType {
  let id = 0;
  let unit: number | undefined;
  eachField(reader, (field) => {
    id = field.id;
    if (field.id !== LogicalTypeId.TIMESTAMP || field.type !== ThriftType.STRUCT) return false;
    // TimestampType { 1: isAdjustedToUTC, 2: unit }. The UTC flag is advisory
    // here: the value is epoch milliseconds either way, and that is what a
    // `Date` holds.
    eachField(reader, (inner) => {
      if (inner.id !== 2 || inner.type !== ThriftType.STRUCT) return false;
      unit = decodeTimeUnit(reader);
      return true;
    });
    return true;
  });
  return { id, unit };
}

function decodeSchemaElement(reader: CompactReader): SchemaElement {
  let name = "";
  let physical: number | undefined;
  let repetition: number | undefined;
  let numChildren = 0;
  let convertedType: number | undefined;
  let logical: LogicalType | undefined;
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        physical = reader.i32();
        return true;
      }
      case 3: {
        repetition = reader.i32();
        return true;
      }
      case 4: {
        name = reader.string();
        return true;
      }
      case 5: {
        numChildren = reader.i32();
        return true;
      }
      case 6: {
        convertedType = reader.i32();
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
  return { name, physical, repetition, numChildren, convertedType, logical };
}

function decodeColumnMetaData(reader: CompactReader, chunk: MutableChunk): void {
  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        chunk.physical = reader.i32();
        return true;
      }
      case 3: {
        const { size } = reader.listBegin();
        const path: string[] = [];
        for (let index = 0; index < size; index++) path.push(reader.string());
        chunk.path = path;
        return true;
      }
      case 4: {
        chunk.codec = reader.i32();
        return true;
      }
      case 5: {
        chunk.numValues = toCount(reader.i64(), "A column chunk's num_values");
        return true;
      }
      case 9: {
        chunk.dataPageOffset = toCount(reader.i64(), "A column chunk's data_page_offset");
        return true;
      }
      case 11: {
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
        const { size } = reader.listBegin();
        for (let index = 0; index < size; index++) columns.push(decodeColumnChunk(reader));
        return true;
      }
      case 3: {
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
        const { size } = reader.listBegin();
        for (let index = 0; index < size; index++) schema.push(decodeSchemaElement(reader));
        return true;
      }
      case 3: {
        numRows = toCount(reader.i64(), "The footer's num_rows");
        return true;
      }
      case 4: {
        const { size } = reader.listBegin();
        for (let index = 0; index < size; index++) rowGroups.push(decodeRowGroup(reader));
        return true;
      }
      case 6: {
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
  let compressedSize = -1;
  let numValues = 0;
  let encoding: number = Encoding.PLAIN;
  let definitionLevelEncoding: number = Encoding.RLE;

  eachField(reader, (field) => {
    switch (field.id) {
      case 1: {
        pageType = reader.i32();
        return true;
      }
      case 3: {
        compressedSize = reader.i32();
        return true;
      }
      case 5: {
        if (field.type !== ThriftType.STRUCT) return false;
        eachField(reader, (inner) => {
          switch (inner.id) {
            case 1: {
              numValues = reader.i32();
              return true;
            }
            case 2: {
              encoding = reader.i32();
              return true;
            }
            case 3: {
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

  if (compressedSize < 0) {
    throw malformed(`A page header at offset ${input.offset} declares no page size`);
  }
  return { pageType, compressedSize, numValues, encoding, definitionLevelEncoding };
}
