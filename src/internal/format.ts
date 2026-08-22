import { CompactWriter, ThriftType } from "./thrift.ts";
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

export const ConvertedType: { readonly UTF8: 0; readonly TIMESTAMP_MILLIS: 9 } = {
  UTF8: 0,
  TIMESTAMP_MILLIS: 9,
} as const;

export const FieldRepetitionType: { readonly REQUIRED: 0; readonly OPTIONAL: 1 } = {
  REQUIRED: 0,
  OPTIONAL: 1,
} as const;

export const Encoding: { readonly PLAIN: 0; readonly RLE: 3 } = { PLAIN: 0, RLE: 3 } as const;

export const CompressionCodec: { readonly UNCOMPRESSED: 0 } = { UNCOMPRESSED: 0 } as const;

export const PageType: { readonly DATA_PAGE: 0 } = { DATA_PAGE: 0 } as const;

/** `PAR1`, the four magic bytes that open and close every Parquet file. */
export const MAGIC: Uint8Array = /* @__PURE__ */ new Uint8Array([0x50, 0x41, 0x52, 0x31]);

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
