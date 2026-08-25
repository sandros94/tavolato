import { describe, expect, it } from "vitest";
import {
  createWriter,
  decimal,
  defineSchema,
  isTavolatoError,
  readParquet,
  readRowGroups,
  readSchema,
  TavolatoError,
  timestamp,
  uuid,
} from "../src/index.ts";
import type { ReadRow } from "../src/index.ts";
import { ByteWriter } from "../src/internal/bytes.ts";
import {
  CompressionCodec,
  encodeDataPageHeader,
  encodeFileMetadata,
  Encoding,
  PageType,
  PhysicalType,
  type RowGroupMeta,
  snapshotColumn,
} from "../src/internal/format.ts";
import { CompactWriter, ThriftType } from "../src/internal/thrift.ts";
import { plainBody, sealFile, startFile, withPhysicalType } from "./_build.ts";
import { expectError } from "./_errors.ts";
import { sync } from "./_sync.ts";

/**
 * The reader's failure surface: a malformed file must fail with a typed error
 * at the byte that gave it away, and a file that is real Parquet but outside
 * the subset must say which feature it tripped over.
 *
 * Nothing here may crash ungracefully — no `RangeError`, no `TypeError`, no
 * loop that never ends.
 */

/** The logical types the sample file's annotated columns need. */
const TYPES = [decimal({ precision: 9, scale: 2 }), uuid()];

/** Reads with the sample's column types registered, as its own writer would. */
function read(bytes: Uint8Array): unknown {
  return readParquet(bytes, { types: TYPES });
}

/**
 * A small but complete file: two row groups, nulls, and one column of every
 * physical type the writer can emit — the fixed-width ones and their
 * annotations included, so the sweeps below cover the annotation decoder too.
 */
function sample(): Uint8Array {
  const schema = defineSchema({
    s: { type: "string", optional: true },
    f: { type: "f64" },
    g: { type: "f32" },
    i: { type: "i64" },
    n: { type: "i32" },
    b: { type: "bool", optional: true },
    t: { type: "timestamp" },
    p: { type: TYPES[0] },
    u: { type: TYPES[1], optional: true },
  });
  const writer = createWriter(schema, { rowGroupSize: 2 });
  const id = "b3f2c1a0-1111-4222-8333-444455556666";
  writer.append({ s: "alpha", f: 1.5, g: 0.5, i: 1n, n: 1, b: true, t: 0, p: "1.25", u: id });
  writer.append({ s: null, f: -0.25, g: -1, i: 2n, n: -2, b: null, t: 1000, p: "-0.01", u: null });
  writer.append({ s: "gamma", f: 0, g: 2, i: 3n, n: 3, b: false, t: 2000, p: "0.00", u: id });
  return sync(writer.finish());
}

/** The smallest file the writer emits: one i64 column, one row. */
function minimal(): Uint8Array {
  const writer = createWriter(defineSchema({ n: { type: "i64" } }));
  writer.append({ n: 1n });
  return sync(writer.finish());
}

/** A three-null optional i64 page carrying caller-spelled definition-level bytes. */
function optionalLevelsFile(levelBytes: Uint8Array): Uint8Array {
  const out = startFile();
  const dataPageOffset = out.length;
  const body = new ByteWriter();
  body.u32(levelBytes.length);
  body.raw(levelBytes);
  const bodyBytes = body.toBytes();
  const header = encodeDataPageHeader(bodyBytes.length, bodyBytes.length, 3);
  out.raw(header);
  out.raw(bodyBytes);
  const size = header.length + bodyBytes.length;
  const group: RowGroupMeta = {
    columns: [
      {
        name: "n",
        physical: "i64",
        optional: true,
        codec: CompressionCodec.UNCOMPRESSED,
        numValues: 3,
        nullCount: 3,
        dataPageOffset,
        totalUncompressedSize: size,
        totalCompressedSize: size,
      },
    ],
    numRows: 3,
    totalByteSize: size,
    totalCompressedSize: size,
    fileOffset: dataPageOffset,
  };
  return sealFile(
    out,
    encodeFileMetadata(
      [snapshotColumn({ name: "n", type: "i64", optional: true })],
      [group],
      3,
      "probe",
    ),
  );
}

/**
 * Overwrites one byte, first asserting what was there. The assertion is the
 * point: if the writer's layout ever moves, these tests fail loudly instead of
 * quietly patching something harmless.
 */
function patch(bytes: Uint8Array, offset: number, from: number, to: number): Uint8Array {
  expect(bytes[offset]).toBe(from);
  const copy = bytes.slice();
  copy[offset] = to;
  return copy;
}

/** Runs `read`, requiring that whatever it throws is a `TavolatoError`. */
function caught(read: () => unknown): TavolatoError | undefined {
  try {
    read();
    return undefined;
  } catch (error) {
    if (!(error instanceof TavolatoError)) throw error;
    return error;
  }
}

describe("file envelope", () => {
  it("rejects an empty input", () => {
    expectError("ERR_READ_MALFORMED", () => readParquet(new Uint8Array()));
  });

  it("rejects an input too short to hold an envelope", () => {
    for (const length of [1, 4, 11, 12]) {
      expectError("ERR_READ_MALFORMED", () => readParquet(new Uint8Array(length)));
    }
  });

  it("rejects a missing leading magic", () => {
    const bytes = minimal();
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(patch(bytes, 0, 0x50, 0x51)));
    expect(error.message).toContain("PAR1");
  });

  it("rejects a missing trailing magic", () => {
    const bytes = minimal();
    expectError("ERR_READ_MALFORMED", () =>
      readParquet(patch(bytes, bytes.length - 1, 0x31, 0x32)),
    );
  });

  it("rejects a footer length that does not fit the file", () => {
    const bytes = minimal();
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    for (const length of [0, 0xff_ff_ff_ff, bytes.length]) {
      const copy = bytes.slice();
      new DataView(copy.buffer).setUint32(copy.length - 8, length, true);
      const error = expectError("ERR_READ_MALFORMED", () => readParquet(copy));
      expect(error.message).toContain(String(length));
    }
    // The untouched file still reads, so the patches above are what broke it.
    expect(view.getUint32(bytes.length - 8, true)).toBeGreaterThan(0);
    expect(readParquet(bytes).rows).toEqual([{ n: 1n }]);
  });

  it("rejects bytes after the compact FileMetaData struct inside its declared slice", () => {
    const bytes = minimal();
    const suffix = bytes.length - 8;
    const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(
      suffix,
      true,
    );
    const copy = new Uint8Array(bytes.length + 1);
    copy.set(bytes.subarray(0, suffix), 0);
    copy[suffix] = 0xff;
    new DataView(copy.buffer).setUint32(suffix + 1, length + 1, true);
    copy.set(bytes.subarray(suffix + 4), suffix + 5);

    const error = expectError("ERR_READ_MALFORMED", () => readParquet(copy));
    expect(error.message).toContain("FileMetaData");
    expect(error.message).toContain("trailing");
    expectError("ERR_READ_MALFORMED", () => readSchema(copy));
  });
});

describe("uncompressed page sizes", () => {
  /** Locates the fixed header prefix tavolato writes for one required i64. */
  function pageStarts(bytes: Uint8Array): number[] {
    const header = [0x15, 0x00, 0x15, 0x10, 0x15, 0x10, 0x2c];
    const starts: number[] = [];
    for (let offset = 0; offset + header.length <= bytes.length; offset++) {
      if (header.every((byte, index) => bytes[offset + index] === byte)) starts.push(offset);
    }
    return starts;
  }

  /** Inflates one of an uncompressed page's two size declarations from eight to nine. */
  function contradictPageSize(
    bytes: Uint8Array,
    page: number,
    pages: number,
    size: "uncompressed" | "compressed" = "uncompressed",
  ): Uint8Array {
    const starts = pageStarts(bytes);
    expect(starts).toHaveLength(pages);
    return patch(bytes, starts[page] + (size === "uncompressed" ? 3 : 5), 0x10, 0x12);
  }

  it("refuses a page whose two sizes contradict its UNCOMPRESSED codec", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(contradictPageSize(minimal(), 0, 1)),
    );
    expect(error.column).toBe("n");
    expect(error.message).toContain("9 uncompressed bytes");
    expect(error.message).toContain("8 compressed bytes");
  });

  it("refuses the reverse size contradiction too", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(contradictPageSize(minimal(), 0, 1, "compressed")),
    );
    expect(error.column).toBe("n");
    expect(error.message).toContain("Truncated input");
  });

  it("does not inspect a contradictory page projected away", () => {
    const writer = createWriter(defineSchema({ kept: { type: "i64" }, broken: { type: "i64" } }));
    writer.append({ kept: 1n, broken: 2n });
    const bytes = contradictPageSize(sync(writer.finish()), 1, 2);

    expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(readParquet(bytes, { columns: ["kept"] }).rows).toEqual([{ kept: 1n }]);
  });
});

describe("definition-level boundaries", () => {
  it("accepts one final padded bit-packed group", () => {
    expect(readParquet(optionalLevelsFile(new Uint8Array([0x03, 0x00]))).rows).toEqual([
      { n: null },
      { n: null },
      { n: null },
    ]);
  });

  it("accepts a final bit-packed run padded to DuckDB's 32-group block", () => {
    const levels = new Uint8Array([0x41, ...new Uint8Array(32)]);
    expect(readParquet(optionalLevelsFile(levels)).rows).toEqual([
      { n: null },
      { n: null },
      { n: null },
    ]);
  });

  it("refuses a trailing encoded run or byte inside the declared level substream", () => {
    for (const levels of [
      new Uint8Array([0x03, 0x00, 0x10, 0x00]),
      new Uint8Array([0x03, 0x00, 0xff]),
    ]) {
      const error = expectError("ERR_READ_MALFORMED", () =>
        readParquet(optionalLevelsFile(levels)),
      );
      expect(error.column).toBe("n");
    }
  });

  it("refuses an RLE run larger than the page's value count", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(optionalLevelsFile(new Uint8Array([0x10, 0x00]))),
    );
    expect(error.message).toContain("8 values");
    expect(error.column).toBe("n");
  });
});

describe("unsupported features", () => {
  /*
   * The writer's first data page header starts at byte 4 and, for a one row
   * i64 column, is exactly:
   *
   *   15 00  PageHeader.type          = DATA_PAGE
   *   15 10  uncompressed_page_size   = 8
   *   15 10  compressed_page_size     = 8
   *   2c     DataPageHeader (struct)
   *   15 02    num_values             = 1
   *   15 00    encoding               = PLAIN
   *   15 06    definition_level_enc   = RLE
   *   15 06    repetition_level_enc   = RLE
   *   00 00  two stop fields
   */
  const PAGE_TYPE_VALUE = 5;
  const ENCODING_VALUE = 14;

  it("names a data page v2", () => {
    // zigzag(3) = 6 → DATA_PAGE_V2.
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), PAGE_TYPE_VALUE, 0x00, 0x06)),
    );
    expect(error.message).toContain("DATA_PAGE_V2");
    expect(error.column).toBe("n");
  });

  it("names a dictionary page found where a data page was promised", () => {
    // zigzag(2) = 4 → DICTIONARY_PAGE.
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), PAGE_TYPE_VALUE, 0x00, 0x04)),
    );
    expect(error.message).toContain("DICTIONARY_PAGE");
  });

  it("names the encoding a page claims to use", () => {
    // zigzag(8) = 16 → RLE_DICTIONARY.
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), ENCODING_VALUE, 0x00, 0x10)),
    );
    expect(error.message).toContain("RLE_DICTIONARY");
    expect(error.column).toBe("n");
  });

  it("names the encoding of a nullable column's definition levels", () => {
    const schema = defineSchema({ n: { type: "i64", optional: true } });
    const writer = createWriter(schema);
    writer.append({ n: 1n });
    // Same layout, with the level stream adding its 4 byte length prefix and
    // two bytes of RLE: the page is 14 bytes, zigzag(14) = 28, still one byte,
    // so definition_level_encoding is still the value at offset 16.
    const bytes = patch(sync(writer.finish()), 7, 0x1c, 0x1c);
    const error = expectError(
      "ERR_READ_UNSUPPORTED",
      () => readParquet(patch(bytes, 16, 0x06, 0x08)), // zigzag(4) = 8 → BIT_PACKED
    );
    expect(error.message).toContain("BIT_PACKED");
    expect(error.column).toBe("n");
  });

  it("always says that tavolato only reads what it writes", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(patch(minimal(), ENCODING_VALUE, 0x00, 0x10)),
    );
    expect(error.message).toContain("tavolato only reads the files it writes");
  });
});

describe("types the reader refuses outright", () => {
  it("refuses INT96 permanently, and says why", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(withPhysicalType(minimal(), 3)),
    );
    expect(error.message).toContain("INT96");
    expect(error.message).toContain("deprecated");
    // No remedy: this one does not lift for any option.
    expect(error.message).not.toContain("ReadOptions");
    expect(error.column).toBe("n");
  });

  it("refuses a physical type Parquet does not define", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(withPhysicalType(minimal(), 9)),
    );
    expect(error.message).toContain("9");
    expect(error.column).toBe("n");
  });

  it("refuses a FIXED_LEN_BYTE_ARRAY with no type_length", () => {
    // Physical type changed to FIXED_LEN_BYTE_ARRAY, but the element carries no
    // width, so the column's values have no length at all.
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(withPhysicalType(minimal(), 7)),
    );
    expect(error.message).toContain("type_length");
    expect(error.column).toBe("n");
  });

  it("names the annotation, with its parameters, and the remedy", () => {
    const schema = defineSchema({ p: { type: decimal({ precision: 9, scale: 2 }) } });
    const writer = createWriter(schema);
    writer.append({ p: "1.25" });
    const bytes = sync(writer.finish());

    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(error.message).toContain("DECIMAL(precision=9, scale=2)");
    expect(error.message).toContain("INT32");
    expect(error.message).toContain("pass a matching type in ReadOptions.types");
    expect(error.column).toBe("p");

    // A type that claims a *different* decimal does not claim this one.
    expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(bytes, { types: [decimal({ precision: 9, scale: 4 })] }),
    );
  });

  it("refuses a millisecond timestamp no Date can hold, and names the remedy", () => {
    // Written through the adapter, which stamps exactly the annotation the
    // built-in `timestamp` column type carries — so the count comes back to a
    // reader that would have to hand it over as an Invalid Date.
    const millis = timestamp({ unit: "millis", isAdjustedToUTC: true });
    const writer = createWriter(defineSchema({ t: { type: millis } }));
    writer.append({ t: 9_000_000_000_000_000n });
    const bytes = sync(writer.finish());

    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(bytes));
    expect(error.column).toBe("t");
    expect(error.message).toContain("Date");
    expect(error.message).toContain("9000000000000000");
    expect(error.message).toContain("ReadOptions.types");

    // The remedy works: the adapter claims the column and hands back the count.
    expect(readParquet(bytes, { types: [millis] }).rows[0].t).toBe(9_000_000_000_000_000n);
  });

  it("names a fixed-width column by the width it declares", () => {
    const writer = createWriter(defineSchema({ u: { type: uuid() } }));
    writer.append({ u: "b3f2c1a0-1111-4222-8333-444455556666" });
    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(sync(writer.finish())));
    expect(error.message).toContain("FIXED_LEN_BYTE_ARRAY(16) annotated UUID");
  });
});

describe("truncation", () => {
  it("rejects every truncation of a real file", () => {
    // Every prefix loses the trailing magic, so every one of them must be
    // refused — and must be refused quickly: the suite's timeout is the proof
    // that nothing loops.
    const bytes = sample();
    expect(bytes.length).toBeGreaterThan(200);
    const codes = new Set<string>();
    const survived: number[] = [];
    for (let length = 0; length < bytes.length; length++) {
      const error = caught(() => read(bytes.subarray(0, length)));
      if (error === undefined) survived.push(length);
      else codes.add(error.code);
    }
    expect(survived).toEqual([]);
    expect([...codes]).toEqual(["ERR_READ_MALFORMED"]);
  });

  it("rejects a file whose middle has been cut out", () => {
    // Envelope intact, page bytes gone: the footer's offsets now point past
    // the end, or into the wrong bytes.
    const bytes = sample();
    for (let removed = 1; removed < bytes.length - 20; removed += 3) {
      const copy = new Uint8Array(bytes.length - removed);
      copy.set(bytes.subarray(0, 8), 0);
      copy.set(bytes.subarray(8 + removed), 8);
      expect(caught(() => read(copy))).toBeInstanceOf(TavolatoError);
    }
  });

  it("survives every single-byte corruption of a real file", () => {
    // Not every flip is detectable — some land in a string's bytes or a value —
    // so the contract is weaker but stricter in kind: read it, or throw a
    // TavolatoError. Never a RangeError, never a hang.
    const bytes = sample();
    let thrown = 0;
    for (let offset = 0; offset < bytes.length; offset++) {
      for (const mask of [0x01, 0x80, 0xff]) {
        const copy = bytes.slice();
        copy[offset] ^= mask;
        if (caught(() => read(copy)) !== undefined) thrown++;
      }
    }
    // Most corruptions are structural and must be caught, not shrugged off.
    expect(thrown).toBeGreaterThan(bytes.length);
  });

  it("survives every single-byte corruption with no column types registered", () => {
    // The same sweep with the annotated columns unclaimed: every read now ends
    // in a refusal rather than a value, and `caught` still insists that every
    // one of them is a TavolatoError.
    const bytes = sample();
    const codes = new Set<string>();
    for (let offset = 0; offset < bytes.length; offset++) {
      for (const mask of [0x01, 0xff]) {
        const copy = bytes.slice();
        copy[offset] ^= mask;
        const error = caught(() => readParquet(copy));
        if (error !== undefined) codes.add(error.code);
      }
    }
    expect([...codes].sort()).toEqual(["ERR_READ_MALFORMED", "ERR_READ_UNSUPPORTED"]);
  });
});

/**
 * A footer whose fields are not the Thrift types `parquet.thrift` declares.
 *
 * Every compact type carries its payload differently — a varint, a length and
 * then bytes, a struct that ends in a stop byte — so a field claimed without
 * being read the way it was written leaves the rest of the footer misaligned,
 * and the failure surfaces fields later, at an offset that means nothing to
 * anyone. The reader claims a field only when its type is the declared one and
 * skips it otherwise: the file is still refused, but by the check that owns the
 * missing value, with the column named.
 */
describe("a footer whose fields are the wrong Thrift type", () => {
  type RequiredField =
    | "FileMetaData.version"
    | "FileMetaData.schema"
    | "FileMetaData.num_rows"
    | "FileMetaData.row_groups"
    | "SchemaElement.name"
    | "RowGroup.columns"
    | "RowGroup.total_byte_size"
    | "RowGroup.num_rows"
    | "ColumnChunk.file_offset"
    | "ColumnMetaData.type"
    | "ColumnMetaData.encodings"
    | "ColumnMetaData.path_in_schema"
    | "ColumnMetaData.codec"
    | "ColumnMetaData.num_values"
    | "ColumnMetaData.total_uncompressed_size"
    | "ColumnMetaData.total_compressed_size"
    | "ColumnMetaData.data_page_offset"
    | "PageHeader.type"
    | "PageHeader.uncompressed_page_size"
    | "PageHeader.compressed_page_size"
    | "PageHeader.data_page_header"
    | "DataPageHeader.num_values"
    | "DataPageHeader.encoding"
    | "DataPageHeader.definition_level_encoding"
    | "DataPageHeader.repetition_level_encoding";

  /** Writes one `7n` page whose header is spelled out by `header`, wrong types and all. */
  function writeDoctoredPage(
    out: ByteWriter,
    header: (writer: CompactWriter) => void,
    trailingBodyByte = false,
  ): { readonly uncompressedSize: number; readonly compressedSize: number } {
    const writer = new CompactWriter();
    writer.structBegin();
    header(writer);
    writer.structEnd();
    const encoded = writer.toBytes();
    const body = plainBody([7n]);
    out.raw(encoded);
    out.raw(body);
    if (trailingBodyByte) out.u8(0xff);
    const size = encoded.length + body.length + (trailingBodyByte ? 1 : 0);
    return { uncompressedSize: size, compressedSize: size };
  }

  /** The `DataPageHeader` a correct page carries, for a header being doctored elsewhere. */
  function dataPageHeader(writer: CompactWriter, numValues: (writer: CompactWriter) => void): void {
    writer.fieldStructBegin(5);
    numValues(writer);
    writer.fieldI32(2, Encoding.PLAIN);
    writer.fieldI32(3, Encoding.RLE); // definition_level_encoding
    writer.fieldI32(4, Encoding.RLE); // repetition_level_encoding
    writer.structEnd();
  }

  /**
   * A one row `INT64` file, written field by field so that a single field can
   * be given the wrong type. Each hook replaces exactly the field it names.
   */
  function handBuilt(
    doctored: {
      physical?: number;
      typeLength?: (writer: CompactWriter) => void;
      path?: (writer: CompactWriter) => void;
      codec?: (writer: CompactWriter) => void;
      numValues?: (writer: CompactWriter) => void;
      fileOffset?: bigint;
      fileOffsetAfterMetadata?: boolean;
      pathAfterCounts?: boolean;
      pageHeader?: (writer: CompactWriter) => void;
      required?: { readonly field: RequiredField; readonly mode: "missing" | "wrong" };
      wrongListElement?:
        | "FileMetaData.schema"
        | "FileMetaData.row_groups"
        | "RowGroup.columns"
        | "ColumnMetaData.encodings";
      version?: number;
      inlineMetadata?: boolean;
      encryptedMetadata?: boolean;
      cryptoMetadata?: boolean;
      offsetIndexOffset?: boolean;
      createdBy?: boolean;
      unknownOptional?: boolean;
      bodyTrailingByte?: boolean;
      totalCompressedSizeDelta?: number;
      totalUncompressedSizeDelta?: number;
      rowGroupTotalSizeDelta?: number;
    } = {},
  ): Uint8Array {
    const required = (
      writer: CompactWriter,
      field: RequiredField,
      id: number,
      expected: number,
      valid: () => void,
    ): void => {
      if (doctored.required?.field !== field) {
        valid();
        return;
      }
      if (doctored.required.mode === "missing") return;
      if (expected === ThriftType.BINARY) writer.fieldI32(id, 0);
      else writer.fieldString(id, "wrong");
    };

    const out = startFile();
    const dataPageOffset = out.length;
    const page =
      doctored.pageHeader === undefined
        ? writeDoctoredPage(
            out,
            (writer) => {
              required(writer, "PageHeader.type", 1, ThriftType.I32, () =>
                writer.fieldI32(1, PageType.DATA_PAGE),
              );
              const bodySize = doctored.bodyTrailingByte ? 9 : 8;
              required(writer, "PageHeader.uncompressed_page_size", 2, ThriftType.I32, () =>
                writer.fieldI32(2, bodySize),
              );
              required(writer, "PageHeader.compressed_page_size", 3, ThriftType.I32, () =>
                writer.fieldI32(3, bodySize),
              );
              required(writer, "PageHeader.data_page_header", 5, ThriftType.STRUCT, () => {
                writer.fieldStructBegin(5);
                required(writer, "DataPageHeader.num_values", 1, ThriftType.I32, () =>
                  writer.fieldI32(1, 1),
                );
                required(writer, "DataPageHeader.encoding", 2, ThriftType.I32, () =>
                  writer.fieldI32(2, Encoding.PLAIN),
                );
                required(
                  writer,
                  "DataPageHeader.definition_level_encoding",
                  3,
                  ThriftType.I32,
                  () => writer.fieldI32(3, Encoding.RLE),
                );
                required(
                  writer,
                  "DataPageHeader.repetition_level_encoding",
                  4,
                  ThriftType.I32,
                  () => writer.fieldI32(4, Encoding.RLE),
                );
                if (doctored.unknownOptional) writer.fieldString(99, "future");
                writer.structEnd();
              });
              if (doctored.unknownOptional) writer.fieldString(99, "future");
            },
            doctored.bodyTrailingByte,
          )
        : writeDoctoredPage(out, doctored.pageHeader);

    const writer = new CompactWriter();
    writer.structBegin(); // FileMetaData
    required(writer, "FileMetaData.version", 1, ThriftType.I32, () =>
      writer.fieldI32(1, doctored.version ?? 1),
    );
    required(writer, "FileMetaData.schema", 2, ThriftType.LIST, () => {
      if (doctored.wrongListElement === "FileMetaData.schema") {
        writer.fieldListBegin(2, ThriftType.I32, 1);
        writer.elementI32(0);
        return;
      }
      writer.fieldListBegin(2, ThriftType.STRUCT, 2);
      writer.structBegin(); // the root group
      writer.fieldString(4, "schema");
      writer.fieldI32(5, 1);
      if (doctored.unknownOptional) writer.fieldString(99, "future");
      writer.structEnd();
      writer.structBegin(); // the one leaf
      writer.fieldI32(1, doctored.physical ?? PhysicalType.INT64);
      doctored.typeLength?.(writer);
      writer.fieldI32(3, 0); // REQUIRED
      required(writer, "SchemaElement.name", 4, ThriftType.BINARY, () =>
        writer.fieldString(4, "n"),
      );
      if (doctored.unknownOptional) writer.fieldString(99, "future");
      writer.structEnd();
    });
    required(writer, "FileMetaData.num_rows", 3, ThriftType.I64, () => writer.fieldI64(3, 1n));
    required(writer, "FileMetaData.row_groups", 4, ThriftType.LIST, () => {
      if (doctored.wrongListElement === "FileMetaData.row_groups") {
        writer.fieldListBegin(4, ThriftType.I32, 1);
        writer.elementI32(0);
        return;
      }
      writer.fieldListBegin(4, ThriftType.STRUCT, 1);
      writer.structBegin(); // the one row group
      required(writer, "RowGroup.columns", 1, ThriftType.LIST, () => {
        if (doctored.wrongListElement === "RowGroup.columns") {
          writer.fieldListBegin(1, ThriftType.I32, 1);
          writer.elementI32(0);
          return;
        }
        writer.fieldListBegin(1, ThriftType.STRUCT, 1);
        writer.structBegin(); // the one column chunk
        const writeFileOffset = (): void =>
          required(writer, "ColumnChunk.file_offset", 2, ThriftType.I64, () =>
            writer.fieldI64(2, doctored.fileOffset ?? 0n),
          );
        if (!doctored.fileOffsetAfterMetadata) writeFileOffset();
        if (doctored.inlineMetadata !== false) {
          writer.fieldStructBegin(3); // meta_data
          required(writer, "ColumnMetaData.type", 1, ThriftType.I32, () =>
            writer.fieldI32(1, PhysicalType.INT64),
          );
          required(writer, "ColumnMetaData.encodings", 2, ThriftType.LIST, () => {
            if (doctored.wrongListElement === "ColumnMetaData.encodings") {
              writer.fieldListBegin(2, ThriftType.BINARY, 1);
              writer.elementString("plain");
              return;
            }
            writer.fieldListBegin(2, ThriftType.I32, 1);
            writer.elementI32(Encoding.PLAIN);
          });
          const writePath = (): void => {
            if (doctored.path === undefined) {
              required(writer, "ColumnMetaData.path_in_schema", 3, ThriftType.LIST, () => {
                writer.fieldListBegin(3, ThriftType.BINARY, 1);
                writer.elementString("n");
              });
            } else {
              doctored.path(writer);
            }
          };
          if (!doctored.pathAfterCounts) writePath();
          if (doctored.codec === undefined) {
            required(writer, "ColumnMetaData.codec", 4, ThriftType.I32, () =>
              writer.fieldI32(4, 0),
            );
          } else doctored.codec(writer);
          if (doctored.numValues === undefined) {
            required(writer, "ColumnMetaData.num_values", 5, ThriftType.I64, () =>
              writer.fieldI64(5, 1n),
            );
          } else doctored.numValues(writer);
          required(writer, "ColumnMetaData.total_uncompressed_size", 6, ThriftType.I64, () =>
            writer.fieldI64(
              6,
              BigInt(page.uncompressedSize + (doctored.totalUncompressedSizeDelta ?? 0)),
            ),
          );
          required(writer, "ColumnMetaData.total_compressed_size", 7, ThriftType.I64, () =>
            writer.fieldI64(
              7,
              BigInt(page.compressedSize + (doctored.totalCompressedSizeDelta ?? 0)),
            ),
          );
          required(writer, "ColumnMetaData.data_page_offset", 9, ThriftType.I64, () =>
            writer.fieldI64(9, BigInt(dataPageOffset)),
          );
          if (doctored.pathAfterCounts) writePath();
          if (doctored.unknownOptional) writer.fieldString(99, "future");
          writer.structEnd();
        }
        if (doctored.fileOffsetAfterMetadata) writeFileOffset();
        if (doctored.encryptedMetadata) {
          writer.fieldBinary(9, new Uint8Array([1, 2, 3]));
        }
        if (doctored.cryptoMetadata) {
          writer.fieldStructBegin(8); // crypto_metadata
          writer.fieldStructBegin(1); // ENCRYPTION_WITH_FOOTER_KEY
          writer.structEnd();
          writer.structEnd();
        }
        if (doctored.offsetIndexOffset) writer.fieldI64(4, 123n);
        if (doctored.unknownOptional) writer.fieldString(99, "future");
        writer.structEnd();
      });
      required(writer, "RowGroup.total_byte_size", 2, ThriftType.I64, () =>
        writer.fieldI64(
          2,
          BigInt(
            page.uncompressedSize +
              (doctored.totalUncompressedSizeDelta ?? 0) +
              (doctored.rowGroupTotalSizeDelta ?? 0),
          ),
        ),
      );
      required(writer, "RowGroup.num_rows", 3, ThriftType.I64, () => writer.fieldI64(3, 1n));
      if (doctored.unknownOptional) writer.fieldString(99, "future");
      writer.structEnd();
    });
    if (doctored.createdBy !== false) writer.fieldString(6, "probe");
    if (doctored.unknownOptional) writer.fieldString(99, "future");
    writer.structEnd();
    return sealFile(out, writer.toBytes());
  }

  const REQUIRED_FIELDS: readonly RequiredField[] = [
    "FileMetaData.version",
    "FileMetaData.schema",
    "FileMetaData.num_rows",
    "FileMetaData.row_groups",
    "SchemaElement.name",
    "RowGroup.columns",
    "RowGroup.total_byte_size",
    "RowGroup.num_rows",
    "ColumnChunk.file_offset",
    "ColumnMetaData.type",
    "ColumnMetaData.encodings",
    "ColumnMetaData.path_in_schema",
    "ColumnMetaData.codec",
    "ColumnMetaData.num_values",
    "ColumnMetaData.total_uncompressed_size",
    "ColumnMetaData.total_compressed_size",
    "ColumnMetaData.data_page_offset",
    "PageHeader.type",
    "PageHeader.uncompressed_page_size",
    "PageHeader.compressed_page_size",
    "PageHeader.data_page_header",
    "DataPageHeader.num_values",
    "DataPageHeader.encoding",
    "DataPageHeader.definition_level_encoding",
    "DataPageHeader.repetition_level_encoding",
  ];

  function requiredFieldColumn(field: RequiredField): string | undefined {
    return field.startsWith("Page") ||
      field.startsWith("DataPage") ||
      field === "ColumnChunk.file_offset" ||
      (field.startsWith("ColumnMetaData.") && field !== "ColumnMetaData.path_in_schema")
      ? "n"
      : undefined;
  }

  it("reads the undoctored file, including required fields whose value is zero", () => {
    expect(readParquet(handBuilt()).rows).toEqual([{ n: 7n }]);
  });

  it("refuses trailing bytes in an otherwise internally consistent page and chunk", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(handBuilt({ bodyTrailingByte: true })),
    );
    expect(error.message).toContain("trailing body bytes");
    expect(error.column).toBe("n");
  });

  it.each([-1, 1])("refuses a total_compressed_size offset by %i", (delta) => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(handBuilt({ totalCompressedSizeDelta: delta })),
    );
    expect(error.column).toBe("n");
  });

  it("refuses total_uncompressed_size that differs from its serialized pages", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(handBuilt({ totalUncompressedSizeDelta: 1 })),
    );
    expect(error.message).toContain("uncompressed chunk bytes");
    expect(error.column).toBe("n");
  });

  it("refuses a row group's total_byte_size that differs from every chunk", () => {
    const bytes = handBuilt({ rowGroupTotalSizeDelta: 1 });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.message).toContain("total_byte_size");
    expectError("ERR_READ_MALFORMED", () => readSchema(bytes));
  });

  it.each(REQUIRED_FIELDS)("refuses a missing required %s", (field) => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        handBuilt({
          required: { field, mode: "missing" },
          fileOffsetAfterMetadata: field === "ColumnChunk.file_offset",
          pathAfterCounts: field.startsWith("ColumnMetaData."),
        }),
      ),
    );
    expect(error.message).toContain(field);
    expect(error.column).toBe(requiredFieldColumn(field));
  });

  it.each(REQUIRED_FIELDS)("skips and refuses a wrong-type required %s", (field) => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        handBuilt({
          required: { field, mode: "wrong" },
          fileOffsetAfterMetadata: field === "ColumnChunk.file_offset",
          pathAfterCounts: field.startsWith("ColumnMetaData."),
        }),
      ),
    );
    expect(error.message).toContain(field);
    expect(error.column).toBe(requiredFieldColumn(field));
  });

  it("attributes an out-of-order unusable file_offset to its decoded column", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        handBuilt({
          fileOffset: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
          fileOffsetAfterMetadata: true,
        }),
      ),
    );
    expect(error.message).toContain("ColumnChunk.file_offset");
    expect(error.column).toBe("n");
  });

  it("attributes an unusable count decoded before path_in_schema", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        handBuilt({
          numValues: (writer) => writer.fieldI64(5, BigInt(Number.MAX_SAFE_INTEGER) + 1n),
          pathAfterCounts: true,
        }),
      ),
    );
    expect(error.message).toContain("ColumnMetaData.num_values");
    expect(error.column).toBe("n");
  });

  it.each([
    "FileMetaData.schema",
    "FileMetaData.row_groups",
    "RowGroup.columns",
    "ColumnMetaData.encodings",
  ] as const)("consumes and refuses wrong-element lists for %s", (field) => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(handBuilt({ wrongListElement: field })),
    );
    expect(error.message).toContain(field);
  });

  it("accepts both supported footer versions", () => {
    expect(readParquet(handBuilt({ version: 1 })).rows).toEqual([{ n: 7n }]);
    expect(readParquet(handBuilt({ version: 2 })).rows).toEqual([{ n: 7n }]);
  });

  it("refuses a reserved footer version as unsupported", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(handBuilt({ version: 3 })));
    expect(error.message).toContain("version 3");
  });

  it("skips unknown optional fields at every supported struct level", () => {
    expect(readParquet(handBuilt({ unknownOptional: true })).rows).toEqual([{ n: 7n }]);
  });

  it("keeps official optional footer fields optional", () => {
    expect(readParquet(handBuilt({ createdBy: false })).rows).toEqual([{ n: 7n }]);
  });

  it("does not require a DATA_PAGE header for another valid page type", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(
        handBuilt({
          pageHeader: (writer) => {
            writer.fieldI32(1, 1); // INDEX_PAGE, with no unrelated union member
            writer.fieldI32(2, 8);
            writer.fieldI32(3, 8);
          },
        }),
      ),
    );
    expect(error.column).toBe("n");
    expect(error.message).toContain("INDEX_PAGE");
    expect(error.message).not.toContain("data_page_header");
  });

  it("refuses absent inline column metadata without calling it a required Thrift field", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(handBuilt({ inlineMetadata: false })),
    );
    expect(error.message).toContain("inline metadata");
    expect(error.message).not.toContain("required");
  });

  it("refuses encrypted external column metadata honestly", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(handBuilt({ inlineMetadata: false, encryptedMetadata: true })),
    );
    expect(error.message).toContain("encrypted metadata");
  });

  it.each([
    ["crypto_metadata", { cryptoMetadata: true }],
    ["encrypted_column_metadata", { encryptedMetadata: true }],
  ] as const)("refuses inline metadata accompanied by %s", (_name, marker) => {
    const error = expectError("ERR_READ_UNSUPPORTED", () => readParquet(handBuilt(marker)));
    expect(error.message).toContain("encrypted metadata");
    expect(error.column).toBe("n");
  });

  it("does not mistake offset_index_offset for encrypted metadata", () => {
    const error = expectError("ERR_READ_UNSUPPORTED", () =>
      readParquet(handBuilt({ inlineMetadata: false, offsetIndexOffset: true })),
    );
    expect(error.message).toContain("without inline metadata");
    expect(error.message).not.toContain("encrypted metadata");
  });

  it("refuses a type_length that is not an i32", () => {
    const bytes = handBuilt({
      physical: PhysicalType.FIXED_LEN_BYTE_ARRAY,
      typeLength: (writer) => writer.fieldBinary(2, new Uint8Array([16])),
    });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.column).toBe("n");
    expect(error.message).toContain("type_length");
  });

  it("refuses a path_in_schema that is not a list", () => {
    const bytes = handBuilt({ path: (writer) => writer.fieldString(3, "n") });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.column).toBeUndefined();
    expect(error.message).toContain("ColumnMetaData.path_in_schema");
  });

  it("refuses a path_in_schema that is a list of the wrong thing", () => {
    // A list whose header is already read cannot be handed back to the
    // skipper, so its elements are consumed here — two varints, and the
    // fields after them still land where they should.
    const bytes = handBuilt({
      path: (writer) => {
        writer.fieldListBegin(3, ThriftType.I32, 2);
        writer.elementI32(1);
        writer.elementI32(2);
      },
    });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.column).toBeUndefined();
    expect(error.message).toContain("ColumnMetaData.path_in_schema");
  });

  it("refuses a num_values that is not an i64", () => {
    const bytes = handBuilt({ numValues: (writer) => writer.fieldString(5, "one") });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.column).toBe("n");
    expect(error.message).toContain("ColumnMetaData.num_values");
  });

  it("skips and refuses a page type that is not an i32", () => {
    const bytes = handBuilt({
      pageHeader: (writer) => {
        writer.fieldString(1, "data"); // type, which is an i32
        writer.fieldI32(2, 8); // uncompressed_page_size
        writer.fieldI32(3, 8); // compressed_page_size
        dataPageHeader(writer, (inner) => inner.fieldI32(1, 1));
      },
    });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.column).toBe("n");
    expect(error.message).toContain("PageHeader.type");
  });

  it("refuses a page's num_values that is not an i32", () => {
    const bytes = handBuilt({
      pageHeader: (writer) => {
        writer.fieldI32(1, PageType.DATA_PAGE);
        writer.fieldI32(2, 8);
        writer.fieldI32(3, 8);
        dataPageHeader(writer, (inner) => inner.fieldString(1, "one"));
      },
    });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.column).toBe("n");
    expect(error.message).toContain("DataPageHeader.num_values");
  });

  it("skips and refuses a codec id that is not an i32", () => {
    const bytes = handBuilt({
      codec: (writer) => {
        writer.fieldStructBegin(4);
        writer.structEnd();
      },
    });
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(bytes));
    expect(error.column).toBe("n");
    expect(error.message).toContain("ColumnMetaData.codec");
  });
});

/**
 * `__proto__` is a column name like any other.
 *
 * Parquet names columns with UTF-8 strings and reserves none of them, so a file
 * may carry one called `__proto__` — and JavaScript is the only party here that
 * finds the name special. `record.__proto__ = value` creates no property at
 * all: it runs `Object.prototype`'s setter, which drops a primitive on the
 * floor and, handed an object, replaces the row's prototype with it. Every
 * value a file states has to come back as an own property, and none of them may
 * reach a prototype slot.
 */
describe("a column named __proto__", () => {
  // Computed keys throughout: `{ __proto__: x }` in a literal sets the
  // prototype rather than declaring a property, which is the problem itself.
  const schema = defineSchema({
    ["__proto__"]: { type: "i64" },
    n: { type: "i64" },
    maybe: { type: "string", optional: true },
  });

  /** Two rows whose `__proto__` column holds a value like any other. */
  function written(): Uint8Array {
    const writer = createWriter(schema);
    writer.append({ ["__proto__"]: 1n, n: 10n, maybe: "a" });
    writer.append({ ["__proto__"]: 2n, n: 20n, maybe: null });
    return sync(writer.finish());
  }

  /** Asserts one row carries every column as its own, enumerable property. */
  function expectOwnColumns(row: ReadRow, proto: bigint, n: bigint): void {
    expect(Object.hasOwn(row, "__proto__")).toBe(true);
    expect(Object.getPrototypeOf(row)).toBe(Object.prototype);
    expect(Object.keys(row).sort()).toEqual(["__proto__", "maybe", "n"]);
    expect(row["__proto__"]).toBe(proto);
    expect(row.n).toBe(n);
  }

  it("reads the column back as an own property of every row", () => {
    const { rows } = readParquet(written());
    expect(rows).toHaveLength(2);
    expectOwnColumns(rows[0], 1n, 10n);
    expectOwnColumns(rows[1], 2n, 20n);
    expect(rows[0].maybe).toBe("a");
    expect(rows[1].maybe).toBe(null);
  });

  it("reads it back the same way one row group at a time", () => {
    const groups = [...readRowGroups(written())];
    expect(groups).toHaveLength(1);
    const rows = sync(groups[0]);
    expectOwnColumns(rows[0], 1n, 10n);
    expectOwnColumns(rows[1], 2n, 20n);
  });

  it("keeps the returned definition a plain object with the column on it", () => {
    const { definition, columns } = readParquet(written()).schema;
    expect(Object.getPrototypeOf(definition)).toBe(Object.prototype);
    expect(Object.hasOwn(definition, "__proto__")).toBe(true);
    expect(Object.keys(definition)).toEqual(["__proto__", "n", "maybe"]);
    expect(columns.map((column) => column.name)).toEqual(["__proto__", "n", "maybe"]);
  });

  it("never lets a column's value become a row's prototype", () => {
    // A `Date` is the value that would actually take: assigned to `__proto__`
    // it replaces the prototype instead of being dropped.
    const dated = defineSchema({ ["__proto__"]: { type: "timestamp" } });
    const writer = createWriter(dated);
    writer.append({ ["__proto__"]: 1_700_000_000_000 });
    const { rows } = readParquet(sync(writer.finish()));

    expect(Object.getPrototypeOf(rows[0])).toBe(Object.prototype);
    expect(Object.hasOwn(rows[0], "__proto__")).toBe(true);
    expect(rows[0]["__proto__"]).toEqual(new Date(1_700_000_000_000));
  });

  it("treats an absent __proto__ column as absent, not as Object.prototype", () => {
    // `row["__proto__"]` on a row that carries none hands back
    // Object.prototype, which is neither a value nor a missing column unless
    // the writer asks whether the key is really there.
    const optional = defineSchema({
      ["__proto__"]: { type: "string", optional: true },
      n: { type: "i64" },
    });
    const writer = createWriter(optional);
    writer.append({ n: 1n });
    const { rows } = readParquet(sync(writer.finish()));
    expect(rows[0]["__proto__"]).toBe(null);

    // And a required one that is missing is missing, named as such.
    const error = expectError("ERR_ROW_VALUE_MISSING", () =>
      // @ts-expect-error the column is deliberately omitted
      createWriter(schema).append({ n: 1n, maybe: null }),
    );
    expect(error.column).toBe("__proto__");
  });
});

describe("readSchema", () => {
  it("rejects a malformed envelope just like readParquet", () => {
    expectError("ERR_READ_MALFORMED", () => readSchema(new Uint8Array(20)));
  });

  it("reports an unsupported schema without reading a page", () => {
    // The page bytes are destroyed, but the schema is still readable: proof
    // that readSchema stops at the footer.
    const bytes = sample();
    const copy = bytes.slice();
    copy.fill(0xff, 4, 40);
    expect(readSchema(copy, { types: TYPES }).columns.map((column) => column.name)).toEqual([
      "s",
      "f",
      "g",
      "i",
      "n",
      "b",
      "t",
      "p",
      "u",
    ]);
    expect(caught(() => read(copy))).toBeInstanceOf(TavolatoError);
  });

  it("claims columns with the same types readParquet would", () => {
    const bytes = sample();
    expect(readSchema(bytes, { types: TYPES })).toEqual(
      (readParquet(bytes, { types: TYPES }) as { schema: unknown }).schema,
    );
    // Without them the annotated columns have no meaning, and it says so.
    const error = expectError("ERR_READ_UNSUPPORTED", () => readSchema(bytes));
    expect(error.column).toBe("p");
  });
});

describe("error identity", () => {
  it("throws TavolatoError, narrowable by code", () => {
    const error = caught(() => readParquet(new Uint8Array(4)));
    expect(error).toBeInstanceOf(TavolatoError);
    expect(isTavolatoError(error, "ERR_READ_MALFORMED")).toBe(true);
    expect(isTavolatoError(error, "ERR_READ_UNSUPPORTED")).toBe(false);
  });
});
