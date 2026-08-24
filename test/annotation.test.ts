import { describe, expect, it } from "vitest";
import { defineColumnType } from "../src/index.ts";
import type { Annotation, LogicalAdapter } from "../src/index.ts";
import {
  annotationName,
  annotationOf,
  ConvertedType,
  decodeFileMetadata,
  encodeFileMetadata,
  LogicalTypeId,
  PhysicalType,
  type SchemaElement,
} from "../src/internal/format.ts";
import { CompactWriter, ThriftType } from "../src/internal/thrift.ts";

/**
 * The annotation model: one open shape both spellings of a Parquet annotation
 * decode into, and the one shape the writer stamps back out.
 *
 * `parquet.thrift` froze this list, so the model mirrors it rather than
 * tavolato's own scope — which is what lets an annotation nothing here has a
 * column type for still be *named*, offered to an adapter, and refused by name
 * if nobody wants it.
 */

/** A `SchemaElement` carrying nothing but the annotation under test. */
function element(fields: Partial<SchemaElement>): SchemaElement {
  return { name: "c", numChildren: 0, ...fields };
}

/** A column type that stamps `annotation` and claims nothing. */
function stamping(annotation: Annotation): LogicalAdapter<number, number> {
  return defineColumnType<number, number>({
    name: `stamps ${annotation.kind}`,
    physical: "i32",
    matches: () => false,
    annotate: () => annotation,
    read: (raw) => raw as number,
    write: (value) => value,
  });
}

/** Writes a one-column footer and reads the annotation back out of it. */
function stamped(annotation: Annotation): Annotation {
  const columns = [{ name: "c", type: stamping(annotation), optional: false }];
  const metadata = decodeFileMetadata(encodeFileMetadata(columns, [], 0, "test"));
  return annotationOf(metadata.schema[1]);
}

describe("the deprecated ConvertedType", () => {
  it.each([
    [ConvertedType.UTF8, { kind: "string" }],
    [ConvertedType.JSON, { kind: "json" }],
    [ConvertedType.BSON, { kind: "bson" }],
    [ConvertedType.ENUM, { kind: "enum" }],
    [ConvertedType.DATE, { kind: "date" }],
    // The old enum only knew two resolutions, and defined both as UTC.
    [ConvertedType.TIME_MILLIS, { kind: "time", unit: "millis", isAdjustedToUTC: true }],
    [ConvertedType.TIME_MICROS, { kind: "time", unit: "micros", isAdjustedToUTC: true }],
    [ConvertedType.TIMESTAMP_MILLIS, { kind: "timestamp", unit: "millis", isAdjustedToUTC: true }],
    [ConvertedType.TIMESTAMP_MICROS, { kind: "timestamp", unit: "micros", isAdjustedToUTC: true }],
    [ConvertedType.INT_8, { kind: "integer", bitWidth: 8, isSigned: true }],
    [ConvertedType.INT_16, { kind: "integer", bitWidth: 16, isSigned: true }],
    [ConvertedType.INT_32, { kind: "integer", bitWidth: 32, isSigned: true }],
    [ConvertedType.INT_64, { kind: "integer", bitWidth: 64, isSigned: true }],
    [ConvertedType.UINT_8, { kind: "integer", bitWidth: 8, isSigned: false }],
    [ConvertedType.UINT_16, { kind: "integer", bitWidth: 16, isSigned: false }],
    [ConvertedType.UINT_32, { kind: "integer", bitWidth: 32, isSigned: false }],
    [ConvertedType.UINT_64, { kind: "integer", bitWidth: 64, isSigned: false }],
  ])("decodes %i into the model", (convertedType, expected) => {
    expect(annotationOf(element({ convertedType }))).toEqual(expected);
  });

  it("takes a decimal's parameters off the element, where the old spelling kept them", () => {
    expect(
      annotationOf(element({ convertedType: ConvertedType.DECIMAL, precision: 12, scale: 4 })),
    ).toEqual({ kind: "decimal", precision: 12, scale: 4 });
    // Missing parameters are not invented: a decimal of no precision matches
    // nothing, and is refused by name.
    expect(annotationOf(element({ convertedType: ConvertedType.DECIMAL }))).toEqual({
      kind: "decimal",
      precision: 0,
      scale: 0,
    });
  });

  it("keeps the nested and unnamed ones nameable rather than reading them as nothing", () => {
    expect(annotationOf(element({ convertedType: ConvertedType.MAP }))).toEqual({
      kind: "unknown",
      id: LogicalTypeId.MAP,
    });
    expect(annotationOf(element({ convertedType: ConvertedType.MAP_KEY_VALUE }))).toEqual({
      kind: "unknown",
      id: LogicalTypeId.MAP,
    });
    expect(annotationOf(element({ convertedType: ConvertedType.LIST }))).toEqual({
      kind: "unknown",
      id: LogicalTypeId.LIST,
    });
    expect(annotationOf(element({ convertedType: ConvertedType.INTERVAL }))).toEqual({
      kind: "unknown",
      id: LogicalTypeId.INTERVAL,
    });
    // An id outside the enum is not a bare column: reading it as `none` would
    // hand back values tavolato cannot vouch for.
    expect(annotationOf(element({ convertedType: 99 }))).toEqual({ kind: "unknown", id: 0 });
  });

  it("reads no annotation at all as none", () => {
    expect(annotationOf(element({}))).toEqual({ kind: "none" });
  });

  it("loses to the modern spelling wherever a file carries both", () => {
    expect(
      annotationOf(
        element({ convertedType: ConvertedType.TIMESTAMP_MILLIS, logical: { kind: "json" } }),
      ),
    ).toEqual({ kind: "json" });
  });
});

describe("the annotations a column type stamps", () => {
  it.each([
    [{ kind: "string" }],
    [{ kind: "json" }],
    [{ kind: "bson" }],
    [{ kind: "enum" }],
    [{ kind: "uuid" }],
    [{ kind: "date" }],
    [{ kind: "float16" }],
    [{ kind: "decimal", precision: 38, scale: 6 }],
    [{ kind: "time", unit: "millis", isAdjustedToUTC: false }],
    [{ kind: "time", unit: "micros", isAdjustedToUTC: true }],
    [{ kind: "time", unit: "nanos", isAdjustedToUTC: false }],
    [{ kind: "timestamp", unit: "millis", isAdjustedToUTC: true }],
    [{ kind: "timestamp", unit: "micros", isAdjustedToUTC: false }],
    [{ kind: "timestamp", unit: "nanos", isAdjustedToUTC: true }],
    [{ kind: "integer", bitWidth: 8, isSigned: true }],
    [{ kind: "integer", bitWidth: 16, isSigned: false }],
    [{ kind: "integer", bitWidth: 32, isSigned: true }],
    [{ kind: "integer", bitWidth: 64, isSigned: false }],
  ] as [Annotation][])("survives the trip through a footer: %o", (annotation) => {
    expect(stamped(annotation)).toEqual(annotation);
  });

  it("writes nothing at all for a bare column", () => {
    expect(stamped({ kind: "none" })).toEqual({ kind: "none" });
  });
});

/**
 * The `LogicalType` union as it comes off the wire, written by hand: these are
 * the shapes no writer here produces — a member from a later release, a
 * parameter in the wrong Thrift type, a union with nothing in it.
 *
 * None of them may throw. An annotation that cannot be read is `unknown`, and
 * `unknown` is refused one layer up, where the column has a name.
 */
function decodedLogicalType(write: (writer: CompactWriter) => void): Annotation {
  const writer = new CompactWriter();
  writer.structBegin(); // FileMetaData
  writer.fieldI32(1, 1); // version
  writer.fieldListBegin(2, ThriftType.STRUCT, 2);
  writer.structBegin(); // the root group
  writer.fieldString(4, "schema");
  writer.fieldI32(5, 1);
  writer.structEnd();
  writer.structBegin(); // the one leaf
  writer.fieldI32(1, PhysicalType.INT64);
  writer.fieldI32(3, 0);
  writer.fieldString(4, "c");
  writer.fieldStructBegin(10);
  write(writer);
  writer.structEnd();
  writer.structEnd();
  writer.fieldI64(3, 0n);
  writer.fieldListBegin(4, ThriftType.STRUCT, 0);
  writer.fieldString(6, "test");
  writer.structEnd();
  return annotationOf(decodeFileMetadata(writer.toBytes()).schema[1]);
}

describe("a LogicalType this version cannot read", () => {
  it("keeps a member from a later release, by its field id", () => {
    // 17 is GEOMETRY, which tavolato has no reading for and every writer is
    // free to produce.
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(17);
        writer.structEnd();
      }),
    ).toEqual({ kind: "unknown", id: 17 });
  });

  it("reads a union carrying nothing as unnamed", () => {
    expect(decodedLogicalType(() => {})).toEqual({ kind: "unknown", id: 0 });
  });

  it("refuses to guess a resolution the file does not give", () => {
    // TimestampType with its UTC flag but no unit: milliseconds would be a
    // guess, and a wrong one moves every value by a factor of a thousand.
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.TIMESTAMP);
        writer.fieldBool(1, true);
        writer.structEnd();
      }),
    ).toEqual({ kind: "unknown", id: LogicalTypeId.TIMESTAMP });

    // A TimeUnit union member nobody has defined yet.
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.TIME);
        writer.fieldBool(1, false);
        writer.fieldStructBegin(2);
        writer.fieldStructBegin(9);
        writer.structEnd();
        writer.structEnd();
        writer.structEnd();
      }),
    ).toEqual({ kind: "unknown", id: LogicalTypeId.TIME });
  });

  it("refuses an integer width the format does not define", () => {
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.INTEGER);
        writer.fieldI8(1, 24);
        writer.fieldBool(2, true);
        writer.structEnd();
      }),
    ).toEqual({ kind: "unknown", id: LogicalTypeId.INTEGER });
  });

  it("refuses a parameter that is not the Thrift type the format declares", () => {
    // `IntType.bitWidth` is an i8. A writer that puts an i32 there is not one
    // this reader will second-guess.
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.INTEGER);
        writer.fieldI32(1, 32);
        writer.fieldBool(2, true);
        writer.structEnd();
      }),
    ).toEqual({ kind: "unknown", id: LogicalTypeId.INTEGER });

    // Same for a whole member that is not the struct it should be.
    expect(
      decodedLogicalType((writer) => {
        writer.fieldI32(LogicalTypeId.DECIMAL, 0);
      }),
    ).toEqual({ kind: "unknown", id: 0 });
  });
});

describe("annotationName", () => {
  it.each([
    [{ kind: "string" }, "STRING"],
    [{ kind: "json" }, "JSON"],
    [{ kind: "bson" }, "BSON"],
    [{ kind: "enum" }, "ENUM"],
    [{ kind: "uuid" }, "UUID"],
    [{ kind: "date" }, "DATE"],
    [{ kind: "float16" }, "FLOAT16"],
    [{ kind: "decimal", precision: 9, scale: 2 }, "DECIMAL(precision=9, scale=2)"],
    [
      { kind: "time", unit: "micros", isAdjustedToUTC: false },
      "TIME(MICROS, isAdjustedToUTC=false)",
    ],
    [
      { kind: "timestamp", unit: "nanos", isAdjustedToUTC: true },
      "TIMESTAMP(NANOS, isAdjustedToUTC=true)",
    ],
    [{ kind: "integer", bitWidth: 8, isSigned: true }, "INTEGER(8, signed)"],
    [{ kind: "integer", bitWidth: 64, isSigned: false }, "INTEGER(64, unsigned)"],
    // A refusal names what it found even when what it found is from a later
    // release than this one.
    [{ kind: "unknown", id: LogicalTypeId.NULL }, "UNKNOWN"],
    [{ kind: "unknown", id: 16 }, "VARIANT"],
    [{ kind: "unknown", id: 42 }, "logical type 42"],
    [{ kind: "unknown", id: 0 }, "an annotation this version has no name for"],
  ] as [Annotation, string][])("names %o", (annotation, name) => {
    expect(annotationName(annotation)).toBe(name);
  });
});
