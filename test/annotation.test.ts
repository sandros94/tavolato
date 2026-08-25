import { describe, expect, it } from "vitest";
import { defineColumnType, readParquet } from "../src/index.ts";
import type { Annotation, LogicalAdapter } from "../src/index.ts";
import {
  annotationName,
  annotationOf,
  ConvertedType,
  decodeFileMetadata,
  encodeFileMetadata,
  LogicalTypeId,
  MAGIC,
  PhysicalType,
  type SchemaElement,
  snapshotColumn,
  TimeUnit,
} from "../src/internal/format.ts";
import { CompactWriter, ThriftType } from "../src/internal/thrift.ts";
import { expectError } from "./_errors.ts";

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

/** A legal storage layout for `annotation`; only used to test footer spelling. */
function storageOf(annotation: Annotation): {
  readonly physical: "i32" | "i64" | "bytes" | "fixed";
  readonly typeLength?: number;
} {
  switch (annotation.kind) {
    case "string":
    case "json":
    case "bson":
    case "enum": {
      return { physical: "bytes" };
    }
    case "uuid": {
      return { physical: "fixed", typeLength: 16 };
    }
    case "float16": {
      return { physical: "fixed", typeLength: 2 };
    }
    case "decimal": {
      return { physical: "fixed", typeLength: 16 };
    }
    case "timestamp": {
      return { physical: "i64" };
    }
    case "time": {
      return { physical: annotation.unit === "millis" ? "i32" : "i64" };
    }
    case "integer": {
      return { physical: annotation.bitWidth === 64 ? "i64" : "i32" };
    }
    default: {
      return { physical: "i32" };
    }
  }
}

/** A column type that stamps `annotation` and claims nothing. */
function stamping(annotation: Annotation): LogicalAdapter<number, number> {
  return defineColumnType<number, number>({
    name: `stamps ${annotation.kind}`,
    ...storageOf(annotation),
    matches: () => false,
    annotate: () => annotation,
    read: (raw) => raw as number,
    write: (value) => value,
  });
}

/** Writes a one-column footer and reads the annotation back out of it. */
function stamped(annotation: Annotation): Annotation {
  const columns = [snapshotColumn({ name: "c", type: stamping(annotation), optional: false })];
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
 * parameter in the wrong Thrift type, or a union with nothing in it.
 * Structurally invalid known members are malformed; correctly wired future
 * values remain `unknown` and claimable.
 */
function footerWithLogicalType(
  write: ((writer: CompactWriter) => void) | undefined,
  options: {
    readonly physical?: number;
    readonly typeLength?: number;
    readonly convertedType?: number;
  } = {},
): Uint8Array {
  const writer = new CompactWriter();
  writer.structBegin(); // FileMetaData
  writer.fieldI32(1, 1); // version
  writer.fieldListBegin(2, ThriftType.STRUCT, 2);
  writer.structBegin(); // the root group
  writer.fieldString(4, "schema");
  writer.fieldI32(5, 1);
  writer.structEnd();
  writer.structBegin(); // the one leaf
  writer.fieldI32(1, options.physical ?? PhysicalType.INT64);
  if (options.typeLength !== undefined) writer.fieldI32(2, options.typeLength);
  writer.fieldI32(3, 0);
  writer.fieldString(4, "c");
  if (options.convertedType !== undefined) writer.fieldI32(6, options.convertedType);
  if (write !== undefined) {
    writer.fieldStructBegin(10);
    write(writer);
    writer.structEnd();
  }
  writer.structEnd();
  writer.fieldI64(3, 0n);
  writer.fieldListBegin(4, ThriftType.STRUCT, 0);
  writer.fieldString(6, "test");
  writer.structEnd();
  return writer.toBytes();
}

function decodedLogicalType(write: (writer: CompactWriter) => void): Annotation {
  return annotationOf(decodeFileMetadata(footerWithLogicalType(write)).schema[1]);
}

/** The same footer, wrapped in the envelope that makes it a file `readParquet` takes. */
function fileWithLogicalType(
  write: ((writer: CompactWriter) => void) | undefined,
  options?: Parameters<typeof footerWithLogicalType>[1],
): Uint8Array {
  const footer = footerWithLogicalType(write, options);
  const bytes = new Uint8Array(MAGIC.length * 2 + footer.length + 4);
  bytes.set(MAGIC, 0);
  bytes.set(footer, MAGIC.length);
  new DataView(bytes.buffer).setUint32(MAGIC.length + footer.length, footer.length, true);
  bytes.set(MAGIC, MAGIC.length + footer.length + 4);
  return bytes;
}

describe("strict Thrift annotation unions", () => {
  const recognizedLogicalTypes = [
    LogicalTypeId.STRING,
    LogicalTypeId.MAP,
    LogicalTypeId.LIST,
    LogicalTypeId.ENUM,
    LogicalTypeId.DECIMAL,
    LogicalTypeId.DATE,
    LogicalTypeId.TIME,
    LogicalTypeId.TIMESTAMP,
    LogicalTypeId.INTEGER,
    LogicalTypeId.NULL,
    LogicalTypeId.JSON,
    LogicalTypeId.BSON,
    LogicalTypeId.UUID,
    LogicalTypeId.FLOAT16,
    LogicalTypeId.VARIANT,
    LogicalTypeId.GEOMETRY,
    LogicalTypeId.GEOGRAPHY,
    LogicalTypeId.FILE,
  ] as const;

  it.each(recognizedLogicalTypes)("requires STRUCT for recognized LogicalType member %i", (id) => {
    expectError("ERR_READ_MALFORMED", () => decodedLogicalType((writer) => writer.fieldI32(id, 0)));
  });

  it("requires exactly one LogicalType member", () => {
    expectError("ERR_READ_MALFORMED", () => decodedLogicalType(() => {}));
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.STRING);
        writer.structEnd();
        writer.fieldStructBegin(LogicalTypeId.DATE);
        writer.structEnd();
      }),
    );
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(42);
        writer.structEnd();
        writer.fieldStructBegin(LogicalTypeId.STRING);
        writer.structEnd();
      }),
    );
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(42);
        writer.structEnd();
        writer.fieldI32(43, 0);
      }),
    );
  });

  it.each([
    LogicalTypeId.DECIMAL,
    LogicalTypeId.TIME,
    LogicalTypeId.TIMESTAMP,
    LogicalTypeId.INTEGER,
  ])("refuses an empty recognized LogicalType payload %i", (id) => {
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(id);
        writer.structEnd();
      }),
    );
  });

  it("requires exactly one TimeUnit member of the declared STRUCT type", () => {
    const decoded = (writeUnit: (writer: CompactWriter) => void): Annotation =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.TIME);
        writer.fieldBool(1, false);
        writer.fieldStructBegin(2);
        writeUnit(writer);
        writer.structEnd();
        writer.structEnd();
      });

    expectError("ERR_READ_MALFORMED", () => decoded(() => {}));
    expectError("ERR_READ_MALFORMED", () =>
      decoded((writer) => writer.fieldI32(TimeUnit.MILLIS, 0)),
    );
    expectError("ERR_READ_MALFORMED", () =>
      decoded((writer) => {
        writer.fieldStructBegin(TimeUnit.MILLIS);
        writer.structEnd();
        writer.fieldStructBegin(TimeUnit.MICROS);
        writer.structEnd();
      }),
    );
    expectError("ERR_READ_MALFORMED", () =>
      decoded((writer) => {
        writer.fieldStructBegin(9);
        writer.structEnd();
        writer.fieldStructBegin(TimeUnit.NANOS);
        writer.structEnd();
      }),
    );
  });

  it("keeps one unknown future union member forward compatible", () => {
    expect(
      decodedLogicalType((writer) => {
        writer.fieldI32(42, 0);
      }),
    ).toEqual({ kind: "unknown", id: 42 });
  });

  it("ignores future fields inside a recognized empty payload struct", () => {
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.STRING);
        writer.fieldString(42, "future");
        writer.structEnd();
      }),
    ).toEqual({ kind: "string" });
  });

  it("ignores future fields inside parameter and TimeUnit structs", () => {
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.INTEGER);
        writer.fieldI8(1, 32);
        writer.fieldBool(2, true);
        writer.fieldString(42, "future");
        writer.structEnd();
      }),
    ).toEqual({ kind: "integer", bitWidth: 32, isSigned: true });

    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.TIME);
        writer.fieldBool(1, false);
        writer.fieldStructBegin(2);
        writer.fieldStructBegin(TimeUnit.MILLIS);
        writer.fieldString(42, "future");
        writer.structEnd();
        writer.structEnd();
        writer.structEnd();
      }),
    ).toEqual({ kind: "time", unit: "millis", isAdjustedToUTC: false });
  });

  it("keeps the last duplicate parameter value, matching other decoded structs", () => {
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.DECIMAL);
        writer.fieldI32(1, 1);
        writer.fieldI32(1, 2);
        writer.fieldI32(2, 4);
        writer.structEnd();
      }),
    ).toEqual({ kind: "decimal", precision: 4, scale: 2 });
  });
});

describe("a LogicalType this version cannot read", () => {
  it("keeps a member from a later release, by its field id", () => {
    // 42 stands for a future member this version cannot know a contract for.
    expect(
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(42);
        writer.structEnd();
      }),
    ).toEqual({ kind: "unknown", id: 42 });
  });

  it("leaves a future annotation claimable without guessing its physical contract", () => {
    const future = defineColumnType({
      name: "future",
      physical: "i64",
      matches: (annotation) => annotation.kind === "unknown" && annotation.id === 42,
      annotate: (): Annotation => ({
        kind: "timestamp",
        unit: "micros",
        isAdjustedToUTC: false,
      }),
      read: (raw) => raw as bigint,
      write: (value: bigint) => value,
    });
    const file = fileWithLogicalType((writer) => {
      writer.fieldStructBegin(42);
      writer.structEnd();
    });

    expect(readParquet(file, { types: [future] }).schema.columns[0].type).toBe(future);
  });

  it("refuses a union carrying nothing", () => {
    expectError("ERR_READ_MALFORMED", () => decodedLogicalType(() => {}));
  });

  it("refuses to guess a resolution the file does not give", () => {
    // TimestampType with its UTC flag but no unit: milliseconds would be a
    // guess, and a wrong one moves every value by a factor of a thousand.
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.TIMESTAMP);
        writer.fieldBool(1, true);
        writer.structEnd();
      }),
    );

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
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.INTEGER);
        writer.fieldI32(1, 32);
        writer.fieldBool(2, true);
        writer.structEnd();
      }),
    );

    // Same for a whole member that is not the struct it should be.
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldI32(LogicalTypeId.DECIMAL, 0);
      }),
    );
  });

  it("refuses a bool parameter that is not a bool, rather than reading past it", () => {
    // `IntType.isSigned` is a bool, and a bool's value rides in its own field
    // header — so there is nothing to read *only* when the field really is
    // one. Any other type carries a payload, and claiming the field without
    // consuming that payload leaves the rest of the footer misaligned.
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.INTEGER);
        writer.fieldI8(1, 32);
        writer.fieldString(2, "nope");
        writer.structEnd();
      }),
    );
  });

  it("does not read an absent isSigned as an unsigned column", () => {
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.INTEGER);
        writer.fieldI8(1, 8);
        writer.structEnd();
      }),
    );
  });

  it("refuses a UTC flag that is not a bool", () => {
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.TIMESTAMP);
        writer.fieldI32(1, 1);
        writer.fieldStructBegin(2);
        writer.fieldStructBegin(TimeUnit.MILLIS);
        writer.structEnd();
        writer.structEnd();
        writer.structEnd();
      }),
    );
  });

  it("refuses a decimal parameter that is not the Thrift type the format declares", () => {
    // `DecimalType.scale` is an i32, which on the wire is a varint. A binary
    // field there is a length followed by bytes: reading it as a varint takes
    // the length for the scale and leaves the bytes to be parsed as fields.
    expectError("ERR_READ_MALFORMED", () =>
      decodedLogicalType((writer) => {
        writer.fieldStructBegin(LogicalTypeId.DECIMAL);
        writer.fieldString(1, "nope");
        writer.fieldI32(2, 12);
        writer.structEnd();
      }),
    );
  });

  it("reports a malformed annotation at its structural cause", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        fileWithLogicalType((writer) => {
          writer.fieldStructBegin(LogicalTypeId.INTEGER);
          writer.fieldI8(1, 32);
          writer.fieldString(2, "nope");
          writer.structEnd();
        }),
      ),
    );
    expect(error.message).toContain("IntType.isSigned");
  });
});

describe("an annotation on an illegal physical type", () => {
  it("is malformed before a caller's broad matcher can claim it", () => {
    let matches = 0;
    const broad = defineColumnType({
      name: "broad",
      physical: "i64",
      matches: () => {
        matches++;
        return true;
      },
      annotate: (): Annotation => ({
        kind: "timestamp",
        unit: "micros",
        isAdjustedToUTC: false,
      }),
      read: (raw) => raw as bigint,
      write: (value: bigint) => value,
    });
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        fileWithLogicalType((writer) => {
          writer.fieldStructBegin(LogicalTypeId.DATE);
          writer.structEnd();
        }),
        { types: [broad] },
      ),
    );
    expect(error.column).toBe("c");
    expect(error.message).toContain("DATE");
    expect(error.message).toContain("INT32");
    expect(matches).toBe(0);
  });

  it.each([
    [2, "MAP"],
    [3, "LIST"],
    [16, "VARIANT"],
    [19, "FILE"],
  ])("refuses group-only logical type %s on a primitive", (id, name) => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        fileWithLogicalType((writer) => {
          writer.fieldStructBegin(id);
          writer.structEnd();
        }),
      ),
    );
    expect(error.column).toBe("c");
    expect(error.message).toContain(name);
    expect(error.message).toContain("group");
  });

  it.each([
    [17, "GEOMETRY"],
    [18, "GEOGRAPHY"],
  ])("requires BYTE_ARRAY for logical type %s", (id, name) => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(
        fileWithLogicalType((writer) => {
          writer.fieldStructBegin(id);
          writer.structEnd();
        }),
      ),
    );
    expect(error.message).toContain(name);
    expect(error.message).toContain("BYTE_ARRAY");
  });

  it("requires FIXED_LEN_BYTE_ARRAY(12) for legacy INTERVAL", () => {
    const error = expectError("ERR_READ_MALFORMED", () =>
      readParquet(fileWithLogicalType(undefined, { convertedType: ConvertedType.INTERVAL })),
    );
    expect(error.message).toContain("INTERVAL");
    expect(error.message).toContain("FIXED_LEN_BYTE_ARRAY(12)");
  });

  it("leaves legal unsupported primitive annotations claimable", () => {
    const geometry = defineColumnType({
      name: "geometry",
      physical: "bytes",
      matches: (annotation) => annotation.kind === "unknown" && annotation.id === 17,
      annotate: (): Annotation => ({ kind: "bson" }),
      read: (raw) => raw,
      write: (value) => value,
    });
    const interval = defineColumnType({
      name: "interval",
      physical: "fixed",
      typeLength: 12,
      matches: (annotation) => annotation.kind === "unknown" && annotation.id === 9,
      annotate: (): Annotation => ({ kind: "none" }),
      read: (raw) => raw,
      write: (value) => value,
    });
    const geometryFile = fileWithLogicalType(
      (writer) => {
        writer.fieldStructBegin(17);
        writer.structEnd();
      },
      { physical: PhysicalType.BYTE_ARRAY },
    );
    const intervalFile = fileWithLogicalType(undefined, {
      physical: PhysicalType.FIXED_LEN_BYTE_ARRAY,
      typeLength: 12,
      convertedType: ConvertedType.INTERVAL,
    });

    expect(readParquet(geometryFile, { types: [geometry] }).schema.columns[0].type).toBe(geometry);
    expect(readParquet(intervalFile, { types: [interval] }).schema.columns[0].type).toBe(interval);
  });

  it("allows UNKNOWN on any primitive", () => {
    const nulls = defineColumnType({
      name: "nulls",
      physical: "i64",
      matches: (annotation) => annotation.kind === "unknown" && annotation.id === 11,
      annotate: (): Annotation => ({
        kind: "timestamp",
        unit: "micros",
        isAdjustedToUTC: false,
      }),
      read: (raw) => raw,
      write: (value) => value,
    });
    const file = fileWithLogicalType((writer) => {
      writer.fieldStructBegin(11);
      writer.structEnd();
    });
    expect(readParquet(file, { types: [nulls] }).schema.columns[0].type).toBe(nulls);
  });

  it.each([
    [0, 0],
    [4, -1],
    [4, 5],
  ])("refuses DECIMAL(%s, %s) parameters before matching", (precision, scale) => {
    let matches = 0;
    const broad = defineColumnType({
      name: "broad-bytes",
      physical: "bytes",
      matches: () => {
        matches++;
        return true;
      },
      annotate: (): Annotation => ({ kind: "json" }),
      read: (raw) => raw,
      write: (value) => value,
    });
    const file = fileWithLogicalType(
      (writer) => {
        writer.fieldStructBegin(LogicalTypeId.DECIMAL);
        writer.fieldI32(1, scale);
        writer.fieldI32(2, precision);
        writer.structEnd();
      },
      { physical: PhysicalType.BYTE_ARRAY },
    );
    const error = expectError("ERR_READ_MALFORMED", () => readParquet(file, { types: [broad] }));
    expect(error.message).toContain("DECIMAL");
    expect(matches).toBe(0);
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
    // A refusal names known unsupported members and future members alike.
    [{ kind: "unknown", id: LogicalTypeId.NULL }, "UNKNOWN"],
    [{ kind: "unknown", id: LogicalTypeId.VARIANT }, "VARIANT"],
    [{ kind: "unknown", id: LogicalTypeId.GEOMETRY }, "GEOMETRY"],
    [{ kind: "unknown", id: LogicalTypeId.GEOGRAPHY }, "GEOGRAPHY"],
    [{ kind: "unknown", id: LogicalTypeId.FILE }, "FILE"],
    [{ kind: "unknown", id: 42 }, "logical type 42"],
    [{ kind: "unknown", id: 0 }, "an annotation this version has no name for"],
  ] as [Annotation, string][])("names %o", (annotation, name) => {
    expect(annotationName(annotation)).toBe(name);
  });
});
