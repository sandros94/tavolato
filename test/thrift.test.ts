import { describe, expect, it } from "vitest";
import { ByteReader, ByteWriter } from "../src/internal/bytes.ts";
import {
  CompactReader,
  CompactWriter,
  ThriftType,
  unzigzag32,
  unzigzag64,
  zigzag32,
  zigzag64,
} from "../src/internal/thrift.ts";
import { expectError } from "./_errors.ts";

/**
 * Every expectation here is a byte sequence derived by hand from the Thrift
 * compact protocol specification, not from a reference implementation.
 *
 * @see https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
 */

function hex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join(" ");
}

function encode(build: (writer: CompactWriter) => void): string {
  const writer = new CompactWriter();
  writer.structBegin();
  build(writer);
  writer.structEnd();
  return hex(writer.toBytes());
}

function unhex(text: string): Uint8Array {
  return new Uint8Array(text.split(" ").map((byte) => Number.parseInt(byte, 16)));
}

/** Opens a reader over hand-written hex. */
function reader(text: string): CompactReader {
  return new CompactReader(new ByteReader(unhex(text)));
}

/**
 * Reads a struct made of one field the reader does not know, followed by field
 * 2 holding the i32 `7`. Returns that `7` only if the unknown field was skipped
 * by exactly the right number of bytes.
 */
function skipsPast(unknownField: string): number {
  const compact = reader(`${unknownField} 15 0e 00`);
  let value = -1;
  compact.structBegin();
  for (let field = compact.fieldBegin(); field !== null; field = compact.fieldBegin()) {
    if (field.id === 2) value = compact.i32();
    else compact.skip(field.type);
  }
  compact.structEnd();
  return value;
}

describe("varint", () => {
  it("encodes the example from the specification", () => {
    // The spec walks through 50399 → 0xDF 0x89 0x03.
    const out = new ByteWriter();
    out.varint(50_399);
    expect(hex(out.toBytes())).toBe("df 89 03");
  });

  it("encodes small values in a single byte", () => {
    const out = new ByteWriter();
    out.varint(0);
    out.varint(1);
    out.varint(127);
    expect(hex(out.toBytes())).toBe("00 01 7f");
  });

  it("encodes 128 as two bytes", () => {
    const out = new ByteWriter();
    out.varint(128);
    expect(hex(out.toBytes())).toBe("80 01");
  });

  it("stays exact above 2^31", () => {
    const out = new ByteWriter();
    out.varint(2 ** 32); // 4294967296
    expect(hex(out.toBytes())).toBe("80 80 80 80 10");
  });

  it("encodes 64-bit magnitudes", () => {
    const out = new ByteWriter();
    out.varintBig(2n ** 64n - 2n);
    expect(hex(out.toBytes())).toBe("fe ff ff ff ff ff ff ff ff 01");
  });
});

describe("zigzag", () => {
  it("maps 32-bit integers to the unsigned domain", () => {
    expect(zigzag32(0)).toBe(0);
    expect(zigzag32(-1)).toBe(1);
    expect(zigzag32(1)).toBe(2);
    expect(zigzag32(-2)).toBe(3);
    expect(zigzag32(2)).toBe(4);
    expect(zigzag32(2_147_483_647)).toBe(4_294_967_294);
    expect(zigzag32(-2_147_483_648)).toBe(4_294_967_295);
  });

  it("maps 64-bit integers to the unsigned domain", () => {
    expect(zigzag64(0n)).toBe(0n);
    expect(zigzag64(-1n)).toBe(1n);
    expect(zigzag64(1n)).toBe(2n);
    expect(zigzag64(2n ** 63n - 1n)).toBe(2n ** 64n - 2n);
    expect(zigzag64(-(2n ** 63n))).toBe(2n ** 64n - 1n);
  });
});

describe("CompactWriter", () => {
  it("writes an empty struct as a lone stop field", () => {
    expect(encode(() => {})).toBe("00");
  });

  it("writes i32 fields with a short field header", () => {
    // 0x15 = delta 1 << 4 | I32(5); zigzag(1) = 2.
    expect(encode((w) => w.fieldI32(1, 1))).toBe("15 02 00");
  });

  it("writes i8 fields as one raw byte, not a zigzag varint", () => {
    // 0x13 = delta 1 << 4 | I8(3); the value follows verbatim. This is the one
    // integer the compact protocol does not transform, and `IntType.bitWidth`
    // is the only place Parquet uses it.
    expect(encode((w) => w.fieldI8(1, 64))).toBe("13 40 00");
    expect(encode((w) => w.fieldI8(1, 8))).toBe("13 08 00");
  });

  it("writes booleans inside the field header", () => {
    // BOOLEAN_TRUE = 1, BOOLEAN_FALSE = 2; no payload bytes follow.
    expect(
      encode((w) => {
        w.fieldBool(1, true);
        w.fieldBool(2, false);
      }),
    ).toBe("11 12 00");
  });

  it("writes strings as length-prefixed UTF-8", () => {
    // 0x48 = delta 4 << 4 | BINARY(8); length 3; "abc".
    expect(encode((w) => w.fieldString(4, "abc"))).toBe("48 03 61 62 63 00");
  });

  it("writes empty binary as a zero length", () => {
    expect(encode((w) => w.fieldBinary(1, new Uint8Array()))).toBe("18 00 00");
  });

  it("writes UTF-8 multi-byte strings by byte length", () => {
    // "é" is two bytes in UTF-8, so the length prefix is 2 rather than 1.
    expect(encode((w) => w.fieldString(1, "é"))).toBe("18 02 c3 a9 00");
  });

  it("writes i64 fields as zigzag varints", () => {
    // 0x16 = delta 1 << 4 | I64(6); zigzag(-1) = 1.
    expect(encode((w) => w.fieldI64(1, -1n))).toBe("16 01 00");
  });

  it("writes the largest i64 as a ten byte varint", () => {
    expect(encode((w) => w.fieldI64(1, 2n ** 63n - 1n))).toBe(
      "16 fe ff ff ff ff ff ff ff ff 01 00",
    );
  });

  it("writes doubles little-endian", () => {
    // 0x17 = delta 1 << 4 | DOUBLE(7); 1.0 is 3FF0000000000000 big-endian.
    expect(encode((w) => w.fieldDouble(1, 1))).toBe("17 00 00 00 00 00 00 f0 3f 00");
  });

  it("writes short-form list headers", () => {
    // 0x29 = delta 2 << 4 | LIST(9); 0x35 = size 3 << 4 | I32(5).
    expect(
      encode((w) => {
        w.fieldListBegin(2, ThriftType.I32, 3);
        w.elementI32(0);
        w.elementI32(3);
        w.elementI32(-1);
      }),
    ).toBe("29 35 00 06 01 00");
  });

  it("switches to the long-form list header at 15 elements", () => {
    // 0xF8 = size marker 0xF << 4 | BINARY(8), then the real size as a varint.
    expect(
      encode((w) => {
        w.fieldListBegin(1, ThriftType.BINARY, 15);
        for (let index = 0; index < 15; index++) w.elementBinary(new Uint8Array());
      }),
    ).toBe(`19 f8 0f ${"00 ".repeat(15).trim()} 00`);
  });

  it("writes an empty list with a zero size nibble", () => {
    expect(encode((w) => w.fieldListBegin(1, ThriftType.STRUCT, 0))).toBe("19 0c 00");
  });

  it("switches to the long-form field header past a delta of 15", () => {
    // Field 20 after field 1 is a delta of 19: type byte alone, then zigzag(20) = 40.
    expect(
      encode((w) => {
        w.fieldI32(1, 0);
        w.fieldI32(20, 0);
      }),
    ).toBe("15 00 05 28 00 00");
  });

  it("restores the enclosing field id after a nested struct", () => {
    // Inner struct restarts deltas at 0; the outer field 4 is then a delta of 1.
    expect(
      encode((w) => {
        w.fieldStructBegin(3);
        w.fieldI32(1, 0);
        w.structEnd();
        w.fieldI32(4, 0);
      }),
    ).toBe("3c 15 00 00 15 00 00");
  });

  it("restores the enclosing field id across list elements", () => {
    expect(
      encode((w) => {
        w.fieldListBegin(1, ThriftType.STRUCT, 2);
        for (let index = 0; index < 2; index++) {
          w.structBegin();
          w.fieldI32(1, 7);
          w.structEnd();
        }
        w.fieldI32(2, 0);
      }),
    ).toBe("19 2c 15 0e 00 15 0e 00 15 00 00");
  });
});

describe("ByteReader varint", () => {
  it("reads the example from the specification", () => {
    expect(new ByteReader(unhex("df 89 03")).varint()).toBe(50_399);
  });

  it("reads values above 2^31 exactly", () => {
    expect(new ByteReader(unhex("80 80 80 80 10")).varint()).toBe(2 ** 32);
  });

  it("reads 64-bit magnitudes", () => {
    expect(new ByteReader(unhex("fe ff ff ff ff ff ff ff ff 01")).varintBig()).toBe(2n ** 64n - 2n);
  });

  it("refuses a varint that never terminates", () => {
    expectError("ERR_READ_MALFORMED", () =>
      new ByteReader(unhex("ff ff ff ff ff ff ff ff ff ff ff")).varint(),
    );
  });

  it("refuses a value too large to be a length", () => {
    // Ten continuation-free bytes worth of payload is far past 2^53.
    expectError("ERR_READ_MALFORMED", () =>
      new ByteReader(unhex("ff ff ff ff ff ff ff ff ff 7f")).varint(),
    );
  });

  it("refuses to read past the end", () => {
    expectError("ERR_READ_MALFORMED", () => new ByteReader(unhex("01 02")).u32());
  });
});

describe("unzigzag", () => {
  it("inverts zigzag32", () => {
    for (const value of [0, -1, 1, -2, 2, 2_147_483_647, -2_147_483_648]) {
      expect(unzigzag32(zigzag32(value))).toBe(value);
    }
  });

  it("inverts zigzag64", () => {
    for (const value of [0n, -1n, 1n, 2n ** 63n - 1n, -(2n ** 63n)]) {
      expect(unzigzag64(zigzag64(value))).toBe(value);
    }
  });
});

describe("CompactReader", () => {
  it("reads back everything CompactWriter writes", () => {
    const writer = new CompactWriter();
    writer.structBegin();
    writer.fieldBool(1, true);
    writer.fieldBool(2, false);
    writer.fieldI32(3, -1);
    writer.fieldI64(4, 2n ** 63n - 1n);
    writer.fieldDouble(5, 1.5);
    writer.fieldString(6, "日本語");
    writer.fieldListBegin(20, ThriftType.I32, 3); // past a delta of 15: long form
    writer.elementI32(1);
    writer.elementI32(-2);
    writer.elementI32(3);
    writer.fieldStructBegin(21);
    writer.fieldI32(1, 9);
    writer.structEnd();
    writer.structEnd();

    const compact = new CompactReader(new ByteReader(writer.toBytes()));
    const seen: unknown[] = [];
    compact.structBegin();
    for (let field = compact.fieldBegin(); field !== null; field = compact.fieldBegin()) {
      switch (field.id) {
        case 1:
        case 2: {
          seen.push(compact.bool(field.type));
          break;
        }
        case 3: {
          seen.push(compact.i32());
          break;
        }
        case 4: {
          seen.push(compact.i64());
          break;
        }
        case 5: {
          seen.push(compact.double());
          break;
        }
        case 6: {
          seen.push(compact.string());
          break;
        }
        case 20: {
          const { elementType, size } = compact.listBegin();
          seen.push(elementType, size);
          for (let index = 0; index < size; index++) seen.push(compact.i32());
          break;
        }
        default: {
          compact.skip(field.type);
        }
      }
    }
    compact.structEnd();

    expect(seen).toEqual([
      true,
      false,
      -1,
      2n ** 63n - 1n,
      1.5,
      "日本語",
      ThriftType.I32,
      3,
      1,
      -2,
      3,
    ]);
  });

  it("reads an i8 back, sign extended", () => {
    for (const value of [0, 8, 16, 32, 64, 127, -1, -128]) {
      const writer = new CompactWriter();
      writer.structBegin();
      writer.fieldI8(1, value);
      writer.structEnd();
      const compact = new CompactReader(new ByteReader(writer.toBytes()));
      compact.structBegin();
      expect(compact.fieldBegin()).toEqual({ id: 1, type: ThriftType.I8 });
      expect(compact.i8()).toBe(value);
      compact.structEnd();
    }
  });

  it("reads an empty struct as no fields at all", () => {
    const compact = reader("00");
    compact.structBegin();
    expect(compact.fieldBegin()).toBeNull();
    compact.structEnd();
  });

  it("reads the long-form field header", () => {
    // 05 28 = type I32 alone, then zigzag(20) = 40.
    const compact = reader("05 28 00 00");
    compact.structBegin();
    expect(compact.fieldBegin()).toEqual({ id: 20, type: ThriftType.I32 });
    expect(compact.i32()).toBe(0);
    compact.structEnd();
  });

  it("reads the long-form list header", () => {
    const compact = reader("f8 0f");
    expect(compact.listBegin()).toEqual({ elementType: ThriftType.BINARY, size: 15 });
  });

  it("skips a field of every compact type", () => {
    const unknowns: Record<string, string> = {
      "boolean (value in the header)": "11",
      "i8": "13 7f",
      "i16": "14 0e",
      "i32": "15 0e",
      "i64": "16 0e",
      "double": "17 00 00 00 00 00 00 f0 3f",
      "binary": "18 03 61 62 63",
      "uuid": `1d ${"00 ".repeat(16).trim()}`,
      "list of i32": "19 25 02 04",
      "set of booleans": "1a 21 01 02",
      "map of binary to i32": "1b 01 85 01 61 02",
      "empty map (no key/value type byte)": "1b 00",
      "struct": "1c 15 02 00",
      "nested struct in a list": "19 1c 15 02 00",
    };
    // Paired with its label so a failure names the type that mis-skipped.
    expect(Object.entries(unknowns).map(([what, unknown]) => [what, skipsPast(unknown)])).toEqual(
      Object.keys(unknowns).map((what) => [what, 7]),
    );
  });

  it("refuses an unknown compact type", () => {
    // Type 14 is not assigned by the compact protocol.
    expectError("ERR_READ_MALFORMED", () => skipsPast("1e"));
  });

  it("refuses a value that nests past the depth cap", () => {
    // 0x1c opens a struct whose only field opens another, forever.
    expectError("ERR_READ_MALFORMED", () => skipsPast("1c ".repeat(200).trim()));
  });

  it("refuses a container that claims more elements than there are bytes", () => {
    // A list header claiming 2^20 i32 elements over an empty payload.
    expectError("ERR_READ_MALFORMED", () => skipsPast("19 f5 80 80 40"));
  });
});
