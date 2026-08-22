import { describe, expect, it } from "vitest";
import { ByteWriter } from "../src/internal/bytes.ts";
import { CompactWriter, ThriftType, zigzag32, zigzag64 } from "../src/internal/thrift.ts";

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
