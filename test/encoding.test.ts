import { describe, expect, it } from "vitest";
import { ByteReader, ByteWriter } from "../src/internal/bytes.ts";
import {
  bitWidthForMaxLevel,
  type ColumnValues,
  decodeRleBitPackedHybrid,
  encodeRleBitPackedHybrid,
  readPlain,
  writePlain,
} from "../src/internal/encoding.ts";
import { expectError } from "./_errors.ts";

/**
 * Expectations derived by hand from Encodings.md.
 *
 * @see https://github.com/apache/parquet-format/blob/master/Encodings.md
 */

function hex(bytes: Uint8Array): string {
  return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join(" ");
}

function plain(values: Parameters<typeof writePlain>[1]): string {
  const out = new ByteWriter();
  writePlain(out, values);
  return hex(out.toBytes());
}

describe("bitWidthForMaxLevel", () => {
  it("reports zero bits for a column that cannot be null", () => {
    expect(bitWidthForMaxLevel(0)).toBe(0);
  });

  it("reports the minimum width for a max level", () => {
    expect(bitWidthForMaxLevel(1)).toBe(1);
    expect(bitWidthForMaxLevel(2)).toBe(2);
    expect(bitWidthForMaxLevel(3)).toBe(2);
    expect(bitWidthForMaxLevel(4)).toBe(3);
    expect(bitWidthForMaxLevel(7)).toBe(3);
    expect(bitWidthForMaxLevel(8)).toBe(4);
  });
});

describe("RLE / bit-packing hybrid", () => {
  it("emits an RLE run for eight identical values", () => {
    // rle-header = varint(8 << 1) = 0x10, then the repeated value on one byte.
    expect(
      hex(
        encodeRleBitPackedHybrid(
          Array.from({ length: 8 }, () => 1),
          1,
        ),
      ),
    ).toBe("10 01");
  });

  it("emits an RLE run for a long run of nulls", () => {
    // 1000 levels of 0: varint(2000) = 0xD0 0x0F, then the value 0.
    expect(
      hex(
        encodeRleBitPackedHybrid(
          Array.from({ length: 1000 }, () => 0),
          1,
        ),
      ),
    ).toBe("d0 0f 00");
  });

  it("bit-packs alternating values least-significant-bit first", () => {
    // bit-packed-header = varint(1 << 1 | 1) = 0x03; bits 0,2,4,6 set = 0x55.
    expect(hex(encodeRleBitPackedHybrid([1, 0, 1, 0, 1, 0, 1, 0], 1))).toBe("03 55");
  });

  it("pads the final group only", () => {
    // Three values become one padded group: bits 0 and 2 set = 0x05.
    expect(hex(encodeRleBitPackedHybrid([1, 0, 1], 1))).toBe("03 05");
  });

  it("mixes bit-packed and RLE runs", () => {
    const alternating = [0, 1, 0, 1, 0, 1, 0, 1]; // one bit-packed group -> 0xAA
    const identical = Array.from({ length: 8 }, () => 1); // -> RLE run of 8
    const remainder = [0]; // -> a final, padded bit-packed group
    expect(hex(encodeRleBitPackedHybrid([...alternating, ...identical, ...remainder], 1))).toBe(
      "03 aa 10 01 03 00",
    );
  });

  it("handles a single value", () => {
    expect(hex(encodeRleBitPackedHybrid([1], 1))).toBe("03 01");
  });

  it("handles an empty level list", () => {
    expect(hex(encodeRleBitPackedHybrid([], 1))).toBe("");
  });

  it("caps a bit-packed run at 63 groups so its header stays one byte", () => {
    // 1000 alternating values never repeat eight times, so everything is
    // bit-packed: 125 groups of 0x55, split as a full 63-group run
    // (header (63 << 1) | 1 = 0x7f) and a 62-group run ((62 << 1) | 1 = 0x7d).
    const levels = Array.from({ length: 1000 }, (_, index) => (index % 2 === 0 ? 1 : 0));
    expect(hex(encodeRleBitPackedHybrid(levels, 1))).toBe(
      `7f ${"55 ".repeat(63)}7d ${"55 ".repeat(62)}`.trim(),
    );
  });

  it("packs wider bit widths across byte boundaries", () => {
    // The worked example from Encodings.md: 0..7 at bit width 3.
    expect(hex(encodeRleBitPackedHybrid([0, 1, 2, 3, 4, 5, 6, 7], 3))).toBe("03 88 c6 fa");
  });

  it("writes the repeated value on a whole number of bytes", () => {
    // Bit width 9 rounds up to a two byte repeated value.
    expect(
      hex(
        encodeRleBitPackedHybrid(
          Array.from({ length: 8 }, () => 300),
          9,
        ),
      ),
    ).toBe("10 2c 01");
  });
});

describe("PLAIN encoding", () => {
  it("writes BYTE_ARRAY with a four byte little-endian length", () => {
    expect(
      plain({
        kind: "bytes",
        items: [new Uint8Array(), new Uint8Array([0x61])],
      }),
    ).toBe("00 00 00 00 01 00 00 00 61");
  });

  it("writes INT64 little-endian", () => {
    expect(plain({ kind: "i64", items: [1n, -1n] })).toBe(
      "01 00 00 00 00 00 00 00 ff ff ff ff ff ff ff ff",
    );
  });

  it("writes INT32 little-endian", () => {
    expect(plain({ kind: "i32", items: [1, -1, 2 ** 31 - 1] })).toBe(
      "01 00 00 00 ff ff ff ff ff ff ff 7f",
    );
  });

  it("writes DOUBLE little-endian", () => {
    expect(plain({ kind: "f64", items: [1] })).toBe("00 00 00 00 00 00 f0 3f");
  });

  it("writes FLOAT little-endian", () => {
    expect(plain({ kind: "f32", items: [1, -2] })).toBe("00 00 80 3f 00 00 00 c0");
  });

  it("writes FIXED_LEN_BYTE_ARRAY without a length prefix", () => {
    // The width lives in the schema, so the values are simply concatenated.
    expect(
      plain({
        kind: "fixed",
        typeLength: 2,
        items: [new Uint8Array([0x00, 0x3c]), new Uint8Array([0xff, 0x7b])],
      }),
    ).toBe("00 3c ff 7b");
  });

  it("bit-packs BOOLEAN least-significant-bit first", () => {
    expect(plain({ kind: "bool", items: [true, false, true] })).toBe("05");
  });

  it("spills BOOLEAN into a second byte past eight values", () => {
    expect(plain({ kind: "bool", items: Array.from({ length: 9 }, () => true) })).toBe("ff 01");
  });

  it("writes nothing for an empty column", () => {
    expect(plain({ kind: "bool", items: [] })).toBe("");
  });
});

function unhex(text: string): Uint8Array {
  return text === ""
    ? new Uint8Array()
    : new Uint8Array(text.split(" ").map((byte) => Number.parseInt(byte, 16)));
}

/** Encodes, then decodes, and returns what came back. */
function levelRoundtrip(levels: readonly number[], bitWidth: number): number[] {
  return decodeRleBitPackedHybrid(
    encodeRleBitPackedHybrid(levels, bitWidth),
    bitWidth,
    levels.length,
  );
}

describe("RLE / bit-packing hybrid decoding", () => {
  it("decodes the hand-written fixtures the encoder produces", () => {
    expect(decodeRleBitPackedHybrid(unhex("10 01"), 1, 8)).toEqual(
      Array.from({ length: 8 }, () => 1),
    );
    expect(decodeRleBitPackedHybrid(unhex("d0 0f 00"), 1, 1000)).toEqual(
      Array.from({ length: 1000 }, () => 0),
    );
    expect(decodeRleBitPackedHybrid(unhex("03 55"), 1, 8)).toEqual([1, 0, 1, 0, 1, 0, 1, 0]);
    expect(decodeRleBitPackedHybrid(unhex("03 88 c6 fa"), 3, 8)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(decodeRleBitPackedHybrid(unhex("10 2c 01"), 9, 8)).toEqual(
      Array.from({ length: 8 }, () => 300),
    );
  });

  it("stops at count, discarding a final group's padding", () => {
    // "03 05" holds three real values and five zeroes of padding.
    expect(decodeRleBitPackedHybrid(unhex("03 05"), 1, 3)).toEqual([1, 0, 1]);
  });

  it("refuses trailing encoded runs or bytes after the requested levels", () => {
    expectError("ERR_READ_MALFORMED", () => decodeRleBitPackedHybrid(unhex("03 05 10 01"), 1, 3));
    expectError("ERR_READ_MALFORMED", () => decodeRleBitPackedHybrid(unhex("03 05 ff"), 1, 3));
  });

  it("refuses an RLE run that exceeds the requested level count", () => {
    expectError("ERR_READ_MALFORMED", () => decodeRleBitPackedHybrid(unhex("10 01"), 1, 3));
  });

  it("accepts a final bit-packed run padded to 32 groups", () => {
    const bytes = new Uint8Array([0x41, 0x05, ...new Uint8Array(31)]);
    expect(decodeRleBitPackedHybrid(bytes, 1, 3)).toEqual([1, 0, 1]);
  });

  it("refuses a bit-packed run beyond Parquet's signed-i32 value limit", () => {
    // 268,435,456 groups => 2,147,483,648 values; the header is enough to
    // reject it, without reading or allocating its impossible body.
    const error = expectError("ERR_READ_MALFORMED", () =>
      decodeRleBitPackedHybrid(new Uint8Array([0x81, 0x80, 0x80, 0x80, 0x02]), 1, 1, "n"),
    );
    expect(error.message).toContain("268435456 groups");
    expect(error.column).toBe("n");
  });

  it("decodes a mix of bit-packed and RLE runs", () => {
    expect(decodeRleBitPackedHybrid(unhex("03 aa 10 01 03 00"), 1, 17)).toEqual([
      0, 1, 0, 1, 0, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0,
    ]);
  });

  it("inverts the encoder for every shape it can produce", () => {
    const shapes: readonly (readonly number[])[] = [
      [],
      [1],
      [0],
      [1, 0, 1],
      Array.from({ length: 8 }, () => 1),
      Array.from({ length: 1000 }, (_, index) => (index % 2 === 0 ? 1 : 0)),
      Array.from({ length: 1003 }, (_, index) => (index % 5 === 0 ? 0 : 1)),
      Array.from({ length: 600 }, (_, index) => (index < 300 ? 0 : 1)),
    ];
    for (const levels of shapes) expect(levelRoundtrip(levels, 1)).toEqual([...levels]);
    // Wider levels are never written by tavolato, but the codec is general.
    expect(levelRoundtrip([0, 1, 2, 3, 4, 5, 6, 7], 3)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(
      levelRoundtrip(
        Array.from({ length: 20 }, () => 300),
        9,
      ),
    ).toEqual(Array.from({ length: 20 }, () => 300));
  });

  it("reads nothing when no levels are asked for", () => {
    expect(decodeRleBitPackedHybrid(new Uint8Array(), 1, 0)).toEqual([]);
  });

  it("refuses a run that would yield no values, rather than spinning", () => {
    expectError("ERR_READ_MALFORMED", () => decodeRleBitPackedHybrid(unhex("00 00"), 1, 4));
    expectError("ERR_READ_MALFORMED", () => decodeRleBitPackedHybrid(unhex("01 00"), 1, 4));
  });

  it("refuses to run off the end of the level stream", () => {
    expectError("ERR_READ_MALFORMED", () => decodeRleBitPackedHybrid(unhex("03"), 1, 8));
  });
});

describe("PLAIN decoding", () => {
  /** Writes `values`, reads them back, and returns the result. */
  function plainRoundtrip(values: ColumnValues): ColumnValues {
    const out = new ByteWriter();
    writePlain(out, values);
    return readPlain(
      new ByteReader(out.toBytes()),
      values.kind,
      values.items.length,
      values.kind === "fixed" ? values.typeLength : undefined,
    );
  }

  it("inverts BYTE_ARRAY, length prefix included", () => {
    const items = [
      new Uint8Array(),
      new Uint8Array([0x61]),
      new Uint8Array([0xf0, 0x9f, 0x8e, 0x89]),
    ];
    expect(plainRoundtrip({ kind: "bytes", items })).toEqual({ kind: "bytes", items });
  });

  it("inverts INT64 at the signed extremes", () => {
    const items = [0n, 1n, -1n, 2n ** 63n - 1n, -(2n ** 63n)];
    expect(plainRoundtrip({ kind: "i64", items })).toEqual({ kind: "i64", items });
  });

  it("inverts INT32 at the signed extremes", () => {
    const items = [0, 1, -1, 2 ** 31 - 1, -(2 ** 31)];
    expect(plainRoundtrip({ kind: "i32", items })).toEqual({ kind: "i32", items });
  });

  it("inverts DOUBLE, negative zero included", () => {
    const items = [1, -0, Number.NaN, Number.POSITIVE_INFINITY, 5e-324];
    const read = plainRoundtrip({ kind: "f64", items });
    expect(read.items).toEqual(items);
    expect(Object.is(read.items[1], -0)).toBe(true);
  });

  it("inverts FLOAT for values single precision holds exactly", () => {
    const items = [1, -0, 0.5, Number.NaN, Number.POSITIVE_INFINITY, 3.4028234663852886e38];
    const read = plainRoundtrip({ kind: "f32", items });
    expect(read.items).toEqual(items);
    expect(Object.is(read.items[1], -0)).toBe(true);
  });

  it("rounds a FLOAT once, and never again", () => {
    // 0.1 is not a single, so it moves on the way in — and then stays put.
    const once = plainRoundtrip({ kind: "f32", items: [0.1] }).items[0] as number;
    expect(once).not.toBe(0.1);
    expect(plainRoundtrip({ kind: "f32", items: [once] }).items[0]).toBe(once);
  });

  it("inverts FIXED_LEN_BYTE_ARRAY at its declared width", () => {
    const items = [new Uint8Array([1, 2, 3]), new Uint8Array([0xff, 0, 0xff])];
    expect(plainRoundtrip({ kind: "fixed", typeLength: 3, items })).toEqual({
      kind: "fixed",
      typeLength: 3,
      items,
    });
  });

  it("inverts BOOLEAN across byte boundaries", () => {
    const items = Array.from({ length: 19 }, (_, index) => index % 3 === 0);
    expect(plainRoundtrip({ kind: "bool", items })).toEqual({ kind: "bool", items });
  });

  it("ignores the padding bits of a boolean's final byte", () => {
    // "ff 01" holds nine set bits followed by seven bits of padding.
    expect(readPlain(new ByteReader(unhex("ff 01")), "bool", 9).items).toEqual(
      Array.from({ length: 9 }, () => true),
    );
  });

  it("reads nothing for an empty column", () => {
    for (const kind of ["bytes", "fixed", "f64", "f32", "i64", "i32", "bool"] as const) {
      expect(readPlain(new ByteReader(new Uint8Array()), kind, 0, 4).items).toEqual([]);
    }
  });

  it("refuses to read past the end of a page", () => {
    expectError("ERR_READ_MALFORMED", () => readPlain(new ByteReader(unhex("01 00")), "i64", 1));
    expectError("ERR_READ_MALFORMED", () => readPlain(new ByteReader(unhex("01 00")), "i32", 1));
    expectError("ERR_READ_MALFORMED", () => readPlain(new ByteReader(unhex("01 00")), "f32", 1));
    // A BYTE_ARRAY whose length prefix promises more than the page holds.
    expectError("ERR_READ_MALFORMED", () =>
      readPlain(new ByteReader(unhex("ff 00 00 00 61")), "bytes", 1),
    );
    // A FIXED_LEN_BYTE_ARRAY whose declared width outruns the page.
    expectError("ERR_READ_MALFORMED", () =>
      readPlain(new ByteReader(unhex("61 62")), "fixed", 1, 4),
    );
  });
});
