import { describe, expect, it } from "vitest";
import { ByteWriter } from "../src/internal/bytes.ts";
import {
  bitWidthForMaxLevel,
  encodeRleBitPackedHybrid,
  writePlain,
} from "../src/internal/encoding.ts";

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

  it("writes DOUBLE little-endian", () => {
    expect(plain({ kind: "f64", items: [1] })).toBe("00 00 00 00 00 00 f0 3f");
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
