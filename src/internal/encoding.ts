import { ByteReader, ByteWriter } from "./bytes.ts";
import { malformed } from "../error.ts";
import type { PhysicalKind } from "../types.ts";

/**
 * Non-null column values, kept in the shape the PLAIN encoder wants so no
 * per-value type checks are needed when a page is flushed.
 *
 * One member per {@link PhysicalKind}: the buffer's kind *is* the column's
 * physical type, which is what lets an adapter name its storage and get the
 * right buffer without a second table.
 */
export type ColumnValues =
  | { readonly kind: "bytes"; readonly items: Uint8Array[] }
  /** Every item is exactly `typeLength` bytes long; the writer checks that before it buffers one. */
  | { readonly kind: "fixed"; readonly typeLength: number; readonly items: Uint8Array[] }
  | { readonly kind: "f64"; readonly items: number[] }
  | { readonly kind: "f32"; readonly items: number[] }
  | { readonly kind: "i64"; readonly items: bigint[] }
  | { readonly kind: "i32"; readonly items: number[] }
  | { readonly kind: "bool"; readonly items: boolean[] };

/**
 * Writes values using the PLAIN encoding.
 *
 * @see https://github.com/apache/parquet-format/blob/master/Encodings.md#plain-plain--0
 */
export function writePlain(out: ByteWriter, values: ColumnValues): void {
  switch (values.kind) {
    case "bytes": {
      // BYTE_ARRAY: 4-byte little-endian length, then the bytes.
      for (const item of values.items) {
        out.u32(item.length);
        out.raw(item);
      }
      break;
    }
    case "fixed": {
      // FIXED_LEN_BYTE_ARRAY: the width is in the schema, so no length prefix.
      for (const item of values.items) out.raw(item);
      break;
    }
    case "f64": {
      for (const item of values.items) out.f64(item);
      break;
    }
    case "f32": {
      for (const item of values.items) out.f32(item);
      break;
    }
    case "i64": {
      for (const item of values.items) out.i64(item);
      break;
    }
    case "i32": {
      for (const item of values.items) out.i32(item);
      break;
    }
    case "bool": {
      // BOOLEAN: one bit per value, packed least-significant-bit first. The
      // trailing partial byte is zero padded; readers stop at `num_values`.
      const { items } = values;
      for (let index = 0; index < items.length; index += 8) {
        let byte = 0;
        for (let bit = 0; bit < 8 && index + bit < items.length; bit++) {
          if (items[index + bit]) byte |= 1 << bit;
        }
        out.u8(byte);
      }
      break;
    }
  }
}

/**
 * Reads `count` values written by {@link writePlain}.
 *
 * `count` is the number of *present* values, which for a nullable column is
 * fewer than the page's `num_values`: nulls occupy a definition level but no
 * bytes in the value stream.
 *
 * `typeLength` is the schema's `type_length` and is only read for `"fixed"`,
 * the one kind whose values carry no width of their own.
 */
export function readPlain(
  input: ByteReader,
  kind: PhysicalKind,
  count: number,
  typeLength = 0,
): ColumnValues {
  switch (kind) {
    case "bytes": {
      const items: Uint8Array[] = [];
      for (let index = 0; index < count; index++) items.push(input.raw(input.u32()));
      return { kind, items };
    }
    case "fixed": {
      const items: Uint8Array[] = [];
      for (let index = 0; index < count; index++) items.push(input.raw(typeLength));
      return { kind, typeLength, items };
    }
    case "f64": {
      const items: number[] = [];
      for (let index = 0; index < count; index++) items.push(input.f64());
      return { kind, items };
    }
    case "f32": {
      const items: number[] = [];
      for (let index = 0; index < count; index++) items.push(input.f32());
      return { kind, items };
    }
    case "i64": {
      const items: bigint[] = [];
      for (let index = 0; index < count; index++) items.push(input.i64());
      return { kind, items };
    }
    case "i32": {
      const items: number[] = [];
      for (let index = 0; index < count; index++) items.push(input.i32());
      return { kind, items };
    }
    case "bool": {
      const items: boolean[] = [];
      for (let index = 0; index < count; index += 8) {
        const byte = input.u8();
        for (let bit = 0; bit < 8 && index + bit < count; bit++) {
          items.push((byte & (1 << bit)) !== 0);
        }
      }
      return { kind, items };
    }
  }
}

/** Minimum number of bits needed to hold every level up to `maxLevel`. */
export function bitWidthForMaxLevel(maxLevel: number): number {
  return maxLevel <= 0 ? 0 : 32 - Math.clz32(maxLevel);
}

/**
 * Encodes definition/repetition levels with the RLE / bit-packing hybrid
 * encoding.
 *
 * The algorithm mirrors the reference implementation: values are buffered in
 * groups of eight, a run of eight or more identical values switches to an RLE
 * run, and everything else accumulates into bit-packed runs whose one-byte
 * header is back-patched once the run ends (a bit-packed run is capped at 63
 * groups so that header always fits a single byte).
 *
 * Padding only ever happens on the final group, where the extra values fall
 * beyond `num_values` and are ignored by readers.
 *
 * @see https://github.com/apache/parquet-format/blob/master/Encodings.md#run-length-encoding--bit-packing-hybrid-rle--3
 */
export function encodeRleBitPackedHybrid(levels: readonly number[], bitWidth: number): Uint8Array {
  const out = new ByteWriter(Math.max(16, levels.length >> 2));
  const byteWidth = Math.ceil(bitWidth / 8);
  const buffered: number[] = [0, 0, 0, 0, 0, 0, 0, 0];

  let numBuffered = 0;
  let repeatCount = 0;
  let previousValue = -1;
  let bitPackedGroupCount = 0;
  let headerOffset = -1;

  const endBitPackedRun = (): void => {
    if (headerOffset < 0) return;
    out.patch(headerOffset, (bitPackedGroupCount << 1) | 1);
    headerOffset = -1;
    bitPackedGroupCount = 0;
  };

  const writeRleRun = (): void => {
    endBitPackedRun();
    out.varint(repeatCount * 2);
    for (let index = 0; index < byteWidth; index++) {
      out.u8(Math.floor(previousValue / 2 ** (8 * index)) % 256);
    }
    repeatCount = 0;
    numBuffered = 0;
  };

  const writeBitPackedGroup = (): void => {
    if (bitPackedGroupCount >= 63) endBitPackedRun();
    if (headerOffset < 0) {
      headerOffset = out.reserveByte();
      bitPackedGroupCount = 0;
    }
    let accumulator = 0;
    let accumulatedBits = 0;
    for (let index = 0; index < 8; index++) {
      accumulator += (buffered[index]! % 2 ** bitWidth) * 2 ** accumulatedBits;
      accumulatedBits += bitWidth;
      while (accumulatedBits >= 8) {
        out.u8(accumulator % 256);
        accumulator = Math.floor(accumulator / 256);
        accumulatedBits -= 8;
      }
    }
    numBuffered = 0;
    repeatCount = 0;
    bitPackedGroupCount++;
  };

  for (const level of levels) {
    if (level === previousValue) {
      repeatCount++;
      // Already in (or entering) an RLE run: keep counting, emit nothing.
      if (repeatCount >= 8) continue;
    } else {
      if (repeatCount >= 8) writeRleRun();
      repeatCount = 1;
      previousValue = level;
    }
    buffered[numBuffered++] = level;
    if (numBuffered === 8) writeBitPackedGroup();
  }

  if (repeatCount >= 8) {
    writeRleRun();
  } else if (numBuffered > 0) {
    for (let index = numBuffered; index < 8; index++) buffered[index] = 0;
    writeBitPackedGroup();
  }
  endBitPackedRun();

  return out.toBytes();
}

/**
 * Decodes exactly `count` levels written by {@link encodeRleBitPackedHybrid}.
 *
 * Runs are consumed until `count` levels have been produced; the trailing
 * padding of a final bit-packed group is read and discarded, which is what
 * makes the encoder's "pad the last group" strategy invisible here.
 *
 * A run that would yield no values at all is rejected rather than retried:
 * that is the only shape of input that could otherwise loop forever.
 */
export function decodeRleBitPackedHybrid(
  bytes: Uint8Array,
  bitWidth: number,
  count: number,
): number[] {
  const input = new ByteReader(bytes);
  const byteWidth = Math.ceil(bitWidth / 8);
  const levels: number[] = [];

  while (levels.length < count) {
    // As in the encoder, `%` and `Math.floor` stand in for bitwise operators so
    // run lengths above 2^31 survive.
    const header = input.varint();
    if (header % 2 === 1) {
      const groups = Math.floor(header / 2);
      if (groups === 0) throw malformed("A bit-packed run declares zero groups");
      for (let group = 0; group < groups && levels.length < count; group++) {
        let accumulator = 0;
        let accumulatedBits = 0;
        for (let index = 0; index < 8; index++) {
          while (accumulatedBits < bitWidth) {
            accumulator += input.u8() * 2 ** accumulatedBits;
            accumulatedBits += 8;
          }
          if (levels.length < count) levels.push(accumulator % 2 ** bitWidth);
          accumulator = Math.floor(accumulator / 2 ** bitWidth);
          accumulatedBits -= bitWidth;
        }
      }
    } else {
      const runLength = header / 2;
      if (runLength === 0) throw malformed("An RLE run declares zero values");
      let value = 0;
      for (let index = 0; index < byteWidth; index++) value += input.u8() * 2 ** (8 * index);
      for (let index = 0; index < runLength && levels.length < count; index++) levels.push(value);
    }
  }

  return levels;
}
