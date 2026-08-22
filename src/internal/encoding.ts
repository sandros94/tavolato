import { ByteWriter } from "./bytes.ts";

/**
 * Non-null column values, kept in the shape the PLAIN encoder wants so no
 * per-value type checks are needed when a page is flushed.
 */
export type ColumnValues =
  | { readonly kind: "bytes"; readonly items: Uint8Array[] }
  | { readonly kind: "f64"; readonly items: number[] }
  | { readonly kind: "i64"; readonly items: bigint[] }
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
    case "f64": {
      for (const item of values.items) out.f64(item);
      break;
    }
    case "i64": {
      for (const item of values.items) out.i64(item);
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
