import { malformed } from "../error.ts";

/**
 * A growable little-endian byte sink.
 *
 * Deliberately built on nothing but `Uint8Array` / `DataView` so the core of
 * the library stays usable on any JavaScript runtime (Node, Deno, Bun, workers,
 * browsers) without a single import.
 */
export class ByteWriter {
  #bytes: Uint8Array;
  #view: DataView;
  #length = 0;

  constructor(initialCapacity = 1024) {
    const buffer = new ArrayBuffer(Math.max(16, initialCapacity));
    this.#bytes = new Uint8Array(buffer);
    this.#view = new DataView(buffer);
  }

  /** Number of bytes written so far. Doubles as the current file offset. */
  get length(): number {
    return this.#length;
  }

  #reserve(extra: number): number {
    const offset = this.#length;
    const required = offset + extra;
    if (required > this.#bytes.length) {
      let capacity = this.#bytes.length * 2;
      while (capacity < required) capacity *= 2;
      const buffer = new ArrayBuffer(capacity);
      const bytes = new Uint8Array(buffer);
      bytes.set(this.#bytes.subarray(0, offset));
      this.#bytes = bytes;
      this.#view = new DataView(buffer);
    }
    this.#length = required;
    return offset;
  }

  /** Appends a single byte (only the low 8 bits are kept). */
  u8(value: number): void {
    const offset = this.#reserve(1);
    this.#bytes[offset] = value;
  }

  /** Appends an unsigned 32-bit integer, little-endian. */
  u32(value: number): void {
    const offset = this.#reserve(4);
    this.#view.setUint32(offset, value, true);
  }

  /** Appends a signed 32-bit integer, little-endian. */
  i32(value: number): void {
    const offset = this.#reserve(4);
    this.#view.setInt32(offset, value, true);
  }

  /** Appends a signed 64-bit integer, little-endian. */
  i64(value: bigint): void {
    const offset = this.#reserve(8);
    this.#view.setBigInt64(offset, value, true);
  }

  /**
   * Appends an IEEE-754 single, little-endian. The value is rounded to single
   * precision here, once, and every read of it afterwards is exact.
   */
  f32(value: number): void {
    const offset = this.#reserve(4);
    this.#view.setFloat32(offset, value, true);
  }

  /** Appends an IEEE-754 double, little-endian. */
  f64(value: number): void {
    const offset = this.#reserve(8);
    this.#view.setFloat64(offset, value, true);
  }

  /** Appends raw bytes verbatim. */
  raw(bytes: Uint8Array): void {
    const offset = this.#reserve(bytes.length);
    this.#bytes.set(bytes, offset);
  }

  /**
   * Appends an unsigned LEB128 (varint) encoded integer.
   *
   * `value` must be a non-negative safe integer; 64-bit magnitudes go through
   * {@link varintBig}.
   */
  varint(value: number): void {
    // `%` / `Math.floor` rather than bitwise ops: bitwise coerces to int32 and
    // would corrupt RLE run lengths above 2^31.
    let rest = value;
    while (rest > 0x7f) {
      this.u8((rest % 128) | 0x80);
      rest = Math.floor(rest / 128);
    }
    this.u8(rest);
  }

  /** Appends an unsigned LEB128 (varint) encoded integer of arbitrary width. */
  varintBig(value: bigint): void {
    let rest = value;
    while (rest > 0x7fn) {
      this.u8(Number(rest & 0x7fn) | 0x80);
      rest >>= 7n;
    }
    this.u8(Number(rest));
  }

  /**
   * Reserves a single byte and returns its absolute offset so it can be
   * back-patched later with {@link patch}. Used by the RLE encoder, whose
   * bit-packed run headers are only known once the run ends.
   */
  reserveByte(): number {
    const offset = this.#reserve(1);
    this.#bytes[offset] = 0;
    return offset;
  }

  /** Overwrites a previously reserved byte. */
  patch(offset: number, value: number): void {
    this.#bytes[offset] = value;
  }

  /** Returns a copy of everything written so far. */
  toBytes(): Uint8Array {
    return this.#bytes.slice(0, this.#length);
  }
}

/**
 * A bounds-checked little-endian byte source: the mirror of {@link ByteWriter}.
 *
 * Every read is validated against the end of the buffer, so a truncated or
 * otherwise malformed file raises a typed error at the exact byte that ran out
 * instead of quietly producing `NaN`s, empty strings or an endless loop.
 */
export class ByteReader {
  readonly #bytes: Uint8Array;
  readonly #view: DataView;
  #offset: number;

  constructor(bytes: Uint8Array, offset = 0) {
    this.#bytes = bytes;
    this.#view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    this.#offset = offset;
  }

  /** Current read position. */
  get offset(): number {
    return this.#offset;
  }

  /** Total number of bytes in the buffer. */
  get length(): number {
    return this.#bytes.length;
  }

  /** Moves the read position; the target must lie inside the buffer. */
  seek(offset: number): void {
    if (!Number.isSafeInteger(offset) || offset < 0 || offset > this.#bytes.length) {
      throw malformed(`Offset ${offset} lies outside the ${this.#bytes.length} byte file`);
    }
    this.#offset = offset;
  }

  /** Claims `size` bytes and returns where they start, or throws if they are not there. */
  #take(size: number): number {
    const offset = this.#offset;
    if (!Number.isSafeInteger(size) || size < 0 || offset + size > this.#bytes.length) {
      throw malformed(
        `Truncated input: ${size} bytes were needed at offset ${offset}, ${
          this.#bytes.length - offset
        } are left`,
      );
    }
    this.#offset = offset + size;
    return offset;
  }

  /** Reads a single byte. */
  u8(): number {
    return this.#bytes[this.#take(1)];
  }

  /** Reads an unsigned 32-bit integer, little-endian. */
  u32(): number {
    return this.#view.getUint32(this.#take(4), true);
  }

  /** Reads a signed 32-bit integer, little-endian. */
  i32(): number {
    return this.#view.getInt32(this.#take(4), true);
  }

  /** Reads a signed 64-bit integer, little-endian. */
  i64(): bigint {
    return this.#view.getBigInt64(this.#take(8), true);
  }

  /** Reads an IEEE-754 single, little-endian. */
  f32(): number {
    return this.#view.getFloat32(this.#take(4), true);
  }

  /** Reads an IEEE-754 double, little-endian. */
  f64(): number {
    return this.#view.getFloat64(this.#take(8), true);
  }

  /** Returns a view — not a copy — over the next `size` bytes. */
  raw(size: number): Uint8Array {
    const offset = this.#take(size);
    return this.#bytes.subarray(offset, offset + size);
  }

  /** Advances past `size` bytes. */
  skip(size: number): void {
    this.#take(size);
  }

  /**
   * Reads an unsigned LEB128 (varint) encoded integer.
   *
   * Accumulates by multiplication rather than shifting, mirroring
   * {@link ByteWriter.varint}: bitwise operators coerce to int32 and would
   * corrupt values above 2^31. Anything past ten bytes, or past the safe
   * integer range, is a malformed file rather than a silently rounded number.
   */
  varint(): number {
    let result = 0;
    let scale = 1;
    for (let index = 0; index < 10; index++) {
      const byte = this.u8();
      result += (byte % 128) * scale;
      if (byte < 0x80) {
        if (!Number.isSafeInteger(result)) {
          throw malformed(`Varint ending at offset ${this.#offset} is too large to be a length`);
        }
        return result;
      }
      scale *= 128;
    }
    throw malformed(`Varint ending at offset ${this.#offset} is longer than ten bytes`);
  }

  /** Reads an unsigned LEB128 (varint) encoded integer of arbitrary width. */
  varintBig(): bigint {
    let result = 0n;
    let shift = 0n;
    for (let index = 0; index < 10; index++) {
      const byte = this.u8();
      result |= BigInt(byte % 128) << shift;
      if (byte < 0x80) return result;
      shift += 7n;
    }
    throw malformed(`Varint ending at offset ${this.#offset} is longer than ten bytes`);
  }
}

/** Shared UTF-8 encoder; `TextEncoder` is a web standard available everywhere. */
export const utf8: TextEncoder = /* @__PURE__ */ new TextEncoder();

/**
 * Shared UTF-8 decoder, the mirror of {@link utf8}. `fatal` makes garbage
 * bytes malformed; `ignoreBOM` keeps a leading U+FEFF as content rather than
 * treating its bytes as a transport signature.
 */
const utf8Decoder: TextDecoder = /* @__PURE__ */ new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: true,
});

/** Decodes UTF-8 bytes, turning a decoding failure into a typed error. */
export function decodeUtf8(bytes: Uint8Array): string {
  try {
    return utf8Decoder.decode(bytes);
  } catch (cause) {
    throw malformed(`Invalid UTF-8 in a ${bytes.length} byte string`, undefined, cause);
  }
}
