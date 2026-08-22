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

  /** Appends a signed 64-bit integer, little-endian. */
  i64(value: bigint): void {
    const offset = this.#reserve(8);
    this.#view.setBigInt64(offset, value, true);
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

/** Shared UTF-8 encoder; `TextEncoder` is a web standard available everywhere. */
export const utf8: TextEncoder = /* @__PURE__ */ new TextEncoder();
