import { ByteWriter, utf8 } from "./bytes.ts";

/**
 * Thrift *compact protocol* type ids, as used both in struct field headers and
 * in list element headers.
 *
 * @see https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md
 */
export const ThriftType: {
  readonly BOOLEAN_TRUE: 1;
  readonly BOOLEAN_FALSE: 2;
  readonly I8: 3;
  readonly I16: 4;
  readonly I32: 5;
  readonly I64: 6;
  readonly DOUBLE: 7;
  readonly BINARY: 8;
  readonly LIST: 9;
  readonly SET: 10;
  readonly MAP: 11;
  readonly STRUCT: 12;
  readonly UUID: 13;
} = {
  BOOLEAN_TRUE: 1,
  BOOLEAN_FALSE: 2,
  I8: 3,
  I16: 4,
  I32: 5,
  I64: 6,
  DOUBLE: 7,
  BINARY: 8,
  LIST: 9,
  SET: 10,
  MAP: 11,
  STRUCT: 12,
  UUID: 13,
} as const;

/** ZigZag transform for 32-bit signed integers, returned as an unsigned value. */
export function zigzag32(value: number): number {
  return ((value << 1) ^ (value >> 31)) >>> 0;
}

/** ZigZag transform for 64-bit signed integers, returned as an unsigned value. */
export function zigzag64(value: bigint): bigint {
  return BigInt.asUintN(64, (value << 1n) ^ (value >> 63n));
}

/**
 * A minimal Thrift compact protocol *writer*.
 *
 * Only the subset Parquet footers actually need is implemented: structs
 * (including nested ones and unions, which encode identically), `bool`, `i32`,
 * `i64`, `double`, `binary` / `string`, and homogeneous `list`s of `i32`,
 * `binary` and `struct`. Maps, sets, `i8`/`i16`, `uuid` and the RPC message
 * envelope are intentionally absent — Parquet metadata never uses them.
 *
 * Field ids must be written in ascending order within a struct, which is what
 * lets the encoder always take the compact one-byte "field id delta" form.
 * Ascending order is not a protocol requirement, but it is what every Parquet
 * writer does and it keeps the output byte-comparable.
 */
export class CompactWriter {
  readonly out: ByteWriter;
  #lastFieldId = 0;
  #stack: number[] = [];

  constructor(out: ByteWriter = new ByteWriter()) {
    this.out = out;
  }

  /** Opens a struct: field id deltas restart from zero. */
  structBegin(): void {
    this.#stack.push(this.#lastFieldId);
    this.#lastFieldId = 0;
  }

  /** Writes the stop field and restores the enclosing struct's field id state. */
  structEnd(): void {
    this.out.u8(0);
    this.#lastFieldId = this.#stack.pop() ?? 0;
  }

  #header(id: number, type: number): void {
    const delta = id - this.#lastFieldId;
    if (delta > 0 && delta <= 15) {
      this.out.u8((delta << 4) | type);
    } else {
      // Long form: the type alone, then the absolute (zigzag varint) field id.
      this.out.u8(type);
      this.out.varint(zigzag32(id));
    }
    this.#lastFieldId = id;
  }

  /** Booleans carry their value in the field header itself, so they add no bytes. */
  fieldBool(id: number, value: boolean): void {
    this.#header(id, value ? ThriftType.BOOLEAN_TRUE : ThriftType.BOOLEAN_FALSE);
  }

  fieldI32(id: number, value: number): void {
    this.#header(id, ThriftType.I32);
    this.out.varint(zigzag32(value));
  }

  fieldI64(id: number, value: bigint): void {
    this.#header(id, ThriftType.I64);
    this.out.varintBig(zigzag64(value));
  }

  fieldDouble(id: number, value: number): void {
    this.#header(id, ThriftType.DOUBLE);
    this.out.f64(value);
  }

  fieldBinary(id: number, value: Uint8Array): void {
    this.#header(id, ThriftType.BINARY);
    this.out.varint(value.length);
    this.out.raw(value);
  }

  fieldString(id: number, value: string): void {
    this.fieldBinary(id, utf8.encode(value));
  }

  /** Opens a struct-typed (or union-typed) field. Pair with {@link structEnd}. */
  fieldStructBegin(id: number): void {
    this.#header(id, ThriftType.STRUCT);
    this.structBegin();
  }

  /**
   * Opens a list-typed field. The caller writes exactly `size` elements using
   * the matching `element*` helper (or `structBegin`/`structEnd` pairs when
   * `elementType` is `STRUCT`).
   */
  fieldListBegin(id: number, elementType: number, size: number): void {
    this.#header(id, ThriftType.LIST);
    if (size <= 14) {
      this.out.u8((size << 4) | elementType);
    } else {
      this.out.u8(0xf0 | elementType);
      this.out.varint(size);
    }
  }

  /** Writes one `i32` list element (no field header). */
  elementI32(value: number): void {
    this.out.varint(zigzag32(value));
  }

  /** Writes one `binary` list element (no field header). */
  elementBinary(value: Uint8Array): void {
    this.out.varint(value.length);
    this.out.raw(value);
  }

  /** Writes one `string` list element (no field header). */
  elementString(value: string): void {
    this.elementBinary(utf8.encode(value));
  }

  /** Returns a copy of the encoded bytes. */
  toBytes(): Uint8Array {
    return this.out.toBytes();
  }
}
