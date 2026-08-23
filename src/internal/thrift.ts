import { ByteReader, ByteWriter, decodeUtf8, utf8 } from "./bytes.ts";
import { malformed } from "../error.ts";

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

/** Inverse of {@link zigzag32}. Truncates to 32 bits, exactly as Thrift does. */
export function unzigzag32(value: number): number {
  return (value >>> 1) ^ -(value & 1);
}

/** Inverse of {@link zigzag64}. */
export function unzigzag64(value: bigint): bigint {
  return BigInt.asIntN(64, (value >> 1n) ^ -(value & 1n));
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

/** A struct field header: the field id and the compact type id of its value. */
export interface ThriftField {
  readonly id: number;
  readonly type: number;
}

/**
 * How deep {@link CompactReader.skip} will follow nested containers before
 * calling the file malformed. Parquet metadata never nests past a handful of
 * levels; the cap is what keeps a hostile file from exhausting the JS stack.
 */
const MAX_SKIP_DEPTH = 64;

/**
 * A minimal Thrift compact protocol *reader*, the mirror of
 * {@link CompactWriter}.
 *
 * It decodes the same subset the writer emits — structs, `bool`, `i32`, `i64`,
 * `double`, `binary` / `string` and `list` — and can additionally *skip* any
 * value of any compact type, including maps, sets and types it has no accessor
 * for. Skipping is what the Thrift protocol prescribes for unrecognised
 * fields, and it is what lets a future Parquet release add footer fields
 * without breaking this reader.
 *
 * Usage is a pull loop per struct:
 *
 * ```ts
 * reader.structBegin();
 * for (let field = reader.fieldBegin(); field !== null; field = reader.fieldBegin()) {
 *   if (field.id === 1) value = reader.i32();
 *   else reader.skip(field.type);
 * }
 * reader.structEnd();
 * ```
 */
export class CompactReader {
  readonly in: ByteReader;
  #lastFieldId = 0;
  #stack: number[] = [];
  #depth = 0;

  constructor(input: ByteReader) {
    this.in = input;
  }

  /** Opens a struct: field id deltas restart from zero. */
  structBegin(): void {
    this.#stack.push(this.#lastFieldId);
    this.#lastFieldId = 0;
  }

  /** Restores the enclosing struct's field id state. */
  structEnd(): void {
    this.#lastFieldId = this.#stack.pop() ?? 0;
  }

  /** Reads the next field header, or returns `null` at the struct's stop field. */
  fieldBegin(): ThriftField | null {
    const byte = this.in.u8();
    if (byte === 0) return null;
    const type = byte & 0x0f;
    const delta = byte >> 4;
    // A zero delta means the long form: the absolute field id follows.
    const id = delta === 0 ? unzigzag32(this.in.varint()) : this.#lastFieldId + delta;
    this.#lastFieldId = id;
    return { id, type };
  }

  /** Booleans carry their value in the field header, so the type id *is* the value. */
  bool(type: number): boolean {
    return type === ThriftType.BOOLEAN_TRUE;
  }

  i32(): number {
    return unzigzag32(this.in.varint());
  }

  i64(): bigint {
    return unzigzag64(this.in.varintBig());
  }

  double(): number {
    return this.in.f64();
  }

  /** Returns a view over the field's bytes; the underlying buffer is not copied. */
  binary(): Uint8Array {
    return this.in.raw(this.in.varint());
  }

  string(): string {
    return decodeUtf8(this.binary());
  }

  /** Reads a list header. The caller then reads exactly `size` elements. */
  listBegin(): { readonly elementType: number; readonly size: number } {
    const byte = this.in.u8();
    const elementType = byte & 0x0f;
    const short = byte >> 4;
    return { elementType, size: short === 0x0f ? this.in.varint() : short };
  }

  /**
   * Skips one value of the given compact type.
   *
   * Every branch consumes at least one byte, so a bogus container size cannot
   * spin: the underlying {@link ByteReader} runs out and throws. Depth is
   * capped separately because nesting costs stack, not bytes.
   */
  skip(type: number): void {
    if (++this.#depth > MAX_SKIP_DEPTH) {
      throw malformed(
        `Thrift value at offset ${this.in.offset} nests more than ${MAX_SKIP_DEPTH} deep`,
      );
    }
    switch (type) {
      case ThriftType.BOOLEAN_TRUE:
      case ThriftType.BOOLEAN_FALSE: {
        break; // the value was the type id
      }
      case ThriftType.I8: {
        this.in.skip(1);
        break;
      }
      case ThriftType.I16:
      case ThriftType.I32: {
        this.in.varint();
        break;
      }
      case ThriftType.I64: {
        this.in.varintBig();
        break;
      }
      case ThriftType.DOUBLE: {
        this.in.skip(8);
        break;
      }
      case ThriftType.BINARY: {
        this.in.skip(this.in.varint());
        break;
      }
      case ThriftType.UUID: {
        this.in.skip(16);
        break;
      }
      case ThriftType.LIST:
      case ThriftType.SET: {
        const { elementType, size } = this.listBegin();
        for (let index = 0; index < size; index++) this.#skipElement(elementType);
        break;
      }
      case ThriftType.MAP: {
        const size = this.in.varint();
        // An empty map omits the key/value type byte entirely.
        if (size > 0) {
          const kinds = this.in.u8();
          for (let index = 0; index < size; index++) {
            this.#skipElement(kinds >> 4);
            this.#skipElement(kinds & 0x0f);
          }
        }
        break;
      }
      case ThriftType.STRUCT: {
        this.structBegin();
        for (let field = this.fieldBegin(); field !== null; field = this.fieldBegin()) {
          this.skip(field.type);
        }
        this.structEnd();
        break;
      }
      default: {
        throw malformed(`Unknown Thrift compact type ${type} at offset ${this.in.offset}`);
      }
    }
    this.#depth--;
  }

  /** Container elements differ from fields in one place: a `bool` costs a byte. */
  #skipElement(type: number): void {
    if (type === ThriftType.BOOLEAN_TRUE || type === ThriftType.BOOLEAN_FALSE) {
      this.in.skip(1);
      return;
    }
    this.skip(type);
  }
}
