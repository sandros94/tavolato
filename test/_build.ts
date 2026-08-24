import { ByteWriter } from "../src/internal/bytes.ts";
import { writePlain } from "../src/internal/encoding.ts";
import { encodeDataPageHeader, MAGIC } from "../src/internal/format.ts";

/**
 * Hand-building Parquet files, for the shapes tavolato's own writer cannot
 * produce.
 *
 * The writer emits exactly one data page per column chunk, so no file it makes
 * can exercise the reader's page loop — and a multi-page chunk is what every
 * other writer produces, `parquet-mr` and Arrow cutting a page roughly every
 * megabyte. The suite gets one from here instead.
 */

/** What one written page cost, in the two sizes a column chunk has to add up. */
export interface WrittenPage {
  readonly uncompressedSize: number;
  readonly compressedSize: number;
}

/** Opens a hand-built file: the leading magic, and nothing else yet. */
export function startFile(): ByteWriter {
  const out = new ByteWriter(1024);
  out.raw(MAGIC);
  return out;
}

/**
 * Appends one v1 PLAIN data page of required `INT64` values, optionally passing
 * the body through `compress` exactly as the writer would.
 */
export function writeDataPage(
  out: ByteWriter,
  values: readonly bigint[],
  compress?: (body: Uint8Array) => Uint8Array,
): WrittenPage {
  const body = new ByteWriter(64);
  writePlain(body, { kind: "i64", items: [...values] });
  const raw = body.toBytes();
  const stored = compress === undefined ? raw : compress(raw);
  const header = encodeDataPageHeader(raw.length, stored.length, values.length);
  out.raw(header);
  out.raw(stored);
  return {
    uncompressedSize: header.length + raw.length,
    compressedSize: header.length + stored.length,
  };
}

/** Closes a hand-built file: the footer, its length, and the trailing magic. */
export function sealFile(out: ByteWriter, footer: Uint8Array): Uint8Array {
  out.raw(footer);
  out.u32(footer.length);
  out.raw(MAGIC);
  return out.toBytes();
}
