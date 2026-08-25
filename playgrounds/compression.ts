import { gzipSync, gunzipSync } from "node:zlib";
import {
  createWriter,
  defineSchema,
  readParquet,
  type ReaderCodec,
  type WriterCodec,
} from "tavolato";

const gzip = {
  name: "GZIP",
  compress: (page: Uint8Array) => gzipSync(page),
  decompress: (page: Uint8Array) => gunzipSync(page),
} satisfies WriterCodec & ReaderCodec;

const schema = defineSchema({ message: { type: "string" } });

export async function roundTripCompressed(): Promise<string> {
  const writer = createWriter(schema, { codec: gzip });
  await writer.append({ message: "compressed" });

  const bytes = await writer.finish();
  const { rows } = await readParquet(bytes, { codecs: { GZIP: gzip } });
  return rows[0].message as string;
}
