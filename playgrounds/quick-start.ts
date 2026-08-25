import { createWriter, defineSchema, readParquet, type ParquetFile } from "tavolato";

const schema = defineSchema({
  at: { type: "timestamp" },
  host: { type: "string", optional: true },
  count: { type: "i64" },
});

export function roundTripEvents(): ParquetFile {
  const writer = createWriter(schema);
  writer.append({ at: Date.UTC(2026, 7, 25), host: "web-1", count: 42n });
  writer.append({ at: new Date("2026-08-25T01:00:00Z"), host: null, count: 7 });

  return readParquet(writer.finish());
}
