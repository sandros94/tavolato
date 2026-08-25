import { defineSchema, type ReadRow } from "tavolato";
import { createParquetStore, type ParquetHead, type ParquetStoreClient } from "tavolato/uns3";

const schema = defineSchema({
  at: { type: "timestamp" },
  count: { type: "i64" },
});

export async function storeEvents(
  client: ParquetStoreClient,
): Promise<{ head: ParquetHead; rows: ReadRow[] }> {
  const store = createParquetStore(client, { bucket: "analytics" });
  const key = "events/date=2026-08-25/part-001.parquet";

  await store.put(key, {
    schema,
    rows: [{ at: Date.UTC(2026, 7, 25), count: 1n }],
  });

  const head = await store.head(key);
  const { rows } = await store.get(key, { columns: ["count"], groups: [0] });
  return { head, rows };
}
