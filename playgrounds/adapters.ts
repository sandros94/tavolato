import {
  createWriter,
  date,
  decimal,
  defineSchema,
  readParquet,
  uuid,
  type ParquetFile,
} from "tavolato";

const uuidType = uuid();
const dateType = date({ as: "date" });
const moneyType = decimal({ precision: 12, scale: 2 });

const schema = defineSchema({
  id: { type: uuidType },
  issued: { type: dateType },
  total: { type: moneyType },
});

export function roundTripInvoice(id: string): ParquetFile {
  const writer = createWriter(schema);
  writer.append({
    id,
    issued: new Date("2026-08-25T00:00:00Z"),
    total: "19.99",
  });

  return readParquet(writer.finish(), {
    types: [uuidType, dateType, moneyType],
  });
}
