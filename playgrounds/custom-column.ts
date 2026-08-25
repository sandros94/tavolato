import { createWriter, defineColumnType, defineSchema, readParquet } from "tavolato";

const cents = defineColumnType<number, number>({
  name: "cents",
  physical: "i64",
  matches: (annotation) =>
    annotation.kind === "decimal" && annotation.precision === 18 && annotation.scale === 2,
  annotate: () => ({ kind: "decimal", precision: 18, scale: 2 }),
  read: (raw) => Number(raw as bigint) / 100,
  write: (value) => BigInt(Math.round(value * 100)),
});

const schema = defineSchema({ amount: { type: cents } });

export function roundTripAmount(amount: number): number {
  const writer = createWriter(schema);
  writer.append({ amount });

  const { rows } = readParquet(writer.finish(), { types: [cents] });
  return rows[0].amount as number;
}
