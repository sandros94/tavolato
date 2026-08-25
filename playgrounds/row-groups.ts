import { createWriter, defineSchema, readRowGroups } from "tavolato";

const schema = defineSchema({ count: { type: "i64" } });

export function sumOneGroupAtATime(): bigint {
  const writer = createWriter(schema, { rowGroupSize: 2 });
  writer.appendAll([{ count: 1n }, { count: 2n }, { count: 3n }]);

  let total = 0n;
  const file = readRowGroups(writer.finish(), { columns: ["count"] });
  for (const rows of file) {
    for (const row of rows) total += row.count as bigint;
  }
  return total;
}
