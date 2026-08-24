import { adapterProblem } from "./adapters.ts";
import { columnTypeLength } from "./internal/format.ts";
import { TavolatoError } from "./error.ts";
import type { ColumnType, ParquetSchema, SchemaColumn, SchemaDefinition } from "./types.ts";

const COLUMN_TYPES: ReadonlySet<string> = new Set<ColumnType>([
  "string",
  "json",
  "f64",
  "f32",
  "i64",
  "i32",
  "bool",
  "timestamp",
]);

/**
 * Validates a flat column map and freezes it into a `ParquetSchema`.
 *
 * Column order is the declaration order of the object's own enumerable string
 * keys, and that is the order columns appear in the file.
 *
 * A column's `type` is either one of the built-in names or a logical column
 * type from `defineColumnType`, which is checked here for the same reason the
 * names are: a schema that cannot produce a file should say so before a single
 * row is appended.
 *
 * @example
 * const schema = defineSchema({
 *   at: { type: "timestamp" },
 *   host: { type: "string", optional: true },
 *   n: { type: "i64" },
 *   price: { type: decimal({ precision: 12, scale: 2 }) },
 * });
 *
 * @throws {TavolatoError} `ERR_SCHEMA_EMPTY` when no column is declared.
 * @throws {TavolatoError} `ERR_SCHEMA_COLUMN_INVALID` on a malformed column.
 */
export function defineSchema<const TDefinition extends SchemaDefinition>(
  definition: TDefinition,
): ParquetSchema<TDefinition> {
  if (definition === null || typeof definition !== "object") {
    throw new TavolatoError("Schema must be an object of column definitions", "ERR_SCHEMA_EMPTY");
  }

  const names = Object.keys(definition);
  if (names.length === 0) {
    throw new TavolatoError("Schema must declare at least one column", "ERR_SCHEMA_EMPTY");
  }

  const columns: SchemaColumn[] = [];
  for (const name of names) {
    if (name.length === 0) {
      throw new TavolatoError("Column names must not be empty", "ERR_SCHEMA_COLUMN_INVALID", name);
    }
    const column = definition[name];
    if (column === null || typeof column !== "object") {
      throw new TavolatoError(
        `Column "${name}" must be an object such as { type: "string" }`,
        "ERR_SCHEMA_COLUMN_INVALID",
        name,
      );
    }
    if (typeof column.type === "object" && column.type !== null) {
      const problem = adapterProblem(column.type);
      if (problem !== undefined) {
        throw new TavolatoError(`Column "${name}" ${problem}`, "ERR_SCHEMA_COLUMN_INVALID", name);
      }
      // A column type is a value, not a configuration object: what a schema was
      // validated against should still be what it is when a file is written.
      // `defineColumnType` already froze its own; this covers one that came
      // from somewhere else.
      Object.freeze(column.type);
    } else if (!COLUMN_TYPES.has(column.type)) {
      throw new TavolatoError(
        `Column "${name}" has unsupported type ${JSON.stringify(column.type)}; expected one of ${[
          ...COLUMN_TYPES,
        ].join(", ")}, or a column type from defineColumnType`,
        "ERR_SCHEMA_COLUMN_INVALID",
        name,
      );
    }
    if (column.optional !== undefined && typeof column.optional !== "boolean") {
      throw new TavolatoError(
        `Column "${name}" has a non-boolean "optional" flag`,
        "ERR_SCHEMA_COLUMN_INVALID",
        name,
      );
    }
    const typeLength = columnTypeLength(column.type);
    columns.push(
      Object.freeze({
        name,
        type: column.type,
        optional: column.optional === true,
        ...(typeLength === undefined ? {} : { typeLength }),
      }),
    );
  }

  return Object.freeze({
    columns: Object.freeze(columns) as readonly SchemaColumn[],
    definition,
  });
}
