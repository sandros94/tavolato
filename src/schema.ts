import { TavolatoError } from "./error.ts";
import type { ColumnType, ParquetSchema, SchemaColumn, SchemaDefinition } from "./types.ts";

const COLUMN_TYPES: ReadonlySet<string> = new Set<ColumnType>([
  "string",
  "f64",
  "i64",
  "bool",
  "timestamp",
]);

/**
 * Validates a flat column map and freezes it into a `ParquetSchema`.
 *
 * Column order is the declaration order of the object's own enumerable string
 * keys, and that is the order columns appear in the file.
 *
 * @example
 * const schema = defineSchema({
 *   at: { type: "timestamp" },
 *   host: { type: "string", optional: true },
 *   n: { type: "i64" },
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
    if (!COLUMN_TYPES.has(column.type)) {
      throw new TavolatoError(
        `Column "${name}" has unsupported type ${JSON.stringify(column.type)}; expected one of ${[
          ...COLUMN_TYPES,
        ].join(", ")}`,
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
    columns.push(Object.freeze({ name, type: column.type, optional: column.optional === true }));
  }

  return Object.freeze({
    columns: Object.freeze(columns) as readonly SchemaColumn[],
    definition,
  });
}
