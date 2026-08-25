import { inspectAdapter } from "./adapters.ts";
import { encodeUtf8Exact } from "./internal/bytes.ts";
import {
  columnAnnotation,
  columnPhysical,
  columnTypeLength,
  type ColumnSnapshot,
} from "./internal/format.ts";
import { describe, TavolatoError } from "./error.ts";
import type {
  Annotation,
  ColumnType,
  ParquetSchema,
  SchemaColumn,
  SchemaDefinition,
} from "./types.ts";

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

interface ValidatedColumn {
  readonly name: string;
  readonly type: SchemaColumn["type"];
  readonly optional: boolean;
  readonly physical: ColumnSnapshot["physical"];
  readonly typeLength: number | undefined;
  readonly annotation: ColumnSnapshot["annotation"];
}

/** Owned writer state produced at the structural schema boundary. @internal */
export interface ValidatedSchemaColumn {
  readonly column: SchemaColumn;
  readonly snapshot: ColumnSnapshot;
}

function columnInvalid(message: string, column?: string): never {
  throw new TavolatoError(message, "ERR_SCHEMA_COLUMN_INVALID", column);
}

/**
 * Validates either a declared column or the normalized column carried by a
 * structural `ParquetSchema`. The caller decides whether an omitted optional
 * flag means its declaration-time default or is malformed normalized state.
 */
function validateColumn(
  name: unknown,
  value: unknown,
  optionalMayBeOmitted: boolean,
): ValidatedColumn {
  if (typeof name !== "string") {
    columnInvalid(`Column names must be strings, received ${describe(name)}`);
  }
  if (name.length === 0) columnInvalid("Column names must not be empty", name);
  if (encodeUtf8Exact(name) === undefined) {
    columnInvalid(`Column name ${describe(name)} has no exact UTF-8 representation`, name);
  }
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    columnInvalid(`Column "${name}" must be an object such as { type: "string" }`, name);
  }

  const column = value as { readonly type?: unknown; readonly optional?: unknown };
  const type = column.type;
  let physical: ColumnSnapshot["physical"];
  let typeLength: number | undefined;
  let annotation: ColumnSnapshot["annotation"];
  if (typeof type === "object" && type !== null) {
    const inspected = inspectAdapter(type);
    if (typeof inspected === "string") columnInvalid(`Column "${name}" ${inspected}`, name);
    ({ physical, typeLength, annotation } = inspected);
  } else if (typeof type !== "string" || !COLUMN_TYPES.has(type)) {
    columnInvalid(
      `Column "${name}" has unsupported type ${describe(type)}; expected one of ${[
        ...COLUMN_TYPES,
      ].join(", ")}, or a column type from defineColumnType`,
      name,
    );
  } else {
    physical = columnPhysical(type as ColumnType);
    typeLength = columnTypeLength(type as ColumnType);
    annotation = columnAnnotation(type as ColumnType);
  }

  const optional = column.optional;
  if (typeof optional !== "boolean" && !(optionalMayBeOmitted && optional === undefined)) {
    columnInvalid(`Column "${name}" has a non-boolean "optional" flag`, name);
  }
  return {
    name,
    type: type as SchemaColumn["type"],
    optional: optional === true,
    physical,
    typeLength,
    annotation,
  };
}

/** Copies only the scalar fields the validated annotation kind owns. */
function ownAnnotation(annotation: Annotation): Annotation {
  switch (annotation.kind) {
    case "decimal": {
      return Object.freeze({
        kind: annotation.kind,
        precision: annotation.precision,
        scale: annotation.scale,
      });
    }
    case "time":
    case "timestamp": {
      return Object.freeze({
        kind: annotation.kind,
        unit: annotation.unit,
        isAdjustedToUTC: annotation.isAdjustedToUTC,
      });
    }
    case "integer": {
      return Object.freeze({
        kind: annotation.kind,
        bitWidth: annotation.bitWidth,
        isSigned: annotation.isSigned,
      });
    }
    case "unknown": {
      return Object.freeze({ kind: annotation.kind, id: annotation.id });
    }
    default: {
      return Object.freeze({ kind: annotation.kind });
    }
  }
}

/**
 * Validates the runtime-bearing half of a structurally typed `ParquetSchema`.
 * `definition` deliberately stays out of this path: it exists for inference;
 * `columns` alone defines file order and writer behavior.
 *
 * @internal
 */
export function validateParquetSchema(schema: unknown): readonly ValidatedSchemaColumn[] {
  if (typeof schema !== "object" || schema === null || Array.isArray(schema)) {
    throw new TavolatoError(
      "Schema must be an object carrying a columns array",
      "ERR_SCHEMA_EMPTY",
    );
  }
  const columns = (schema as { readonly columns?: unknown }).columns;
  if (columns === undefined) {
    throw new TavolatoError("Schema must declare at least one column", "ERR_SCHEMA_EMPTY");
  }
  if (!Array.isArray(columns)) {
    columnInvalid(`Schema columns must be an array, received ${describe(columns)}`);
  }
  if (columns.length === 0) {
    throw new TavolatoError("Schema must declare at least one column", "ERR_SCHEMA_EMPTY");
  }

  const names = new Set<string>();
  const validatedColumns: ValidatedSchemaColumn[] = [];
  for (const column of columns) {
    const name =
      typeof column === "object" && column !== null && !Array.isArray(column)
        ? (column as { readonly name?: unknown }).name
        : undefined;
    const validated = validateColumn(name, column, false);
    if (names.has(validated.name)) {
      columnInvalid(`Schema declares the column "${validated.name}" twice`, validated.name);
    }
    names.add(validated.name);
    const ownedColumn = Object.freeze({
      name: validated.name,
      type: validated.type,
      optional: validated.optional,
      ...(validated.typeLength === undefined ? {} : { typeLength: validated.typeLength }),
    });
    const snapshot = Object.freeze({
      name: validated.name,
      optional: validated.optional,
      physical: validated.physical,
      typeLength: validated.typeLength,
      annotation: ownAnnotation(validated.annotation),
    });
    validatedColumns.push(
      Object.freeze({
        column: ownedColumn,
        snapshot,
      }),
    );
  }
  return validatedColumns;
}

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
  if (definition === null || typeof definition !== "object" || Array.isArray(definition)) {
    throw new TavolatoError("Schema must be an object of column definitions", "ERR_SCHEMA_EMPTY");
  }

  const names = Object.keys(definition);
  if (names.length === 0) {
    throw new TavolatoError("Schema must declare at least one column", "ERR_SCHEMA_EMPTY");
  }

  const columns: SchemaColumn[] = [];
  for (const name of names) {
    const column = validateColumn(name, definition[name], true);
    if (typeof column.type === "object") {
      // A column type is a value, not a configuration object: what a schema was
      // validated against should still be what it is when a file is written.
      // `defineColumnType` already froze its own; this covers one that came
      // from somewhere else.
      Object.freeze(column.type);
    }
    columns.push(
      Object.freeze({
        name,
        type: column.type,
        optional: column.optional,
        ...(column.typeLength === undefined ? {} : { typeLength: column.typeLength }),
      }),
    );
  }

  return Object.freeze({
    columns: Object.freeze(columns) as readonly SchemaColumn[],
    definition,
  });
}
