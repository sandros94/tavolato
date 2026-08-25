/**
 * The top-level JSON document literal `null`.
 *
 * Ordinary JavaScript `null` means a Parquet/SQL null before a column adapter
 * runs. This singleton reaches the JSON serializer instead, where it is stored
 * as the four bytes `null`, and readers return this exact identity.
 */
export const JSON_NULL: unique symbol = Symbol("tavolato.JSON_NULL");

/** Type of the singleton {@link JSON_NULL}. */
export type JsonNull = typeof JSON_NULL;
