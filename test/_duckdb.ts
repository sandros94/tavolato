import { spawnSync } from "node:child_process";
import { closeSync, existsSync, mkdtempSync, openSync, rmSync, writeSync } from "node:fs";
import { homedir, tmpdir } from "node:os";
import { join } from "node:path";

function resolveDuckdb(): string {
  const fromEnv = process.env.DUCKDB_BIN;
  if (fromEnv) return fromEnv;
  const local = join(homedir(), ".local", "bin", "duckdb");
  if (existsSync(local)) return local;
  return "duckdb";
}

/** Path to the DuckDB CLI used as the reference Parquet reader. */
export const DUCKDB_BIN: string = resolveDuckdb();

/**
 * Runs SQL through the DuckDB CLI and returns the rows it printed.
 *
 * DuckDB is the executable specification for this library: every file the
 * writer produces is handed to it and read back.
 */
export function duckdb<T = Record<string, unknown>>(sql: string): T[] {
  const result = spawnSync(DUCKDB_BIN, ["-json", "-c", sql], {
    encoding: "utf8",
    maxBuffer: 256 * 1024 * 1024,
  });
  if (result.error) {
    throw new Error(`Failed to run ${DUCKDB_BIN}: ${result.error.message}`);
  }
  if (result.status !== 0) {
    throw new Error(`DuckDB failed for:\n${sql}\n\n${result.stderr}`);
  }
  const stdout = result.stdout.trim();
  return stdout === "" ? [] : (JSON.parse(stdout) as T[]);
}

/** Runs SQL expecting exactly one row and returns it. */
export function duckdbRow<T = Record<string, unknown>>(sql: string): T {
  const rows = duckdb<T>(sql);
  if (rows.length !== 1) throw new Error(`Expected 1 row, got ${rows.length} for:\n${sql}`);
  return rows[0];
}

let directory: string | undefined;

/** Lazily created temp directory shared by the DuckDB suites. */
export function tempDir(): string {
  const existing = directory;
  if (existing !== undefined) return existing;
  const created = mkdtempSync(join(tmpdir(), "tavolato-"));
  directory = created;
  return created;
}

/** Writes bytes to the temp directory and returns a SQL-quoted absolute path. */
export function writeParquet(name: string, bytes: Uint8Array): string {
  const path = join(tempDir(), name);
  // Chunked because a single Node write call tops out at 2^31 - 1 bytes, and
  // the huge-file suite goes past that.
  const fd = openSync(path, "w");
  try {
    const chunkSize = 1024 * 1024 * 1024;
    for (let offset = 0; offset < bytes.length; offset += chunkSize) {
      writeSync(fd, bytes.subarray(offset, offset + chunkSize));
    }
  } finally {
    closeSync(fd);
  }
  return path;
}

/** Removes the temp directory, if one was created. */
export function cleanupTempDir(): void {
  if (directory) rmSync(directory, { recursive: true, force: true });
  directory = undefined;
}

/** Quotes a path for embedding in SQL. */
export function sqlPath(path: string): string {
  return `'${path.replaceAll("'", "''")}'`;
}
