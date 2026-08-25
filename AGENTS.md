# Repository Guidelines

## Project Structure & Module Organization

`src/` contains the runtime-agnostic TypeScript library. Public exports are assembled in `src/index.ts`; the optional object-store integration is exported from `src/uns3.ts`. Parquet byte, encoding, Thrift, and format primitives live in `src/internal/` and are not public API. Tests are colocated by concern in `test/*.test.ts`; shared fixtures and harness helpers use leading underscores, such as `test/_build.ts`. Generated packages go to `dist/`, and coverage output goes to `coverage/`; do not edit either.

## Build, Test, and Development Commands

Use Node `>=24.17.0` and pnpm `11.17.0`.

- `pnpm install --frozen-lockfile`: install the exact locked dependency graph.
- `pnpm dev:prepare`: create development stubs and install Git hooks.
- `pnpm build`: bundle ESM and declaration files with `obuild`.
- `pnpm lint`: run Oxlint and verify Oxfmt formatting.
- `pnpm fmt`: update generated Markdown, apply lint fixes, and format files.
- `pnpm typecheck`: run strict TypeScript checks without emitting files.
- `pnpm test`: run the Vitest suite once.
- `pnpm test:coverage`: produce V8 coverage; no minimum threshold is configured.

The interoperability tests require a `duckdb` executable on `PATH` (CI uses DuckDB 1.5.5).

## Coding Style & Naming Conventions

Follow Oxfmt output: UTF-8, LF endings, final newline, two-space indentation, double quotes, and semicolons. Keep imports explicit with `.ts` extensions and use `import type` where applicable. Use `camelCase` for values/functions, `PascalCase` for types/classes, and uppercase constants only for fixed protocol values. Preserve the core constraint: files under `src/` must not import `node:*` or runtime dependencies. Export intended API only through entrypoints.

## Testing Guidelines

Use Vitest `describe`/`it` blocks and name files `<feature>.test.ts`; colocate `expectTypeOf` assertions with the runtime tests for the same contract. `pnpm typecheck` remains the compiler gate. Add focused unit tests beside cross-reader checks when changing encoded output. Every emitted Parquet shape should round-trip through tavolato and remain readable by DuckDB. Run lint, typecheck, tests, and build before opening a PR.

## Commit & Pull Request Guidelines

History follows Conventional Commits: `feat: ...`, `fix: ...`, `test: ...`, `docs: ...`, `ci: ...`, or `chore: ...`. Keep subjects imperative, lowercase, and concise. PRs should explain behavior and rationale, list verification commands, and link relevant issues. Include compatibility evidence for format changes; screenshots are only useful for documentation rendering changes. Keep generated artifacts out of commits.
