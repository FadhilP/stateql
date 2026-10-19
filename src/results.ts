import { writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { nonNegativeInteger, positiveInteger, type ActionResult, type Core } from "./core.js";
import { StateQLError } from "./errors.js";
import {
  filterMaterializedRows,
  prepareFilterStatement,
  validateFilterParameters,
} from "./filter.js";
import { rowsToCsv } from "./response-data.js";
import type { ResultRecord } from "./store.js";
import type {
  Column,
  ColumnsData,
  ColumnSummary,
  CountData,
  ExportData,
  FilterOptions,
  ResultData,
  Row,
  RowsData,
  RowsOptions,
  SummaryData,
} from "./types.js";
import { compactRows, compactValue, hash } from "./util.js";

export function showResult(core: Core, result: ResultRecord): ActionResult<ResultData> {
  return storedResult(result, core.resultData(result, true));
}

export function filterResult(
  core: Core,
  source: ResultRecord,
  predicate: string,
  options: FilterOptions,
): ActionResult<ResultData> {
  const columns = core.store.resultColumns(source);
  const filter = prepareFilterStatement(columns, predicate);
  const parameters = options.params ?? [];
  validateFilterParameters(filter, parameters);
  const fingerprint = hash({
    command: "filter",
    source: source.id,
    predicate: filter.normalized,
    parameters,
  });
  const cached = core.store.findResult(fingerprint);
  const reusable = cached !== undefined &&
    cached.session_id === source.session_id &&
    cached.connection_id === source.connection_id &&
    Date.parse(cached.expires_at) > core.now().getTime();
  if (reusable) return storedResult(cached, core.resultData(cached, true));

  const saved = core.store.saveResult({
    sessionId: source.session_id,
    connectionId: source.connection_id,
    fingerprint,
    sql: `FILTER ${source.id} WHERE ${predicate.trim()}`,
    parameters,
    rows: filterMaterializedRows(core.store.resultRows(source), filter, parameters),
    columns,
    stateVersion: source.state_version,
    stateSignature: source.state_signature,
    stateConfidence: source.state_confidence,
    expiresAt: source.expires_at,
  });
  return {
    data: core.resultData(saved, false),
    handle: saved.id,
    executed: true,
    stateVersion: saved.state_version,
    confidence: saved.state_confidence,
  };
}

export function resultRows(core: Core, result: ResultRecord, options: RowsOptions): ActionResult<RowsData> {
  const offset = nonNegativeInteger(options.offset ?? 0, "offset");
  const limit = positiveInteger(options.limit ?? 20, "limit");
  if (limit > 1_000) {
    throw new StateQLError(
      "OUTPUT_LIMIT_EXCEEDED",
      "limit cannot exceed 1000 rows.",
      { suggestedAction: "Fetch another page or use export." },
    );
  }
  const rows = selectColumns(
    core.store.resultRows(result).slice(offset, offset + limit),
    core.store.resultColumns(result),
    options.columns,
  );
  const next = offset + rows.length;
  return storedResult(result, {
    result_id: result.id,
    offset,
    limit,
    rows: compactRows(rows, core.settings.maxCellCharacters),
    returned: rows.length,
    total: result.row_count,
    truncated: next < result.row_count,
    next_offset: next < result.row_count ? next : null,
  });
}

export function countResult(result: ResultRecord): ActionResult<CountData> {
  return storedResult(result, { result_id: result.id, rows: result.row_count });
}

export function resultColumns(core: Core, result: ResultRecord): ActionResult<ColumnsData> {
  return storedResult(result, { result_id: result.id, columns: core.store.resultColumns(result) });
}

export function summarizeResult(core: Core, result: ResultRecord): ActionResult<SummaryData> {
  const rows = core.store.resultRows(result);
  return storedResult(result, {
    result_id: result.id,
    total: result.row_count,
    columns: core.store.resultColumns(result).map((column) =>
      summarizeColumn(column.name, rows, core.settings.maxCellCharacters),
    ),
  });
}

export function exportResult(
  core: Core,
  result: ResultRecord,
  output: string,
  format: "json" | "jsonl" | "csv",
  columns: string[] | undefined,
): ActionResult<ExportData> {
  const resultColumns = core.store.resultColumns(result);
  const rows = selectColumns(core.store.resultRows(result), resultColumns, columns);
  const content =
    format === "json"
      ? `${JSON.stringify(rows, null, 2)}\n`
      : format === "jsonl"
        ? `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`
        : rowsToCsv(rows, columns ?? resultColumns.map((column) => column.name));
  const path = resolve(output);
  writeFileSync(path, content, "utf8");
  return {
    data: { result_id: result.id, output: path, format, rows: rows.length },
    handle: result.id,
    executed: true,
  };
}

function storedResult<T>(result: ResultRecord, data: T): ActionResult<T> {
  return {
    data,
    handle: result.id,
    cached: true,
    stateVersion: result.state_version,
    confidence: result.state_confidence,
  };
}

function selectColumns(rows: Row[], available: Column[], selected: string[] | undefined): Row[] {
  if (!selected) return rows;
  const names = available.map((column) => column.name);
  const unknown = selected.filter((name) => !names.includes(name));
  if (selected.length === 0 || unknown.length > 0) {
    throw new StateQLError(
      "INVALID_COMMAND",
      unknown.length > 0 ? `Unknown column: ${unknown.join(", ")}.` : "columns cannot be empty.",
      { suggestedAction: `Use one or more of: ${names.join(", ")}.` },
    );
  }
  // Keep missing keys missing, so MongoDB documents still distinguish missing from null.
  return rows.map((row) =>
    Object.fromEntries(selected.filter((name) => name in row).map((name) => [name, row[name]])),
  );
}

function summarizeColumn(name: string, rows: Row[], maxCellCharacters: number): ColumnSummary {
  const values = rows
    .map((row) => row[name])
    .filter((value) => value !== null && value !== undefined);
  const [min, max] = valueRange(values);
  return {
    name,
    nulls: rows.length - values.length,
    distinct: new Set(values.map((value) => JSON.stringify(value))).size,
    min: compactValue(min, maxCellCharacters),
    max: compactValue(max, maxCellCharacters),
  };
}

const NUMERIC_TEXT = /^-?\d+(?:\.\d+)?$/;

// Exact numerics (PostgreSQL numeric/bigint, MySQL DECIMAL) arrive as strings, so compare them as numbers.
function valueRange(values: unknown[]): [unknown, unknown] {
  const numeric = values.every(
    (value) => typeof value === "number" || (typeof value === "string" && NUMERIC_TEXT.test(value)),
  );
  const text = values.every((value) => typeof value === "string");
  if (values.length === 0) return [null, null];
  if (!numeric && !text) return [null, null];
  const compare = numeric
    ? (left: unknown, right: unknown) => Number(left) - Number(right)
    : (left: unknown, right: unknown) => (String(left) < String(right) ? -1 : String(left) > String(right) ? 1 : 0);
  let min = values[0];
  let max = values[0];
  for (const value of values) {
    if (compare(value, min) < 0) min = value;
    if (compare(value, max) > 0) max = value;
  }
  return [min, max];
}
