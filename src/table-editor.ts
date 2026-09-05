import { BSON } from "mongodb";
import { StateQLError } from "./errors.js";
import type { Driver, MongoWriteCommand, Row, SqlParameters } from "./types.js";

export interface TableIdentity { schema?: string; name: string }
export interface EditableColumn { name: string; type: string; nullable: boolean; generated: boolean; key: number }
export interface EditableTable { table: TableIdentity; driver: Driver; columns: EditableColumn[]; writable: boolean; reason?: string }
export interface TableChange { set?: Record<string, unknown>; unset?: string[] }
export interface TableUpdate { metadata: EditableTable; original: Row; changes: TableChange }

export function quoteIdentifier(name: string, driver: Driver): string {
  if (!name || name.length > 500 || name.includes("\0")) throw new StateQLError("INVALID_COMMAND", "Invalid database identifier.");
  return driver === "mysql" ? "`" + name.replaceAll("`", "``") + "`" : '"' + name.replaceAll('"', '""') + '"';
}

function comparable(value: unknown, column: EditableColumn): boolean {
  if (value === null) return column.nullable;
  const type = column.type.toLowerCase();
  if (/^(?:tinyint|smallint|mediumint|int|integer|bigint|int2|int4|int8)(?:\b|$)/u.test(type))
    return typeof value === "number" ? Number.isSafeInteger(value) : typeof value === "string" && /^-?\d+$/u.test(value);
  if (/^(?:numeric|decimal)(?:\b|$)/u.test(type)) return typeof value === "string" && /^-?\d+(?:\.\d+)?$/u.test(value);
  if (/^(?:boolean|bool)$/u.test(type)) return typeof value === "boolean";
  if (/^(?:text|varchar|character varying|nvarchar|ntext|longtext|mediumtext|tinytext)(?:\b|$)/u.test(type)) return typeof value === "string" && value.length <= 16_384;
  return false;
}

function mongoScalarType(value: unknown): string | undefined {
  if (value === null) return "null";
  if (typeof value === "string") return "string";
  if (typeof value === "boolean") return "bool";
  if (!value || typeof value !== "object" || Array.isArray(value) || Object.keys(value).length !== 1) return undefined;
  const types: Record<string, string> = { $oid: "objectId", $numberInt: "int", $numberLong: "long", $numberDecimal: "decimal", $date: "date" };
  return types[Object.keys(value)[0]!];
}

export function editableRow(metadata: EditableTable, row: Row): boolean {
  if (!metadata.writable || Buffer.byteLength(JSON.stringify(row), "utf8") > 32 * 1024) return false;
  if (metadata.driver === "mongodb") return Object.hasOwn(row, "_id") && Object.entries(row).every(([key, value]) => !key.startsWith("$") && !key.includes(".") && mongoScalarType(value) !== undefined);
  const keys = metadata.columns.filter(column => column.key > 0);
  return keys.length > 0 && keys.every(column => row[column.name] !== null && row[column.name] !== undefined) &&
    metadata.columns.length === Object.keys(row).length && metadata.columns.every(column => Object.hasOwn(row, column.name) && comparable(row[column.name], column));
}

export function compileTableUpdate(update: TableUpdate): { sql: string; params: SqlParameters; mongo?: MongoWriteCommand } {
  const { metadata, original, changes } = update;
  if (!editableRow(metadata, original)) throw new StateQLError("INVALID_COMMAND", "This row has no safe editable identity or comparison types.");
  const entries = Object.entries(changes.set ?? {});
  const unset = changes.unset ?? [];
  if ((!entries.length && !unset.length) || entries.length + unset.length > 100 || Buffer.byteLength(JSON.stringify(changes), "utf8") > 32 * 1024)
    throw new StateQLError("INVALID_COMMAND", "Provide bounded changed cells.");
  if (metadata.driver === "mongodb") {
    for (const name of [...entries.map(([name]) => name), ...unset]) {
      if (name === "_id" || !Object.hasOwn(original, name) || name.startsWith("$") || name.includes("."))
        throw new StateQLError("INVALID_COMMAND", "Only existing non-key fields can be changed.");
    }
    const document = BSON.EJSON.deserialize(original, { relaxed: false }) as Row;
    const mongo: MongoWriteCommand = {
      operation: "updateOne", collection: metadata.table.name,
      filter: { _id: document._id, $expr: { $and: [
        { $eq: ["$$ROOT", { $literal: document }] },
        ...Object.entries(original).map(([name, value]) => ({ $eq: [{ $type: "$" + name }, mongoScalarType(value)] })),
      ] } },
      update: { ...(entries.length ? { $set: BSON.EJSON.deserialize(Object.fromEntries(entries), { relaxed: false }) } : {}), ...(unset.length ? { $unset: Object.fromEntries(unset.map(name => [name, ""])) } : {}) },
      options: { upsert: false, collation: { locale: "simple" } },
    };
    return { sql: "MongoDB conditional row update", params: [], mongo };
  }
  if (unset.length) throw new StateQLError("INVALID_COMMAND", "SQL columns cannot be unset; use null where allowed.");
  const driver = metadata.driver;
  const quote = (name: string) => quoteIdentifier(name, driver);
  const params: unknown[] = [];
  const parameter = (value: unknown) => { params.push(value); return driver === "postgres" ? "$" + params.length : "?"; };
  const assignments = entries.map(([name, value]) => {
    const column = metadata.columns.find(column => column.name === name);
    if (!column || column.key || column.generated || !comparable(value, column)) throw new StateQLError("INVALID_COMMAND", "A changed column is generated, a key, or has an unsupported value.");
    return quote(name) + " = " + parameter(value);
  });
  const predicates = metadata.columns.map(column => {
    const name = quote(column.name);
    if (original[column.name] === null) return name + " IS NULL";
    const placeholder = parameter(original[column.name]);
    const isText = /text|varchar|character varying/u.test(column.type.toLowerCase());
    if (driver === "postgres") {
      const left = isText ? "convert_to(" + name + ", 'UTF8')" : name;
      const right = isText ? "convert_to(" + placeholder + ", 'UTF8')" : placeholder;
      return left + " = " + right;
    }
    if (driver === "mysql") return isText ? "CAST(" + name + " AS BINARY) = CAST(" + placeholder + " AS BINARY)" : name + " = " + placeholder;
    return isText ? "CAST(" + name + " AS BLOB) IS CAST(" + placeholder + " AS BLOB)" : name + " IS " + placeholder;
  });
  const table = [metadata.table.schema, metadata.table.name].filter((name): name is string => Boolean(name)).map(quote).join(".");
  return { sql: "UPDATE " + table + " SET " + assignments.join(", ") + " WHERE " + predicates.join(" AND "), params };
}

export function parseTableUpdate(parameters: string): TableUpdate {
  try {
    const outer: unknown = JSON.parse(parameters);
    if (!Array.isArray(outer) || outer.length !== 1 || typeof outer[0] !== "string" || outer[0].length > 128 * 1024) throw new Error();
    const update = JSON.parse(outer[0]) as TableUpdate;
    if (!update || !update.metadata || !["sqlite", "postgres", "mysql", "mongodb"].includes(update.metadata.driver) ||
      !Array.isArray(update.metadata.columns) || update.metadata.columns.length > 100 ||
      !update.metadata.columns.every(column => typeof column.name === "string" && typeof column.type === "string" && typeof column.nullable === "boolean" && typeof column.generated === "boolean" && Number.isSafeInteger(column.key)) ||
      !update.original || !update.changes || typeof update.changes !== "object") throw new Error();
    compileTableUpdate(update);
    return update;
  } catch { throw new StateQLError("STALE_PLAN", "The stored table update is invalid. Reload the row and plan again."); }
}
