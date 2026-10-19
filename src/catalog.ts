import { version } from "./connection.js";
import { adapterOfKind, type ActionResult, type Core } from "./core.js";
import { StateQLError } from "./errors.js";
import type { DatabaseAdapter } from "./adapters.js";
import type { ConnectionRecord, SessionRecord } from "./store.js";
import type {
  CatalogObject,
  DescribeObjectData,
  ExecutionOptions,
  ListObjectsData,
  ListObjectsFilter,
} from "./types.js";

const OBJECT_KINDS = ["table", "view", "collection", "function", "trigger", "enum", "key"];

export async function inspectSchema(
  core: Core,
  session: SessionRecord,
  kind: string,
  table: string | undefined,
  options: ExecutionOptions,
): Promise<ActionResult<unknown>> {
  const connection = core.requireConnection(session);
  core.rejectDuringStagedTransaction(session, "Schema inspection");
  if (connection.driver === "redis") {
    throw new StateQLError(
      "INVALID_COMMAND",
      "Legacy inspect is not supported for Redis; use listObjects or describeObject.",
    );
  }
  return catalogRead(core, session, connection, options, (adapter) =>
    adapter.kind === "mongodb"
      ? adapter.inspect(kind, table)
      : adapterOfKind(adapter, "sql").inspect(kind, table),
  );
}

export async function listObjects(
  core: Core,
  session: SessionRecord,
  filter: ListObjectsFilter,
  options: ExecutionOptions,
): Promise<ActionResult<ListObjectsData>> {
  validateCatalogFilter(filter);
  const connection = core.requireConnection(session);
  core.rejectDuringStagedTransaction(session, "Catalog discovery");
  return catalogRead(core, session, connection, options, (adapter) => adapter.listObjects(filter));
}

export async function describeObject(
  core: Core,
  session: SessionRecord,
  object: CatalogObject,
  options: ExecutionOptions,
): Promise<ActionResult<DescribeObjectData>> {
  validateCatalogObject(object);
  const connection = core.requireConnection(session);
  core.rejectDuringStagedTransaction(session, "Catalog description");
  return catalogRead(core, session, connection, options, (adapter) => adapter.describeObject(object));
}

async function catalogRead<T>(
  core: Core,
  session: SessionRecord,
  connection: ConnectionRecord,
  options: ExecutionOptions,
  read: (adapter: DatabaseAdapter) => Promise<T>,
): Promise<ActionResult<T>> {
  const context = core.executionContext(options);
  return core.withAdapter(session, connection, "inspect", context, async (adapter) => ({
    data: await read(adapter),
    executed: true,
    stateVersion: version(connection),
    confidence: adapter.confidence,
  }));
}

function validateCatalogFilter(filter: ListObjectsFilter): void {
  if (!filter || typeof filter !== "object" || Array.isArray(filter) || Object.keys(filter).some((key) => !["kind", "schema", "search", "offset", "limit"].includes(key))) throw new StateQLError("INVALID_COMMAND", "Catalog filter contains unknown fields.");
  if (filter.kind !== undefined && !OBJECT_KINDS.includes(filter.kind)) throw new StateQLError("INVALID_COMMAND", "Unknown catalog object kind.");
  for (const [name, value] of [["schema", filter.schema], ["search", filter.search]] as const) if (value !== undefined && (typeof value !== "string" || !value || value.length > 200 || value.includes("\0"))) throw new StateQLError("INVALID_COMMAND", `Catalog ${name} is invalid.`);
  if (filter.offset !== undefined && !((typeof filter.offset === "number" && Number.isSafeInteger(filter.offset) && filter.offset >= 0) || (typeof filter.offset === "string" && /^\d+$/.test(filter.offset)))) throw new StateQLError("INVALID_COMMAND", "Catalog offset is invalid.");
  if (filter.limit !== undefined && (!Number.isSafeInteger(filter.limit) || filter.limit < 1 || filter.limit > 200)) throw new StateQLError("INVALID_COMMAND", "Catalog limit must be 1-200.");
}

function validateCatalogObject(object: CatalogObject): void {
  if (!object || typeof object !== "object" || Array.isArray(object) || !OBJECT_KINDS.includes(object.kind) || typeof object.name !== "string" || !object.name || object.name.length > 500 || object.name.includes("\0") || (object.schema !== undefined && (typeof object.schema !== "string" || !object.schema || object.schema.length > 500 || object.schema.includes("\0"))) || (object.identity !== undefined && (typeof object.identity !== "string" || !object.identity || object.identity.length > 1000 || object.identity.includes("\0")))) throw new StateQLError("INVALID_COMMAND", "Catalog object identity is invalid.");
}
