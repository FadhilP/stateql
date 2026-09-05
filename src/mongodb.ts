import {
  BSON,
  MongoClient,
  type Abortable,
  type AggregateOptions,
  type BulkWriteOptions,
  type ClientSession,
  type CollationOptions,
  type DeleteOptions,
  type Document,
  type FindOptions,
  type Hint,
  type ReplaceOptions,
  type Sort,
  type UpdateOptions,
} from "mongodb";
import {
  AdapterExecutionError,
  AdapterWriteError,
  BatchWriteError,
  type AdapterContext,
} from "./adapters.js";
import type { ConnectionRecord } from "./store.js";
import type {
  Column,
  MongoDocument,
  MongoReadCommand,
  MongoWriteCommand,
  MongoWriteOutcome,
  Row,
} from "./types.js";

const MAX_INSPECTION_ITEMS = 1_000;
const FIND_OPTIONS = new Set([
  "projection",
  "sort",
  "skip",
  "limit",
  "hint",
  "collation",
]);
const AGGREGATE_OPTIONS = new Set(["allowDiskUse", "hint", "collation"]);
const WRITE_OPTIONS: Record<MongoWriteCommand["operation"], ReadonlySet<string>> = {
  insertOne: new Set(),
  insertMany: new Set(["ordered"]),
  updateOne: new Set(["upsert", "collation", "hint"]),
  updateMany: new Set(["upsert", "collation", "hint"]),
  replaceOne: new Set(["upsert", "collation", "hint"]),
  deleteOne: new Set(["collation", "hint"]),
  deleteMany: new Set(["collation", "hint"]),
};
const FORBIDDEN_KEYS = new Set([
  "$out",
  "$merge",
  "$changeStream",
  "$where",
  "$function",
  "$accumulator",
]);
const UPDATE_PIPELINE_STAGES = new Set([
  "$addFields",
  "$set",
  "$project",
  "$unset",
  "$replaceRoot",
  "$replaceWith",
]);
const READ_KEYS: Record<MongoReadCommand["operation"], ReadonlySet<string>> = {
  find: new Set(["operation", "collection", "filter", "options"]),
  aggregate: new Set(["operation", "collection", "pipeline", "options"]),
};
const WRITE_KEYS: Record<MongoWriteCommand["operation"], ReadonlySet<string>> = {
  insertOne: new Set(["operation", "collection", "document", "options"]),
  insertMany: new Set(["operation", "collection", "documents", "options"]),
  updateOne: new Set(["operation", "collection", "filter", "update", "options"]),
  updateMany: new Set(["operation", "collection", "filter", "update", "options"]),
  replaceOne: new Set(["operation", "collection", "filter", "replacement", "options"]),
  deleteOne: new Set(["operation", "collection", "filter", "options"]),
  deleteMany: new Set(["operation", "collection", "filter", "options"]),
};

interface MongoReadResult {
  rows: Row[];
  columns: Column[];
}

interface MongoWriteResult {
  affectedRows: number;
  outcome: MongoWriteOutcome;
}

interface MongoWriteSafety {
  unbounded: boolean;
  destructive: boolean;
}

export function validateMongoReadCommand(value: unknown): MongoReadCommand {
  if (!isRecord(value)) throw new Error("MongoDB read command must be an object.");
  const operation = value.operation;
  if (operation !== "find" && operation !== "aggregate") {
    throw new Error("MongoDB reads only support find and aggregate.");
  }
  rejectUnknownKeys(value, READ_KEYS[operation], "read command");
  validateCollection(value.collection);
  validateOptions(value.options, operation === "find" ? FIND_OPTIONS : AGGREGATE_OPTIONS, operation);

  if (operation === "find") {
    if (value.filter !== undefined && !isDocument(value.filter)) {
      throw new Error("MongoDB find filter must be an object.");
    }
    validateFindOptions(value.options);
  } else {
    if (!Array.isArray(value.pipeline) || !value.pipeline.every(isDocument)) {
      throw new Error("MongoDB aggregate pipeline must be an array of stage objects.");
    }
    for (const stage of value.pipeline) {
      if (Object.keys(stage).length !== 1) {
        throw new Error("Each MongoDB aggregate pipeline stage must contain exactly one operator.");
      }
    }
    validateAggregateOptions(value.options);
  }
  rejectForbiddenValues(value);
  return value as MongoReadCommand;
}

export function validateMongoWriteCommand(value: unknown): MongoWriteCommand {
  if (!isRecord(value)) throw new Error("MongoDB write command must be an object.");
  const operation = value.operation;
  if (!isMongoWriteOperation(operation)) {
    throw new Error(`Unsupported MongoDB write operation "${String(operation)}".`);
  }
  rejectUnknownKeys(value, WRITE_KEYS[operation], "write command");
  validateCollection(value.collection);
  validateOptions(value.options, WRITE_OPTIONS[operation], operation);
  validateWriteOptions(value.options, operation);

  switch (operation) {
    case "insertOne":
      requireDocument(value.document, "insertOne document");
      break;
    case "insertMany":
      if (!Array.isArray(value.documents) || value.documents.length === 0 || !value.documents.every(isDocument)) {
        throw new Error("MongoDB insertMany documents must be a non-empty array of objects.");
      }
      break;
    case "updateOne":
    case "updateMany":
      requireDocument(value.filter, `${operation} filter`);
      validateUpdate(value.update);
      break;
    case "replaceOne":
      requireDocument(value.filter, "replaceOne filter");
      requireDocument(value.replacement, "replaceOne replacement");
      if (Object.keys(value.replacement as MongoDocument).some((key) => key.startsWith("$"))) {
        throw new Error("MongoDB replacement documents cannot contain top-level update operators.");
      }
      break;
    case "deleteOne":
    case "deleteMany":
      requireDocument(value.filter, `${operation} filter`);
      break;
  }
  rejectForbiddenValues(value);
  return value as MongoWriteCommand;
}


export function analyzeMongoWriteSafety(command: MongoWriteCommand): MongoWriteSafety {
  const value = validateMongoWriteCommand(command);
  switch (value.operation) {
    case "insertOne":
    case "insertMany":
      return { unbounded: false, destructive: false };
    case "updateOne":
    case "updateMany":
      return {
        unbounded: Object.keys(value.filter).length === 0,
        destructive: false,
      };
    case "replaceOne":
      return {
        unbounded: Object.keys(value.filter).length === 0,
        destructive: true,
      };
    case "deleteOne":
    case "deleteMany":
      return {
        unbounded: Object.keys(value.filter).length === 0,
        destructive: true,
      };
  }
}

/** Deterministic EJSON preserves BSON types and property order. */
export function serializeMongoCommand(command: MongoReadCommand | MongoWriteCommand): string {
  return BSON.EJSON.stringify(command, { relaxed: false });
}

export function deserializeMongoWriteCommand(value: string): MongoWriteCommand {
  return validateMongoWriteCommand(parseEjson(value));
}


export class MongoAdapter {
  readonly confidence = "ttl_based" as const;
  private readonly client: MongoClient;
  private readonly databaseName: string;
  private readonly readOnly: boolean;
  private connected = false;
  private closed = false;
  private connecting?: Promise<void>;

  constructor(
    connection: ConnectionRecord,
    private readonly context: AdapterContext,
    input: { source: string },
  ) {
    if (connection.driver !== "mongodb") {
      throw new Error("MongoAdapter requires a MongoDB connection record.");
    }
    if (!connection.database_name || connection.database_name.includes("\0")) {
      throw new Error("MongoDB connection requires an explicit valid database.");
    }
    this.databaseName = connection.database_name;
    this.readOnly = Boolean(connection.read_only);
    const timeout = remainingMilliseconds(context);
    this.client = new MongoClient(input.source, {
      connectTimeoutMS: timeout,
      serverSelectionTimeoutMS: timeout,
      waitQueueTimeoutMS: timeout,
    });
  }

  async ping(): Promise<void> {
    await this.connect();
    const signal = operationSignal(this.context);
    try {
      await this.client.db(this.databaseName).command(
        { ping: 1 },
        {
          timeoutMS: remainingMilliseconds(this.context),
          signal,
        },
      );
    } catch (error) {
      throw readError(error, this.context);
    }
  }

  async signature(): Promise<string> {
    throwIfStopped(this.context, false);
    return "mongodb:ttl";
  }

  async read(command: MongoReadCommand, maxRows: number): Promise<MongoReadResult> {
    const value = validateMongoReadCommand(command);
    if (!Number.isSafeInteger(maxRows) || maxRows <= 0) {
      throw new Error("MongoDB maxRows must be a positive integer.");
    }
    await this.connect();
    const collection = this.client.db(this.databaseName).collection(value.collection);
    const signal = operationSignal(this.context);
    let cursor;
    try {
      if (value.operation === "find") {
        const options = findOptions(value.options as MongoDocument | undefined, this.context, signal, maxRows);
        cursor = collection.find(value.filter as Document | undefined ?? {}, options);
      } else {
        const options = aggregateOptions(value.options as MongoDocument | undefined, this.context, signal);
        cursor = collection.aggregate(value.pipeline as Document[], options).limit(maxRows);
      }
      const documents = await collectCursor(cursor, maxRows, this.context);
      return documentsResult(documents);
    } catch (error) {
      throw readError(error, this.context);
    } finally {
      await cursor?.close().catch(() => undefined);
    }
  }

  async write(command: MongoWriteCommand, expectedRows?: 1): Promise<MongoWriteResult> {
    let value: MongoWriteCommand;
    try {
      value = validateMongoWriteCommand(command);
    } catch (error) {
      throw new AdapterWriteError(errorText(error), false);
    }
    if (this.readOnly) throw new AdapterWriteError("Connection is read-only.", false);
    throwIfStopped(this.context, false);
    try {
      await this.connect();
    } catch (error) {
      if (error instanceof AdapterExecutionError) throw error;
      throw new AdapterWriteError(errorText(error), false);
    }

    if (expectedRows === 1 && (value.operation !== "updateOne" || value.options?.upsert)) throw new AdapterWriteError("Conditional edits require updateOne without upsert.", false);
    try {
      const result = await withContext(
        this.executeWrite(value),
        this.context,
        () => this.stop(),
        true,
      );
      if (expectedRows === 1 && result.outcome.matched_count !== 1) throw new AdapterWriteError("ROW_CONFLICT: The document changed or was removed.", false);
      return result;
    } catch (error) {
      if (error instanceof AdapterWriteError) throw error;
      const stopped = writeStoppedError(error, this.context, true);
      if (stopped) throw stopped;
      throw new AdapterWriteError(errorText(error), knownNoWrite(error) ? false : true);
    }
  }

  async writeBatch(
    commands: MongoWriteCommand[],
    isolation: string,
  ): Promise<MongoWriteResult[]> {
    if (isolation.toLowerCase() !== "snapshot") {
      throw new BatchWriteError(`Unsupported MongoDB isolation level "${isolation}".`, false);
    }
    let values: MongoWriteCommand[];
    try {
      if (!Array.isArray(commands)) throw new Error("MongoDB transaction commands must be an array.");
      values = commands.map(validateMongoWriteCommand);
    } catch (error) {
      throw new BatchWriteError(errorText(error), false);
    }
    if (this.readOnly) throw new BatchWriteError("Connection is read-only.", false);
    throwIfStopped(this.context, false);
    try {
      await this.connect();
    } catch (error) {
      if (error instanceof AdapterExecutionError) throw error;
      throw new BatchWriteError(errorText(error), false);
    }

    const session = this.client.startSession();
    let dispatched = false;
    let committing = false;
    try {
      session.startTransaction({
        readConcern: { level: "snapshot" },
        writeConcern: { w: "majority" },
        readPreference: "primary",
        maxCommitTimeMS: remainingMilliseconds(this.context),
      });
      const results: MongoWriteResult[] = [];
      for (const command of values) {
        throwIfStopped(this.context, dispatched);
        dispatched = true;
        results.push(await withContext(
          this.executeWrite(command, session),
          this.context,
          () => this.stop(),
          true,
        ));
      }
      throwIfStopped(this.context, dispatched);
      committing = true;
      await withContext(
        session.commitTransaction({ timeoutMS: remainingMilliseconds(this.context) }),
        this.context,
        () => this.stop(),
        true,
      );
      return results;
    } catch (error) {
      let aborted = false;
      if (!committing && session.inTransaction()) {
        try {
          await session.abortTransaction({
            timeoutMS: remainingMilliseconds(this.context),
          });
          aborted = true;
        } catch {
          // Without a confirmed abort, the staged write outcome is unknown.
        }
      }
      const outcomeUnknown = committing || (dispatched && !aborted);
      const stopped = writeStoppedError(error, this.context, outcomeUnknown);
      if (stopped) throw stopped;
      throw new BatchWriteError(
        errorText(error),
        outcomeUnknown && !knownNoWrite(error),
      );
    } finally {
      await session.endSession().catch(() => undefined);
    }
  }

  async inspect(kind: string, name?: string): Promise<unknown> {
    await this.connect();
    try {
      if (kind === "schema" || kind === "collections") {
        const cursor = this.client.db(this.databaseName).listCollections(
          {},
          {
            nameOnly: true,
            signal: operationSignal(this.context),
            maxTimeMS: remainingMilliseconds(this.context),
            timeoutMS: remainingMilliseconds(this.context),
          },
        );
        const collections = await collectCursor(cursor, MAX_INSPECTION_ITEMS, this.context);
        return ejsonSafe({
          collections: collections.map((collection) => ({
            name: collection.name,
            type: collection.type ?? "collection",
          })),
        });
      }
      validateCollection(name);
      const collectionName = name as string;
      await this.requireCollection(collectionName);
      if (kind === "constraints") {
        return { collection: collectionName, constraints: [] };
      }
      if (kind === "editable") {
        const objects = await this.client.db(this.databaseName).listCollections({ name: collectionName }, { nameOnly: true, signal: operationSignal(this.context), maxTimeMS: remainingMilliseconds(this.context) }).toArray();
        return { writable: objects[0]?.type === "collection", columns: [] };
      }
      const columns = await this.sampleColumns(collectionName);
      if (kind === "columns") return { collection: collectionName, columns };
      const indexes = await this.collectionIndexes(collectionName);
      if (kind === "indexes") return { collection: collectionName, indexes };
      if (kind !== "table" && kind !== "collection") {
        throw new Error(`Unknown inspection kind "${kind}".`);
      }
      return {
        collection: collectionName,
        columns,
        indexes: indexes.length,
        constraints: 0,
      };
    } catch (error) {
      throw readError(error, this.context);
    }
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    await this.client.close().catch(() => undefined);
  }

  private async connect(): Promise<void> {
    throwIfStopped(this.context, false);
    if (this.closed) throw new Error("MongoDB adapter is closed.");
    if (this.connected) return;
    if (!this.connecting) {
      this.connecting = withContext(
        this.client.connect().then(() => undefined),
        this.context,
        () => this.stop(),
        false,
      ).then(() => {
        this.connected = true;
      }).finally(() => {
        this.connecting = undefined;
      });
    }
    await this.connecting;
  }

  private async executeWrite(
    command: MongoWriteCommand,
    session?: ClientSession,
  ): Promise<MongoWriteResult> {
    const collection = this.client.db(this.databaseName).collection(command.collection);
    const timeoutMS = remainingMilliseconds(this.context);
    switch (command.operation) {
      case "insertOne": {
        const result = await collection.insertOne(command.document as Document, { session, timeoutMS });
        return {
          affectedRows: result.acknowledged ? 1 : 0,
          outcome: {
            acknowledged: result.acknowledged,
            inserted_id: ejsonSafe(result.insertedId),
          },
        };
      }
      case "insertMany": {
        const options = writeOptions(command.options, command.operation, session, timeoutMS) as BulkWriteOptions;
        const result = await collection.insertMany(command.documents as Document[], options);
        const insertedIds = Object.entries(result.insertedIds)
          .sort(([left], [right]) => Number(left) - Number(right))
          .map(([, id]) => ejsonSafe(id));
        return {
          affectedRows: result.insertedCount,
          outcome: {
            acknowledged: result.acknowledged,
            inserted_count: result.insertedCount,
            inserted_ids: insertedIds,
          },
        };
      }
      case "updateOne":
      case "updateMany": {
        const options = writeOptions(command.options as MongoDocument | undefined, command.operation, session, timeoutMS) as UpdateOptions;
        const result = command.operation === "updateOne"
          ? await collection.updateOne(command.filter as Document, command.update as Document | Document[], options)
          : await collection.updateMany(command.filter as Document, command.update as Document | Document[], options);
        return updateResult(result);
      }
      case "replaceOne": {
        const options = writeOptions(command.options as MongoDocument | undefined, command.operation, session, timeoutMS) as ReplaceOptions;
        const result = await collection.replaceOne(
          command.filter as Document,
          command.replacement as Document,
          options,
        );
        return updateResult(result);
      }
      case "deleteOne":
      case "deleteMany": {
        const options = writeOptions(command.options, command.operation, session, timeoutMS) as DeleteOptions;
        const result = command.operation === "deleteOne"
          ? await collection.deleteOne(command.filter as Document, options)
          : await collection.deleteMany(command.filter as Document, options);
        return {
          affectedRows: result.deletedCount,
          outcome: {
            acknowledged: result.acknowledged,
            deleted_count: result.deletedCount,
          },
        };
      }
    }
  }

  private async requireCollection(name: string): Promise<void> {
    const cursor = this.client.db(this.databaseName).listCollections(
      { name },
      {
        nameOnly: true,
        signal: operationSignal(this.context),
        maxTimeMS: remainingMilliseconds(this.context),
        timeoutMS: remainingMilliseconds(this.context),
      },
    );
    const found = await collectCursor(cursor, 1, this.context);
    if (found.length === 0) throw new Error(`Collection "${name}" was not found.`);
  }

  private async sampleColumns(name: string): Promise<Column[]> {
    const cursor = this.client.db(this.databaseName).collection(name).find(
      {},
      {
        limit: 1,
        signal: operationSignal(this.context),
        maxTimeMS: remainingMilliseconds(this.context),
        timeoutMS: remainingMilliseconds(this.context),
      },
    );
    const documents = await collectCursor(cursor, 1, this.context);
    return inferColumns(documents);
  }

  private async collectionIndexes(name: string): Promise<unknown[]> {
    const cursor = this.client.db(this.databaseName).collection(name).listIndexes({
      maxTimeMS: remainingMilliseconds(this.context),
      timeoutMS: remainingMilliseconds(this.context),
    });
    const indexes = await collectCursor(cursor, MAX_INSPECTION_ITEMS, this.context);
    return ejsonSafe(indexes) as unknown[];
  }

  private stop(): void {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    void this.client.close().catch(() => undefined);
  }
}

function findOptions(
  options: MongoDocument | undefined,
  context: AdapterContext,
  signal: AbortSignal,
  maxRows: number,
): FindOptions & Abortable {
  const source = options ?? {};
  const requestedLimit = source.limit === undefined ? 0 : numericOption(source.limit, "find limit");
  const result: FindOptions & Abortable = {
    signal,
    maxTimeMS: remainingMilliseconds(context),
    timeoutMS: remainingMilliseconds(context),
    limit: requestedLimit > 0 ? Math.min(requestedLimit, maxRows) : maxRows,
  };
  if (source.projection !== undefined) result.projection = source.projection as Document;
  if (source.sort !== undefined) result.sort = normalizeSort(source.sort) as Sort;
  if (source.skip !== undefined) result.skip = numericOption(source.skip, "find skip");
  if (source.hint !== undefined) result.hint = source.hint as Hint;
  if (source.collation !== undefined) result.collation = source.collation as CollationOptions;
  return result;
}

function aggregateOptions(
  options: MongoDocument | undefined,
  context: AdapterContext,
  signal: AbortSignal,
): AggregateOptions & Abortable {
  const source = options ?? {};
  const result: AggregateOptions & Abortable = {
    signal,
    maxTimeMS: remainingMilliseconds(context),
    timeoutMS: remainingMilliseconds(context),
  };
  if (source.allowDiskUse !== undefined) result.allowDiskUse = source.allowDiskUse as boolean;
  if (source.hint !== undefined) result.hint = source.hint as Hint;
  if (source.collation !== undefined) result.collation = source.collation as CollationOptions;
  return result;
}

function writeOptions(
  options: MongoDocument | undefined,
  operation: MongoWriteCommand["operation"],
  session: ClientSession | undefined,
  timeoutMS: number,
): Record<string, unknown> {
  const result: Record<string, unknown> = { session, timeoutMS };
  for (const key of WRITE_OPTIONS[operation]) {
    const value = options?.[key];
    if (value !== undefined) result[key] = value;
  }
  return result;
}

function updateResult(result: {
  acknowledged: boolean;
  matchedCount: number;
  modifiedCount: number;
  upsertedCount: number;
  upsertedId: unknown;
}): MongoWriteResult {
  return {
    affectedRows: result.modifiedCount + result.upsertedCount,
    outcome: {
      acknowledged: result.acknowledged,
      matched_count: result.matchedCount,
      modified_count: result.modifiedCount,
      upserted_count: result.upsertedCount,
      upserted_id: ejsonSafe(result.upsertedId),
    },
  };
}

function documentsResult(documents: Document[]): MongoReadResult {
  return {
    rows: documents.map((document) => ejsonSafe(document) as Row),
    columns: inferColumns(documents),
  };
}

function inferColumns(documents: Document[]): Column[] {
  const types = new Map<string, string>();
  for (const document of documents) {
    for (const [name, value] of Object.entries(document)) {
      const type = mongoType(value);
      const previous = types.get(name);
      if (!previous || previous === "null") types.set(name, type);
      else if (type !== "null" && previous !== type) types.set(name, "mixed");
    }
  }
  return [...types].map(([name, type]) => ({ name, type }));
}

function mongoType(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (Array.isArray(value)) return "array";
  if (value instanceof Date) return "date";
  const bsonType = bsonTypeName(value);
  if (bsonType) return bsonType.replace(/^[A-Z]/, (letter) => letter.toLowerCase());
  if (typeof value === "object") return "object";
  return typeof value;
}

interface CursorLike<T extends Document> {
  next(): Promise<T | null>;
  close(): Promise<void>;
}

async function collectCursor<T extends Document>(
  cursor: CursorLike<T>,
  limit: number,
  context: AdapterContext,
): Promise<T[]> {
  const values: T[] = [];
  try {
    while (values.length < limit) {
      const value = await withContext(
        cursor.next(),
        context,
        () => void cursor.close().catch(() => undefined),
        false,
      );
      if (value === null) break;
      values.push(value);
    }
    return values;
  } finally {
    await cursor.close().catch(() => undefined);
  }
}

function validateFindOptions(value: unknown): void {
  if (value === undefined) return;
  const options = value as MongoDocument;
  if (options.limit !== undefined) numericOption(options.limit, "find limit");
  if (options.skip !== undefined) numericOption(options.skip, "find skip");
  if (options.projection !== undefined && !isDocument(options.projection)) {
    throw new Error("MongoDB find projection must be an object.");
  }
  if (options.sort !== undefined && !isSort(options.sort)) {
    throw new Error("MongoDB find sort must be an object or an array.");
  }
  validateHintAndCollation(options, "find");
}

function validateAggregateOptions(value: unknown): void {
  if (value === undefined) return;
  const options = value as MongoDocument;
  if (options.allowDiskUse !== undefined && typeof options.allowDiskUse !== "boolean") {
    throw new Error("MongoDB aggregate allowDiskUse must be boolean.");
  }
  validateHintAndCollation(options, "aggregate");
}

function validateWriteOptions(
  value: unknown,
  operation: MongoWriteCommand["operation"],
): void {
  if (value === undefined) return;
  const options = value as MongoDocument;
  for (const key of ["upsert", "ordered"] as const) {
    if (options[key] !== undefined && typeof options[key] !== "boolean") {
      throw new Error(`MongoDB ${operation} ${key} must be boolean.`);
    }
  }
  validateHintAndCollation(options, operation);
}

function validateHintAndCollation(options: MongoDocument, operation: string): void {
  if (
    options.hint !== undefined &&
    typeof options.hint !== "string" &&
    !isDocument(options.hint)
  ) {
    throw new Error(`MongoDB ${operation} hint must be a string or object.`);
  }
  if (options.collation !== undefined && !isDocument(options.collation)) {
    throw new Error(`MongoDB ${operation} collation must be an object.`);
  }
}

function validateUpdate(value: unknown): void {
  if (Array.isArray(value)) {
    if (value.length === 0 || !value.every(isDocument)) {
      throw new Error("MongoDB update pipeline must be a non-empty array of stage objects.");
    }
    for (const stage of value) {
      const keys = Object.keys(stage);
      if (keys.length !== 1 || !UPDATE_PIPELINE_STAGES.has(keys[0] as string)) {
        throw new Error("MongoDB update pipeline contains an unsupported stage.");
      }
    }
    return;
  }
  requireDocument(value, "update document");
  const keys = Object.keys(value as MongoDocument);
  if (keys.length === 0 || keys.some((key) => !key.startsWith("$"))) {
    throw new Error("MongoDB update documents must contain only top-level update operators.");
  }
}

function validateOptions(
  value: unknown,
  allowed: ReadonlySet<string>,
  operation: string,
): void {
  if (value === undefined) return;
  if (!isDocument(value)) throw new Error(`MongoDB ${operation} options must be an object.`);
  rejectUnknownKeys(value, allowed, `${operation} options`);
}

function validateCollection(value: unknown): asserts value is string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.includes("\0") ||
    value.includes("$") ||
    value.startsWith("system.")
  ) {
    throw new Error("Invalid MongoDB collection name.");
  }
}

function requireDocument(value: unknown, name: string): asserts value is MongoDocument {
  if (!isDocument(value)) throw new Error(`MongoDB ${name} must be an object.`);
}

function rejectUnknownKeys(
  value: MongoDocument,
  allowed: ReadonlySet<string>,
  name: string,
): void {
  const key = Object.keys(value).find((candidate) => !allowed.has(candidate));
  if (key) throw new Error(`Unknown MongoDB ${name} field "${key}".`);
}

function rejectForbiddenValues(value: unknown): void {
  const seen = new WeakSet<object>();
  const visit = (item: unknown): void => {
    if (typeof item === "function") throw new Error("MongoDB server-side JavaScript is forbidden.");
    if (!item || typeof item !== "object") return;
    const object = item as object;
    const bsonType = bsonTypeName(item);
    if (bsonType === "Code") throw new Error("MongoDB server-side JavaScript is forbidden.");
    if (bsonType || item instanceof Date || item instanceof RegExp || Buffer.isBuffer(item)) return;
    if (seen.has(object)) throw new Error("MongoDB commands cannot contain circular values.");
    seen.add(object);
    if (item instanceof Map) {
      for (const [key, child] of item) {
        if (FORBIDDEN_KEYS.has(String(key))) forbiddenOperator(String(key));
        visit(child);
      }
    } else if (item instanceof Set || Array.isArray(item)) {
      for (const child of item) visit(child);
    } else {
      for (const [key, child] of Object.entries(item)) {
        if (FORBIDDEN_KEYS.has(key)) forbiddenOperator(key);
        visit(child);
      }
    }
    seen.delete(object);
  };
  visit(value);
}

function forbiddenOperator(key: string): never {
  throw new Error(`MongoDB operator "${key}" is forbidden.`);
}

function numericOption(value: unknown, name: string): number {
  const number = typeof value === "number"
    ? value
    : bsonTypeName(value) === "Int32" || bsonTypeName(value) === "Long" || bsonTypeName(value) === "Double"
      ? Number((value as { valueOf(): unknown }).valueOf())
      : Number.NaN;
  if (!Number.isSafeInteger(number) || number < 0 || number > 2_147_483_647) {
    throw new Error(`MongoDB ${name} must be an integer from 0 through 2147483647.`);
  }
  return number;
}

function normalizeSort(value: unknown): unknown {
  if (!Array.isArray(value)) return value;
  return value.map((item) => {
    if (!Array.isArray(item)) return item;
    return item.map((part, index) => index === 1 ? numericBsonValue(part) : part);
  });
}

function numericBsonValue(value: unknown): unknown {
  const type = bsonTypeName(value);
  return type === "Int32" || type === "Long" || type === "Double"
    ? Number((value as { valueOf(): unknown }).valueOf())
    : value;
}

function isSort(value: unknown): boolean {
  return typeof value === "string" || Array.isArray(value) || isDocument(value);
}

function isDocument(value: unknown): value is MongoDocument {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value) && !bsonTypeName(value);
}

function isRecord(value: unknown): value is MongoDocument {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}


function isMongoWriteOperation(value: unknown): value is MongoWriteCommand["operation"] {
  return value === "insertOne" || value === "insertMany" ||
    value === "updateOne" || value === "updateMany" ||
    value === "replaceOne" || value === "deleteOne" || value === "deleteMany";
}

function bsonTypeName(value: unknown): string | undefined {
  if (!value || typeof value !== "object") return undefined;
  const type = (value as { _bsontype?: unknown })._bsontype;
  return typeof type === "string" ? type : undefined;
}

function parseEjson(value: string): unknown {
  try {
    return BSON.EJSON.parse(value, { relaxed: false }) as unknown;
  } catch {
    throw new Error("Stored MongoDB command is not valid EJSON.");
  }
}

function ejsonSafe<T>(value: T): unknown {
  if (value === undefined) return null;
  return JSON.parse(BSON.EJSON.stringify(value, { relaxed: false })) as unknown;
}

function remainingMilliseconds(context: AdapterContext): number {
  return Math.max(1, Math.min(2_147_483_647, Math.ceil(context.deadline - Date.now())));
}

function operationSignal(context: AdapterContext): AbortSignal {
  const deadlineSignal = AbortSignal.timeout(remainingMilliseconds(context));
  return context.signal ? AbortSignal.any([context.signal, deadlineSignal]) : deadlineSignal;
}

function throwIfStopped(context: AdapterContext, outcomeUnknown: boolean): void {
  if (context.signal?.aborted || context.deadline <= Date.now()) {
    throw stoppedError(context, outcomeUnknown);
  }
}

function stoppedError(context: AdapterContext, outcomeUnknown: boolean): AdapterExecutionError {
  const aborted = context.signal?.aborted ?? false;
  return new AdapterExecutionError(
    aborted ? "Database operation cancelled." : "Database operation timed out.",
    aborted ? "aborted" : "timeout",
    outcomeUnknown,
  );
}

function withContext<T>(
  promise: Promise<T>,
  context: AdapterContext,
  onStop: () => void,
  outcomeUnknown: boolean,
): Promise<T> {
  try {
    throwIfStopped(context, outcomeUnknown);
  } catch (error) {
    return Promise.reject(error);
  }
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (action: () => void): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      context.signal?.removeEventListener("abort", stop);
      action();
    };
    const stop = (): void => finish(() => {
      try {
        onStop();
      } catch {
        // Preserve cancellation/deadline error.
      }
      reject(stoppedError(context, outcomeUnknown));
    });
    const timer = setTimeout(stop, remainingMilliseconds(context));
    context.signal?.addEventListener("abort", stop, { once: true });
    promise.then(
      (result) => context.signal?.aborted || context.deadline <= Date.now()
        ? stop()
        : finish(() => resolve(result)),
      (error) => context.signal?.aborted || context.deadline <= Date.now()
        ? stop()
        : finish(() => reject(error)),
    );
  });
}

function readError(error: unknown, context: AdapterContext): unknown {
  if (error instanceof AdapterExecutionError) return error;
  if (isTimeoutError(error) || context.signal?.aborted || context.deadline <= Date.now()) {
    return stoppedError(context, false);
  }
  return error;
}

function writeStoppedError(
  error: unknown,
  context: AdapterContext,
  outcomeUnknown: boolean,
): AdapterExecutionError | undefined {
  if (error instanceof AdapterExecutionError) {
    return new AdapterExecutionError(error.message, error.reason, outcomeUnknown);
  }
  if (isTimeoutError(error) || context.signal?.aborted || context.deadline <= Date.now()) {
    return stoppedError(context, outcomeUnknown);
  }
  return undefined;
}

function isTimeoutError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const name = (error as { name?: unknown }).name;
  const code = (error as { code?: unknown }).code;
  return name === "MongoOperationTimeoutError" || name === "MongoNetworkTimeoutError" ||
    code === 50 || code === "ETIMEDOUT";
}

function knownNoWrite(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const name = String((error as { name?: unknown }).name ?? "");
  const code = (error as { code?: unknown }).code;
  if (["MongoInvalidArgumentError", "MongoParseError", "MongoCompatibilityError"].includes(name)) return true;
  if ([2, 9, 14, 20, 59, 72, 303].includes(code as number)) return true;
  const message = errorText(error);
  return /transaction numbers are only allowed|does not support transactions|transactions are not supported/i.test(message);
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
