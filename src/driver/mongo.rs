use std::collections::HashMap;
use std::time::Instant;

use mongodb::IndexModel;
use mongodb::bson::{self, Bson, Document, doc};
use mongodb::error::{ErrorKind, WriteFailure};
use mongodb::options::{
    AggregateOptions, ClientOptions, CountOptions, DeleteOptions, DistinctOptions, FindOptions,
    IndexOptions, InsertManyOptions, ReplaceOptions, UpdateModifications, UpdateOptions,
};
use mongodb::sync::{Client, Collection, Database};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::{Cell, Column, Driver, ObjectQuery, Statement, WriteKind, remaining};
use crate::command::Isolation;
use crate::error::{Code, Error, Executed, Result};
use crate::results::RowSink;

const FORBIDDEN_KEYS: [&str; 6] = [
    "$out",
    "$merge",
    "$changeStream",
    "$where",
    "$function",
    "$accumulator",
];
const UPDATE_PIPELINE_STAGES: [&str; 6] = [
    "$addFields",
    "$set",
    "$project",
    "$unset",
    "$replaceRoot",
    "$replaceWith",
];
const EXPLAIN_VERBOSITY: [&str; 3] = ["queryPlanner", "executionStats", "allPlansExecution"];
const FILTER_WRITES: [&str; 5] = [
    "updateOne",
    "updateMany",
    "replaceOne",
    "deleteOne",
    "deleteMany",
];
const SINGLE_DOCUMENT_WRITES: [&str; 4] = ["insertOne", "updateOne", "replaceOne", "deleteOne"];
// Codes the server returns before it changes anything, as in StateQL 0.20 (TypeScript).
const NO_WRITE_CODES: [i32; 7] = [2, 9, 14, 20, 59, 72, 303];
const DESCRIBE_SAMPLE: i64 = 100;
const OPERATIONS: &str = "Reads: find, aggregate, countDocuments, distinct. Writes: insertOne, insertMany, updateOne, updateMany, replaceOne, deleteOne, deleteMany, createIndex, dropIndex.";

struct Shape {
    keys: &'static [&'static str],
    options: &'static [&'static str],
    write: bool,
}

fn shape(operation: &str) -> Option<Shape> {
    let (keys, options, write): (&[&str], &[&str], bool) = match operation {
        "find" => (
            &["filter", "options", "explain"],
            &["projection", "sort", "skip", "limit", "hint", "collation"],
            false,
        ),
        "aggregate" => (
            &["pipeline", "options", "explain"],
            &["allowDiskUse", "hint", "collation"],
            false,
        ),
        "countDocuments" => (
            &["filter", "options"],
            &["skip", "limit", "hint", "collation"],
            false,
        ),
        "distinct" => (&["field", "filter", "options"], &["collation"], false),
        "insertOne" => (&["document", "options"], &[], true),
        "insertMany" => (&["documents", "options"], &["ordered"], true),
        "updateOne" | "updateMany" => (
            &["filter", "update", "options"],
            &["upsert", "collation", "hint"],
            true,
        ),
        "replaceOne" => (
            &["filter", "replacement", "options"],
            &["upsert", "collation", "hint"],
            true,
        ),
        "deleteOne" | "deleteMany" => (&["filter", "options"], &["collation", "hint"], true),
        "createIndex" => (
            &["keys", "options"],
            &[
                "name",
                "unique",
                "sparse",
                "expireAfterSeconds",
                "partialFilterExpression",
                "collation",
            ],
            true,
        ),
        "dropIndex" => (&["index"], &[], true),
        _ => return None,
    };
    Some(Shape {
        keys,
        options,
        write,
    })
}

struct MongoCommand {
    operation: String,
    write: bool,
    collection: String,
    body: Document,
}

impl MongoCommand {
    fn document(&self, key: &str) -> Document {
        self.body.get_document(key).cloned().unwrap_or_default()
    }

    fn filter(&self) -> Document {
        self.document("filter")
    }

    fn options(&self) -> Document {
        let mut options = self.document("options");
        if let Some(Bson::Array(pairs)) = options.get("sort") {
            let sort: Document = pairs
                .iter()
                .filter_map(|pair| match pair.as_array().map(Vec::as_slice) {
                    Some([Bson::String(field), direction]) => {
                        Some((field.clone(), direction.clone()))
                    }
                    _ => None,
                })
                .collect();
            options.insert("sort", sort);
        }
        options
    }

    fn documents(&self, key: &str) -> Vec<Document> {
        let Ok(items) = self.body.get_array(key) else {
            return Vec::new();
        };
        items
            .iter()
            .filter_map(|item| item.as_document().cloned())
            .collect()
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::invalid(message)
}

fn parse_command(stmt: &Statement) -> Result<MongoCommand> {
    if !stmt.params.is_empty() {
        return Err(invalid("MongoDB commands take no --param or --params."));
    }
    let json: Value = serde_json::from_str(&stmt.text)
        .map_err(|error| invalid(format!("The MongoDB command is not valid JSON: {error}")))?;
    let parsed = Bson::try_from(json).map_err(|error| {
        invalid(format!(
            "The MongoDB command is not valid extended JSON: {error}"
        ))
    })?;
    let Bson::Document(mut body) = parsed else {
        return Err(invalid("The MongoDB command must be a JSON object."));
    };
    let operation = match body.remove("operation") {
        Some(Bson::String(operation)) => operation,
        _ => {
            return Err(invalid(format!(
                "The MongoDB command needs \"operation\". {OPERATIONS}"
            )));
        }
    };
    let collection = match body.remove("collection") {
        Some(Bson::String(name)) if valid_collection(&name) => name,
        _ => {
            return Err(invalid(
                "\"collection\" must be a collection name that does not start with system.",
            ));
        }
    };
    let Some(shape) = shape(&operation) else {
        return Err(invalid(format!(
            "Unknown MongoDB operation \"{operation}\". {OPERATIONS}"
        )));
    };
    reject_unknown_keys(&body, shape.keys, "command")?;
    if let Some(options) = body.get("options") {
        let Bson::Document(options) = options else {
            return Err(invalid("MongoDB options must be an object."));
        };
        reject_unknown_keys(options, shape.options, &format!("{operation} options"))?;
        validate_options(options)?;
    }
    validate_body(&operation, &body)?;
    reject_forbidden(&Bson::Document(body.clone()))?;
    Ok(MongoCommand {
        operation,
        write: shape.write,
        collection,
        body,
    })
}

fn valid_collection(name: &str) -> bool {
    !name.is_empty() && !name.contains(['\0', '$']) && !name.starts_with("system.")
}

fn reject_unknown_keys(document: &Document, allowed: &[&str], what: &str) -> Result<()> {
    match document.keys().find(|key| !allowed.contains(&key.as_str())) {
        Some(key) => Err(invalid(format!("Unknown MongoDB {what} field \"{key}\"."))),
        None => Ok(()),
    }
}

fn whole_number(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(number) => Some(i64::from(*number)),
        Bson::Int64(number) => Some(*number),
        Bson::Double(number) if number.fract() == 0.0 && number.abs() < 1e15 => {
            Some(*number as i64)
        }
        _ => None,
    }
}

fn validate_options(options: &Document) -> Result<()> {
    for (key, value) in options {
        let valid = match key.as_str() {
            "limit" | "skip" | "expireAfterSeconds" => {
                whole_number(value).is_some_and(|number| (0..=2_147_483_647).contains(&number))
            }
            "projection" | "collation" | "partialFilterExpression" => {
                matches!(value, Bson::Document(_))
            }
            "sort" => matches!(value, Bson::Document(_) | Bson::Array(_)),
            "hint" => matches!(value, Bson::Document(_) | Bson::String(_)),
            "name" => {
                matches!(value, Bson::String(name) if !name.is_empty() && !name.contains('\0'))
            }
            _ => matches!(value, Bson::Boolean(_)),
        };
        if !valid {
            return Err(invalid(format!(
                "MongoDB option \"{key}\" has a value of the wrong type."
            )));
        }
    }
    Ok(())
}

fn require_document<'a>(body: &'a Document, key: &str, operation: &str) -> Result<&'a Document> {
    body.get_document(key)
        .map_err(|_| invalid(format!("MongoDB {operation} {key} must be an object.")))
}

fn validate_body(operation: &str, body: &Document) -> Result<()> {
    if let Some(explain) = body.get("explain")
        && !matches!(explain, Bson::String(verbosity) if EXPLAIN_VERBOSITY.contains(&verbosity.as_str()))
    {
        return Err(invalid(
            "MongoDB explain must be queryPlanner, executionStats, or allPlansExecution.",
        ));
    }
    let needs_filter = FILTER_WRITES.contains(&operation);
    if needs_filter || body.contains_key("filter") {
        require_document(body, "filter", operation)?;
    }
    match operation {
        "aggregate" => {
            let stages = body
                .get_array("pipeline")
                .map_err(|_| invalid("MongoDB aggregate pipeline must be an array of stages."))?;
            let one_operator = stages
                .iter()
                .all(|stage| stage.as_document().is_some_and(|stage| stage.len() == 1));
            if !one_operator {
                return Err(invalid(
                    "Each MongoDB aggregate stage must be an object with exactly one operator.",
                ));
            }
        }
        "distinct" => {
            let field = body.get_str("field").unwrap_or_default();
            if field.is_empty() || field.starts_with('$') || field.contains('\0') {
                return Err(invalid("MongoDB distinct field must be a field name."));
            }
        }
        "insertOne" => {
            require_document(body, "document", operation)?;
        }
        "insertMany" => {
            let documents = body
                .get_array("documents")
                .ok()
                .filter(|items| !items.is_empty());
            if !documents.is_some_and(|items| items.iter().all(|item| item.as_document().is_some()))
            {
                return Err(invalid(
                    "MongoDB insertMany documents must be a non-empty array of objects.",
                ));
            }
        }
        "updateOne" | "updateMany" => validate_update(body.get("update"))?,
        "replaceOne" => {
            let replacement = require_document(body, "replacement", operation)?;
            if replacement.keys().any(|key| key.starts_with('$')) {
                return Err(invalid(
                    "A MongoDB replacement cannot contain update operators.",
                ));
            }
        }
        "createIndex" => {
            let keys = require_document(body, "keys", operation)?;
            let valid_key = |field: &String, kind: &Bson| {
                let kind_ok = match kind {
                    Bson::String(kind) => {
                        ["text", "2dsphere", "2d", "hashed"].contains(&kind.as_str())
                    }
                    other => matches!(whole_number(other), Some(1 | -1)),
                };
                !field.is_empty() && !field.starts_with('$') && kind_ok
            };
            if keys.is_empty() || !keys.iter().all(|(field, kind)| valid_key(field, kind)) {
                return Err(invalid(
                    "MongoDB createIndex keys must map field names to 1, -1, text, 2dsphere, 2d, or hashed.",
                ));
            }
        }
        "dropIndex" => {
            let index = body.get_str("index").unwrap_or_default();
            if index.is_empty() || index.contains('\0') || index == "*" || index == "_id_" {
                return Err(invalid(
                    "MongoDB dropIndex index must name one index other than _id_.",
                ));
            }
        }
        _ => {}
    }
    Ok(())
}

fn validate_update(update: Option<&Bson>) -> Result<()> {
    match update {
        Some(Bson::Array(stages)) => {
            let allowed = |stage: &Bson| {
                stage.as_document().is_some_and(|stage| {
                    stage.len() == 1
                        && stage
                            .keys()
                            .all(|key| UPDATE_PIPELINE_STAGES.contains(&key.as_str()))
                })
            };
            if stages.is_empty() || !stages.iter().all(allowed) {
                return Err(invalid(
                    "A MongoDB update pipeline may use only $addFields, $set, $project, $unset, $replaceRoot, and $replaceWith.",
                ));
            }
            Ok(())
        }
        Some(Bson::Document(update))
            if !update.is_empty() && update.keys().all(|key| key.starts_with('$')) =>
        {
            Ok(())
        }
        _ => Err(invalid(
            "A MongoDB update must be an object of update operators or an update pipeline.",
        )),
    }
}

fn reject_forbidden(value: &Bson) -> Result<()> {
    match value {
        Bson::JavaScriptCode(_) | Bson::JavaScriptCodeWithScope(_) => {
            Err(invalid("MongoDB server-side JavaScript is forbidden."))
        }
        Bson::Document(document) => {
            for (key, child) in document {
                if FORBIDDEN_KEYS.contains(&key.as_str()) {
                    return Err(Error::new(
                        Code::PermissionDenied,
                        format!("MongoDB operator \"{key}\" is forbidden."),
                    ));
                }
                reject_forbidden(child)?;
            }
            Ok(())
        }
        Bson::Array(items) => items.iter().try_for_each(reject_forbidden),
        _ => Ok(()),
    }
}

fn typed<T: DeserializeOwned>(options: Document, keep: &[&str]) -> Result<T> {
    let kept: Document = options
        .into_iter()
        .filter(|(key, _)| keep.contains(&key.as_str()))
        .collect();
    bson::from_document(kept).map_err(|error| invalid(format!("Invalid MongoDB options: {error}")))
}

fn read_error(error: mongodb::error::Error) -> Error {
    match error.kind.as_ref() {
        ErrorKind::Command(command) if command.code == 50 => Error::new(
            Code::DeadlineExceeded,
            "The operation passed its deadline and was stopped.",
        ),
        ErrorKind::Command(command) if command.code == 13 => {
            Error::new(Code::PermissionDenied, command.message.clone())
        }
        ErrorKind::ServerSelection { .. } => super::connection_failed(error),
        _ => Error::query(error),
    }
}

fn write_error(error: mongodb::error::Error, operation: &str) -> Error {
    let not_sent = match error.kind.as_ref() {
        ErrorKind::InvalidArgument { .. }
        | ErrorKind::ServerSelection { .. }
        | ErrorKind::Authentication { .. } => true,
        ErrorKind::Command(command) => NO_WRITE_CODES.contains(&command.code),
        ErrorKind::Write(WriteFailure::WriteError(_)) => {
            SINGLE_DOCUMENT_WRITES.contains(&operation)
        }
        _ => false,
    };
    let mapped = read_error(error);
    if not_sent {
        mapped
    } else {
        mapped.with_executed(Executed::Unknown)
    }
}

fn type_name(value: &Bson) -> String {
    let name = match value {
        Bson::Null | Bson::Undefined => "null",
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => "number",
        Bson::String(_) => "string",
        Bson::Boolean(_) => "boolean",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::DateTime(_) => "date",
        Bson::ObjectId(_) => "objectId",
        Bson::Binary(_) => "binary",
        Bson::Decimal128(_) => "decimal128",
        other => return format!("{:?}", other.element_type()).to_lowercase(),
    };
    name.to_string()
}

fn is_nested(value: &Bson) -> bool {
    matches!(value, Bson::Document(_) | Bson::Array(_))
}

// A JSON column keeps every text cell as JSON, so a string stays a string in the output.
fn cell(value: Bson, json_column: bool) -> Cell {
    let text = |text: String| {
        if json_column {
            Cell::Text(Value::String(text).to_string())
        } else {
            Cell::Text(text)
        }
    };
    match value {
        Bson::Null | Bson::Undefined => Cell::Null,
        Bson::Int32(number) => Cell::Integer(number.into()),
        Bson::Int64(number) => Cell::Integer(number),
        Bson::Boolean(flag) => Cell::Integer(flag.into()),
        Bson::Double(number) if number.is_finite() => Cell::Real(number),
        Bson::String(value) => text(value),
        Bson::Binary(binary) => Cell::Blob(binary.bytes),
        Bson::ObjectId(id) => text(id.to_hex()),
        Bson::DateTime(date) => text(
            date.try_to_rfc3339_string()
                .unwrap_or_else(|_| date.timestamp_millis().to_string()),
        ),
        Bson::Decimal128(number) => text(number.to_string()),
        other => Cell::Text(other.into_relaxed_extjson().to_string()),
    }
}

#[derive(Default)]
struct Flattener {
    index: HashMap<String, usize>,
    types: Vec<String>,
    json: Vec<bool>,
}

impl Flattener {
    fn push(&mut self, sink: &mut RowSink, document: Document) -> Result<()> {
        let mut entries: Vec<(String, Bson)> = document.into_iter().collect();
        entries.sort_by_key(|(key, _)| key != "_id");
        let mut placed = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let index = self.column(sink, key, &value)?;
            placed.push((index, value));
        }
        let mut row = vec![Cell::Null; self.types.len()];
        for (index, value) in placed {
            row[index] = cell(value, self.json[index]);
        }
        sink.push(row)
    }

    fn column(&mut self, sink: &mut RowSink, key: String, value: &Bson) -> Result<usize> {
        let kind = type_name(value);
        let Some(&index) = self.index.get(&key) else {
            sink.add_column(Column {
                name: key.clone(),
                type_name: kind.clone(),
                json: is_nested(value),
            })?;
            self.index.insert(key, self.types.len());
            self.types.push(kind);
            self.json.push(is_nested(value));
            return Ok(self.types.len() - 1);
        };
        let previous = &self.types[index];
        if *previous == kind || kind == "null" {
            return Ok(index);
        }
        // Only NULL cells exist before the first typed value, so the JSON flag can still change.
        if previous == "null" {
            self.json[index] = is_nested(value);
            self.types[index] = kind;
        } else {
            self.types[index] = "mixed".to_string();
        }
        sink.update_column(index, &self.types[index], self.json[index]);
        Ok(index)
    }

    fn finish(&self, sink: &mut RowSink, fallback: &str) -> Result<()> {
        if self.types.is_empty() {
            sink.add_column(Column::new(fallback, "null"))?;
        }
        Ok(())
    }
}

pub struct MongoDriver {
    db: Database,
}

impl MongoDriver {
    pub fn open(target: &str, deadline: Instant) -> Result<MongoDriver> {
        let mut options = ClientOptions::parse(target)
            .run()
            .map_err(super::connection_failed)?;
        let timeout = remaining(deadline)?;
        options.connect_timeout = Some(timeout);
        options.server_selection_timeout = Some(timeout);
        let Some(name) = options
            .default_database
            .clone()
            .filter(|name| !name.is_empty())
        else {
            return Err(invalid(
                "The MongoDB URL must name a database: mongodb://host/DATABASE.",
            ));
        };
        let client = Client::with_options(options).map_err(super::connection_failed)?;
        let db = client.database(&name);
        db.run_command(doc! {"ping": 1})
            .run()
            .map_err(super::connection_failed)?;
        Ok(MongoDriver { db })
    }

    fn collection(&self, command: &MongoCommand) -> Collection<Document> {
        self.db.collection(&command.collection)
    }

    fn explain(&self, command: &MongoCommand, verbosity: &str) -> Result<Document> {
        let mut inner = match command.operation.as_str() {
            "find" => doc! {"find": &command.collection, "filter": command.filter()},
            _ => {
                doc! {"aggregate": &command.collection, "pipeline": command.documents("pipeline"), "cursor": {}}
            }
        };
        inner.extend(command.options());
        self.db
            .run_command(doc! {"explain": inner, "verbosity": verbosity})
            .run()
            .map_err(read_error)
    }

    fn write(&self, command: &MongoCommand) -> Result<u64> {
        let collection = self.collection(command);
        let options = command.options();
        let operation = command.operation.as_str();
        let failed = |error| write_error(error, operation);
        let affected = match operation {
            "insertOne" => {
                collection
                    .insert_one(command.document("document"))
                    .run()
                    .map_err(failed)?;
                1
            }
            "insertMany" => {
                let options: InsertManyOptions = typed(options, &["ordered"])?;
                let result = collection
                    .insert_many(command.documents("documents"))
                    .with_options(options)
                    .run()
                    .map_err(failed)?;
                result.inserted_ids.len() as u64
            }
            "updateOne" | "updateMany" => {
                let update = match command.body.get("update") {
                    Some(Bson::Array(_)) => {
                        UpdateModifications::Pipeline(command.documents("update"))
                    }
                    _ => UpdateModifications::Document(command.document("update")),
                };
                let options: UpdateOptions = typed(options, &["upsert", "collation", "hint"])?;
                let action = if operation == "updateOne" {
                    collection.update_one(command.filter(), update)
                } else {
                    collection.update_many(command.filter(), update)
                };
                let result = action.with_options(options).run().map_err(failed)?;
                result.modified_count + u64::from(result.upserted_id.is_some())
            }
            "replaceOne" => {
                let options: ReplaceOptions = typed(options, &["upsert", "collation", "hint"])?;
                let result = collection
                    .replace_one(command.filter(), command.document("replacement"))
                    .with_options(options)
                    .run()
                    .map_err(failed)?;
                result.modified_count + u64::from(result.upserted_id.is_some())
            }
            "deleteOne" | "deleteMany" => {
                let options: DeleteOptions = typed(options, &["collation", "hint"])?;
                let action = if operation == "deleteOne" {
                    collection.delete_one(command.filter())
                } else {
                    collection.delete_many(command.filter())
                };
                action
                    .with_options(options)
                    .run()
                    .map_err(failed)?
                    .deleted_count
            }
            "createIndex" => {
                let index_options: IndexOptions = typed(
                    options,
                    &[
                        "name",
                        "unique",
                        "sparse",
                        "expireAfterSeconds",
                        "partialFilterExpression",
                        "collation",
                    ],
                )?;
                let model = IndexModel::builder()
                    .keys(command.document("keys"))
                    .options(index_options)
                    .build();
                collection.create_index(model).run().map_err(failed)?;
                0
            }
            _ => {
                collection
                    .drop_index(command.body.get_str("index").unwrap_or_default())
                    .run()
                    .map_err(failed)?;
                0
            }
        };
        Ok(affected)
    }
}

impl Driver for MongoDriver {
    fn read(
        &mut self,
        stmt: &Statement,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        let command = parse_command(stmt)?;
        if command.write {
            return Err(Error::new(
                Code::ReadOnly,
                "query runs only find, aggregate, countDocuments, and distinct.",
            )
            .with_hint("use exec"));
        }
        let collection = self.collection(&command);
        let max_time = remaining(deadline)?;
        let mut rows = Flattener::default();
        let options = command.options();
        if let Some(Bson::String(verbosity)) = command.body.get("explain") {
            rows.push(sink, self.explain(&command, verbosity)?)?;
            return Ok(None);
        }
        match command.operation.as_str() {
            "countDocuments" => {
                let mut options: CountOptions =
                    typed(options, &["skip", "limit", "hint", "collation"])?;
                options.max_time = Some(max_time);
                let count = collection
                    .count_documents(command.filter())
                    .with_options(options)
                    .run()
                    .map_err(read_error)?;
                rows.push(sink, doc! {"count": count as i64})?;
            }
            "distinct" => {
                let mut options: DistinctOptions = typed(options, &["collation"])?;
                options.max_time = Some(max_time);
                let field = command.body.get_str("field").unwrap_or_default();
                let values = collection
                    .distinct(field, command.filter())
                    .with_options(options)
                    .run()
                    .map_err(read_error)?;
                for value in values {
                    rows.push(sink, doc! {"value": value})?;
                }
                rows.finish(sink, "value")?;
            }
            "find" => {
                let mut options: FindOptions = typed(
                    options,
                    &["projection", "sort", "skip", "limit", "hint", "collation"],
                )?;
                options.max_time = Some(max_time);
                let cursor = collection
                    .find(command.filter())
                    .with_options(options)
                    .run()
                    .map_err(read_error)?;
                for document in cursor {
                    rows.push(sink, document.map_err(read_error)?)?;
                }
                rows.finish(sink, "_id")?;
            }
            _ => {
                let mut options: AggregateOptions =
                    typed(options, &["allowDiskUse", "hint", "collation"])?;
                options.max_time = Some(max_time);
                let cursor = collection
                    .aggregate(command.documents("pipeline"))
                    .with_options(options)
                    .run()
                    .map_err(read_error)?;
                for document in cursor {
                    rows.push(sink, document.map_err(read_error)?)?;
                }
                rows.finish(sink, "_id")?;
            }
        }
        Ok(None)
    }

    fn classify(&self, stmt: &Statement) -> Result<WriteKind> {
        let command = parse_command(stmt)?;
        if !command.write {
            return Err(
                Error::new(Code::InvalidSql, "exec runs only MongoDB writes.")
                    .with_hint("use query"),
            );
        }
        let unbounded =
            FILTER_WRITES.contains(&command.operation.as_str()) && command.filter().is_empty();
        let destructive = matches!(
            command.operation.as_str(),
            "replaceOne" | "deleteOne" | "deleteMany" | "dropIndex"
        );
        Ok(WriteKind {
            statement_type: command.operation,
            unbounded,
            destructive,
        })
    }

    fn execute(&mut self, stmt: &Statement, deadline: Instant) -> Result<u64> {
        remaining(deadline)?;
        self.write(&parse_command(stmt)?)
    }

    // MongoDB has no row count inside the write, so the check counts first. This is not atomic.
    fn write_expecting(
        &mut self,
        stmt: &Statement,
        expected: u64,
        deadline: Instant,
    ) -> Result<u64> {
        let command = parse_command(stmt)?;
        if !FILTER_WRITES.contains(&command.operation.as_str()) {
            return Err(super::unsupported(
                "--expect-rows on MongoDB needs updateOne, updateMany, replaceOne, deleteOne, or deleteMany.",
            ));
        }
        let mut options: CountOptions = typed(command.options(), &["hint", "collation"])?;
        options.max_time = Some(remaining(deadline)?);
        let count = self
            .collection(&command)
            .count_documents(command.filter())
            .with_options(options)
            .run()
            .map_err(read_error)?;
        if count != expected {
            return Err(super::row_count_mismatch(expected, count));
        }
        self.write(&command)
    }

    fn begin(&mut self, _isolation: Option<Isolation>) -> Result<()> {
        Err(super::unsupported(
            "MongoDB does not run inside pipe --atomic.",
        ))
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }

    fn in_transaction(&self) -> bool {
        false
    }

    fn objects(
        &mut self,
        query: &ObjectQuery,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        let mut filter = Document::new();
        match query.kind.as_deref() {
            None => {}
            Some(kind @ ("collection" | "view")) => {
                filter.insert("type", kind);
            }
            Some(_) => return Err(invalid("MongoDB object kinds are collection and view.")),
        }
        let offset = super::offset_cursor(query.cursor.as_deref())?;
        remaining(deadline)?;
        let specs = self
            .db
            .list_collections()
            .filter(filter)
            .run()
            .map_err(read_error)?;
        let search = query.search.as_deref().map(str::to_lowercase);
        let mut found = Vec::new();
        for spec in specs {
            let spec = spec.map_err(read_error)?;
            let matches = search
                .as_deref()
                .is_none_or(|search| spec.name.to_lowercase().contains(search));
            if matches && !spec.name.starts_with("system.") {
                let kind = if matches!(spec.collection_type, mongodb::results::CollectionType::View)
                {
                    "view"
                } else {
                    "collection"
                };
                found.push((spec.name, kind));
            }
        }
        found.sort();
        sink.set_columns(vec![
            Column::new("kind", "string"),
            Column::new("name", "string"),
        ])?;
        let page = found
            .into_iter()
            .skip(offset as usize)
            .take(query.limit as usize);
        for (name, kind) in page {
            sink.push(vec![Cell::Text(kind.to_string()), Cell::Text(name)])?;
        }
        Ok(super::next_offset_cursor(offset, query.limit, sink.total()))
    }

    fn describe(&mut self, name: &str, deadline: Instant) -> Result<Value> {
        let max_time = remaining(deadline)?;
        let mut specs = self
            .db
            .list_collections()
            .filter(doc! {"name": name})
            .run()
            .map_err(read_error)?;
        let Some(spec) = specs.next().transpose().map_err(read_error)? else {
            return Err(Error::new(
                Code::ResultNotFound,
                format!("No collection named \"{name}\"."),
            )
            .with_hint("use objects"));
        };
        let view = matches!(spec.collection_type, mongodb::results::CollectionType::View);
        let collection: Collection<Document> = self.db.collection(name);
        let mut indexes = Vec::new();
        if !view {
            for index in collection.list_indexes().run().map_err(read_error)? {
                let index = bson::to_bson(&index.map_err(read_error)?).map_err(Error::internal)?;
                indexes.push(index.into_relaxed_extjson());
            }
        }
        let sample = collection
            .find(doc! {})
            .limit(DESCRIBE_SAMPLE)
            .max_time(max_time)
            .run()
            .map_err(read_error)?;
        let mut fields: Vec<(String, String)> = Vec::new();
        let mut sampled = 0;
        for document in sample {
            sampled += 1;
            for (key, value) in document.map_err(read_error)? {
                let kind = type_name(&value);
                match fields.iter_mut().find(|(name, _)| *name == key) {
                    None => fields.push((key, kind)),
                    Some((_, previous)) if previous == "null" => *previous = kind,
                    Some((_, previous)) if *previous != kind && kind != "null" => {
                        *previous = "mixed".to_string()
                    }
                    Some(_) => {}
                }
            }
        }
        fields.sort_by_key(|(key, _)| key != "_id");
        let options = bson::to_bson(&spec.options).map_err(Error::internal)?;
        Ok(json!({
            "name": spec.name,
            "kind": if view { "view" } else { "collection" },
            "options": options.into_relaxed_extjson(),
            "indexes": indexes,
            "fields": fields.iter().map(|(name, kind)| json!({"name": name, "type": kind})).collect::<Vec<_>>(),
            "sampled_documents": sampled,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn statement(text: &str) -> Statement {
        Statement {
            text: text.to_string(),
            params: Vec::new(),
        }
    }

    #[test]
    fn commands_follow_the_ts_allowlists() {
        let error = |text: &str| {
            parse_command(&statement(text))
                .err()
                .map(|error| error.code)
        };
        assert_eq!(
            error(r#"{"operation":"find","collection":"users","filter":{"age":{"$gt":1}}}"#),
            None
        );
        assert_eq!(
            error(
                r#"{"operation":"find","collection":"users","filter":{"_id":{"$oid":"0123456789abcdef01234567"}}}"#
            ),
            None
        );
        assert_eq!(
            error(r#"{"operation":"aggregate","collection":"u","pipeline":[{"$out":"x"}]}"#),
            Some(Code::PermissionDenied)
        );
        assert_eq!(
            error(
                r#"{"operation":"aggregate","collection":"u","pipeline":[{"$match":{"$where":"1"}}]}"#
            ),
            Some(Code::PermissionDenied)
        );
        assert_eq!(
            error(r#"{"operation":"find","collection":"u","filter":{"f":{"$code":"x"}}}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            error(r#"{"operation":"find","collection":"system.users"}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            error(r#"{"operation":"find","collection":"u","options":{"batchSize":1}}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            error(r#"{"operation":"find","collection":"u","options":{"limit":-1}}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            error(
                r#"{"operation":"updateOne","collection":"u","filter":{},"update":{"name":"x"}}"#
            ),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            error(
                r#"{"operation":"updateOne","collection":"u","filter":{},"update":[{"$lookup":{}}]}"#
            ),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            error(r#"{"operation":"dropIndex","collection":"u","index":"_id_"}"#),
            Some(Code::InvalidCommand)
        );
        assert_eq!(
            error(r#"{"operation":"drop","collection":"u"}"#),
            Some(Code::InvalidCommand)
        );
    }
}
