mod common;

use std::sync::atomic::{AtomicUsize, Ordering};

use common::*;
use mongodb::bson::{Document, doc};
use serde_json::{Value, json};
use stateql::{Code, PipeMode};

struct Mongo {
    db: mongodb::sync::Database,
    prefix: String,
}

impl Mongo {
    fn name(&self, collection: &str) -> String {
        format!("{}{collection}", self.prefix)
    }

    fn count(&self, collection: &str, filter: Document) -> u64 {
        self.db
            .collection::<Document>(&self.name(collection))
            .count_documents(filter)
            .run()
            .unwrap()
    }
}

impl Drop for Mongo {
    fn drop(&mut self) {
        for name in self.db.list_collection_names().run().unwrap_or_default() {
            if name.starts_with(&self.prefix) {
                let _ = self.db.collection::<Document>(&name).drop().run();
            }
        }
    }
}

fn mongo() -> Option<Mongo> {
    let url = env_url("STQL_TEST_MONGODB_URL")?;
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let prefix = format!(
        "t{}_{}_",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    );
    let client = mongodb::sync::Client::with_uri_str(&url).expect("mongo client");
    let db = client.default_database().expect("the URL names a database");
    let mongo = Mongo { db, prefix };
    mongo
        .db
        .collection::<Document>(&mongo.name("people"))
        .insert_many([
            doc! {"_id": 1, "name": "Ada", "age": 36, "tags": ["math"]},
            doc! {"_id": 2, "name": "Grace", "age": 85, "meta": {"navy": true}},
            doc! {"_id": 3, "name": "Linus", "age": null},
        ])
        .run()
        .expect("fixture");
    Some(mongo)
}

fn connect_env(read_write: bool) -> String {
    json!({"command": "connect", "env": "STQL_TEST_MONGODB_URL", "read_write": read_write})
        .to_string()
}

fn command(value: Value) -> String {
    value.to_string()
}

#[test]
fn reads_flatten_documents_into_typed_columns() {
    let Some(mongo) = mongo() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(true));
    let values = mongo.name("values");
    let insert = json!({"operation": "insertMany", "collection": values, "documents": [
        {"_id": {"$oid": "0123456789abcdef01234567"}, "when": {"$date": "2024-02-29T01:02:03.004Z"},
         "big": {"$numberLong": "9007199254740993"}, "price": {"$numberDecimal": "12.50"},
         "raw": {"$binary": {"base64": "AQI=", "subType": "00"}}, "later": null, "flag": true},
        {"_id": {"$oid": "0123456789abcdef01234568"}, "later": {"x": [1, 2]}, "flag": "yes", "extra": "e"}
    ]});
    ok(&mut session, &exec_json(&insert.to_string(), json!({})));
    let response = ok(
        &mut session,
        &query_json(&command(
            json!({"operation": "find", "collection": values, "options": {"sort": [["_id", 1]]}}),
        )),
    );
    assert_eq!(
        response.columns.unwrap(),
        [
            "_id", "when", "big", "price", "raw", "later", "flag", "extra"
        ]
    );
    assert_eq!(
        response.types.unwrap(),
        [
            "objectId",
            "date",
            "number",
            "decimal128",
            "binary",
            "object",
            "mixed",
            "string"
        ]
    );
    let rows = response.rows.unwrap();
    assert_eq!(
        rows[0],
        vec![
            json!("0123456789abcdef01234567"),
            json!("2024-02-29T01:02:03.004Z"),
            json!(9_007_199_254_740_993_i64),
            json!("12.50"),
            json!({"type": "bytes", "length": 2, "base64": "AQI="}),
            Value::Null,
            json!(1),
            Value::Null,
        ]
    );
    assert_eq!(rows[1][5], json!({"x": [1, 2]}));
    assert_eq!(rows[1][6], json!("yes"));

    let people = mongo.name("people");
    let found = ok(
        &mut session,
        &query_json(&command(
            json!({"operation": "find", "collection": people, "filter": {"age": {"$gte": 30}}, "options": {"sort": {"_id": 1}, "projection": {"name": 1}}}),
        )),
    );
    assert_eq!(
        found.rows,
        Some(vec![
            vec![json!(1), json!("Ada")],
            vec![json!(2), json!("Grace")]
        ])
    );
    let filtered = ok(
        &mut session,
        &json!({"command": "filter", "handle": found.handle, "predicate": "name = 'Grace'"})
            .to_string(),
    );
    assert_eq!(filtered.total, Some(1));
    let counted = ok(
        &mut session,
        &query_json(&command(
            json!({"operation": "countDocuments", "collection": people, "filter": {"age": {"$gt": 40}}}),
        )),
    );
    assert_eq!(counted.rows, Some(vec![vec![json!(1)]]));
    let names = ok(
        &mut session,
        &query_json(&command(
            json!({"operation": "aggregate", "collection": people, "pipeline": [{"$sort": {"_id": -1}}, {"$project": {"_id": 0, "name": 1}}]}),
        )),
    );
    assert_eq!(names.columns.unwrap(), ["name"]);
    let empty = ok(
        &mut session,
        &query_json(&command(
            json!({"operation": "find", "collection": people, "filter": {"age": 1000}}),
        )),
    );
    assert_eq!(empty.total, Some(0));
}

#[test]
fn guards_block_server_side_writes_and_javascript() {
    let Some(mongo) = mongo() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut reader = engine.session("ro").unwrap();
    ok(&mut reader, &connect_env(false));
    let people = mongo.name("people");
    let out = mongo.name("out");
    let corpus = [
        (
            json!({"operation": "aggregate", "collection": people, "pipeline": [{"$out": out}]}),
            Code::PermissionDenied,
        ),
        (
            json!({"operation": "aggregate", "collection": people, "pipeline": [{"$merge": {"into": out}}]}),
            Code::PermissionDenied,
        ),
        (
            json!({"operation": "find", "collection": people, "filter": {"$where": "true"}}),
            Code::PermissionDenied,
        ),
        (
            json!({"operation": "aggregate", "collection": people, "pipeline": [{"$match": {"$expr": {"$function": {"body": "1", "args": [], "lang": "js"}}}}]}),
            Code::PermissionDenied,
        ),
        (
            json!({"operation": "find", "collection": people, "filter": {"name": {"$code": "x"}}}),
            Code::InvalidCommand,
        ),
        (
            json!({"operation": "deleteMany", "collection": people, "filter": {}}),
            Code::ReadOnly,
        ),
        (
            json!({"operation": "drop", "collection": people}),
            Code::InvalidCommand,
        ),
        (
            json!({"operation": "find", "collection": "system.users"}),
            Code::InvalidCommand,
        ),
        (
            json!({"operation": "find", "collection": people, "options": {"noCursorTimeout": true}}),
            Code::InvalidCommand,
        ),
    ];
    for (statement, code) in corpus {
        expect_code(&mut reader, &query_json(&statement.to_string()), code);
    }
    assert_eq!(mongo.count("people", doc! {}), 3);
    assert_eq!(mongo.count("out", doc! {}), 0);
    expect_code(
        &mut reader,
        &exec_json(
            &command(json!({"operation": "insertOne", "collection": people, "document": {"x": 1}})),
            json!({}),
        ),
        Code::ReadOnly,
    );

    let mut writer = engine.session("rw").unwrap();
    ok(&mut writer, &connect_env(true));
    expect_code(
        &mut writer,
        &exec_json(
            &command(json!({"operation": "deleteMany", "collection": people, "filter": {}})),
            json!({"allow_destructive": true}),
        ),
        Code::UnboundedMutation,
    );
    expect_code(
        &mut writer,
        &exec_json(
            &command(json!({"operation": "deleteOne", "collection": people, "filter": {"_id": 1}})),
            json!({}),
        ),
        Code::DestructiveOperation,
    );
    expect_code(
        &mut writer,
        &exec_json(
            &command(json!({"operation": "find", "collection": people})),
            json!({}),
        ),
        Code::InvalidSql,
    );
    let input = format!(
        "{}\n",
        exec_json(
            &command(json!({"operation": "insertOne", "collection": people, "document": {"x": 1}})),
            json!({})
        )
    );
    let mut output = Vec::new();
    let exit = writer
        .pipe(input.as_bytes(), &mut output, PipeMode::Atomic(None))
        .unwrap();
    assert_eq!(exit, Code::Unsupported.exit_code());
    assert_eq!(mongo.count("people", doc! {}), 3);
}

#[test]
fn expect_rows_counts_the_filter_before_the_write() {
    let Some(mongo) = mongo() else { return };
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(true));
    let people = mongo.name("people");
    let update = command(
        json!({"operation": "updateMany", "collection": people, "filter": {"age": {"$ne": null}}, "update": {"$set": {"seen": true}}}),
    );
    let error = expect_code(
        &mut session,
        &exec_json(&update, json!({"expect_rows": 3})),
        Code::RowCountMismatch,
    );
    assert_eq!(error.actual_rows, Some(2));
    assert_eq!(mongo.count("people", doc! {"seen": true}), 0);
    let written = ok(&mut session, &exec_json(&update, json!({"expect_rows": 2})));
    assert_eq!(written.affected_rows, Some(2));
    assert_eq!(mongo.count("people", doc! {"seen": true}), 2);
    expect_code(
        &mut session,
        &exec_json(
            &command(json!({"operation": "insertOne", "collection": people, "document": {"x": 1}})),
            json!({"expect_rows": 1}),
        ),
        Code::Unsupported,
    );
    let error = expect_code(
        &mut session,
        &exec_json(
            &command(
                json!({"operation": "insertOne", "collection": people, "document": {"_id": 1}}),
            ),
            json!({}),
        ),
        Code::QueryFailed,
    );
    assert_eq!(
        error.executed,
        stateql::Executed::No,
        "a duplicate key on one document is a definite failure"
    );
}

#[test]
fn catalog_lists_collections_and_describes_them() {
    let Some(mongo) = mongo() else { return };
    mongo
        .db
        .collection::<Document>(&mongo.name("people"))
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! {"name": 1})
                .build(),
        )
        .run()
        .unwrap();
    let home = Home::new();
    let engine = home.engine();
    let mut session = engine.session("t").unwrap();
    ok(&mut session, &connect_env(false));
    let objects = ok(
        &mut session,
        &json!({"command": "objects", "search": mongo.prefix}).to_string(),
    );
    assert_eq!(
        objects.rows,
        Some(vec![vec![json!("collection"), json!(mongo.name("people"))]])
    );
    let described = ok(
        &mut session,
        &json!({"command": "describe", "name": mongo.name("people")}).to_string(),
    );
    let object = &described.data["object"];
    assert_eq!(object["kind"], json!("collection"));
    assert_eq!(object["sampled_documents"], json!(3));
    assert_eq!(
        object["fields"],
        json!([{"name": "_id", "type": "number"}, {"name": "name", "type": "string"}, {"name": "age", "type": "number"},
               {"name": "tags", "type": "array"}, {"name": "meta", "type": "object"}])
    );
    let indexes: Vec<&str> = object["indexes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|index| index["name"].as_str().unwrap())
        .collect();
    assert_eq!(indexes, ["_id_", "name_1"]);
    expect_code(
        &mut session,
        r#"{"command":"describe","name":"missing_collection"}"#,
        Code::ResultNotFound,
    );
}
