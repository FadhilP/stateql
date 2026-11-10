use std::ops::ControlFlow;

use sqlparser::ast::{OnConflictAction, OnInsert, SqliteOnConflict, Statement, visit_statements};
use sqlparser::dialect::{Dialect as SqlDialect, MySqlDialect, PostgreSqlDialect, SQLiteDialect};
use sqlparser::parser::Parser;

use crate::driver::WriteKind;
use crate::error::{Code, Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
    Mysql,
}

const READ_KEYWORDS: [&str; 8] = [
    "SELECT", "WITH", "VALUES", "TABLE", "EXPLAIN", "SHOW", "DESCRIBE", "DESC",
];

pub fn check_read(dialect: Dialect, sql: &str) -> Result<()> {
    let keyword = first_keyword(dialect, sql).to_ascii_uppercase();
    let sqlite_pragma = dialect == Dialect::Sqlite && keyword == "PRAGMA";
    if !READ_KEYWORDS.contains(&keyword.as_str()) && !sqlite_pragma {
        return Err(
            Error::new(Code::ReadOnly, "query runs only read statements.").with_hint("use exec"),
        );
    }
    if dialect == Dialect::Mysql {
        check_mysql_tokens(sql)?;
    }
    Ok(())
}

// Checked on the raw text, so no string or comment rule of MySQL can hide them.
fn check_mysql_tokens(sql: &str) -> Result<()> {
    let upper = sql.to_ascii_uppercase();
    for token in ["/*!", "OUTFILE", "DUMPFILE"] {
        if upper.contains(token) {
            return Err(Error::new(
                Code::ReadOnly,
                format!("query does not accept {token} on MySQL."),
            )
            .with_hint("remove executable comments and INTO OUTFILE or INTO DUMPFILE"));
        }
    }
    Ok(())
}

fn first_keyword(dialect: Dialect, sql: &str) -> &str {
    let mut rest = sql;
    loop {
        rest = rest.trim_start_matches(|c: char| c.is_whitespace() || c == '(');
        if let Some(after) = rest.strip_prefix("--") {
            let mysql_needs_space =
                dialect == Dialect::Mysql && !after.starts_with(|c: char| c.is_whitespace());
            if mysql_needs_space {
                return "";
            }
            rest = after.split_once('\n').map_or("", |(_, tail)| tail);
        } else if dialect == Dialect::Mysql && rest.starts_with('#') {
            rest = rest.split_once('\n').map_or("", |(_, tail)| tail);
        } else if rest.starts_with("/*") {
            rest = skip_block_comment(rest, dialect == Dialect::Postgres);
        } else {
            break;
        }
    }
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(rest.len());
    &rest[..end]
}

fn skip_block_comment(text: &str, nested: bool) -> &str {
    let mut depth = 0;
    let mut index = 0;
    let bytes = text.as_bytes();
    while index + 1 < bytes.len() {
        match (bytes[index], bytes[index + 1]) {
            (b'/', b'*') if depth == 0 || nested => {
                depth += 1;
                index += 2;
            }
            (b'*', b'/') => {
                depth -= 1;
                index += 2;
                if depth == 0 {
                    return &text[index..];
                }
            }
            _ => index += 1,
        }
    }
    ""
}

enum Kind {
    Query,
    Write(WriteKind),
    Other(String),
}

fn parse(dialect: Dialect, sql: &str) -> Option<Vec<Statement>> {
    let parser_dialect: Box<dyn SqlDialect> = match dialect {
        Dialect::Sqlite => Box::new(SQLiteDialect {}),
        Dialect::Postgres => Box::new(PostgreSqlDialect {}),
        Dialect::Mysql => Box::new(MySqlDialect {}),
    };
    Parser::parse_sql(parser_dialect.as_ref(), sql).ok()
}

pub fn may_write(dialect: Dialect, sql: &str) -> bool {
    let Some(statements) = parse(dialect, sql) else {
        return true;
    };
    let found = visit_statements(&statements, |nested| match classify_one(nested) {
        Kind::Write(_) => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    });
    found.is_break()
}

pub fn classify(dialect: Dialect, sql: &str) -> Result<WriteKind> {
    let Some(statements) = parse(dialect, sql) else {
        return Ok(write("unparsed", true, true));
    };
    let [statement] = statements.as_slice() else {
        return Err(Error::new(
            Code::InvalidSql,
            "exec runs exactly one statement.",
        ));
    };
    let mut kinds = Vec::new();
    let _ = visit_statements(statement, |nested| {
        kinds.push(classify_one(nested));
        ControlFlow::<()>::Continue(())
    });
    let mut combined: Option<WriteKind> = None;
    for kind in kinds {
        match kind {
            Kind::Query => {}
            Kind::Other(keyword) => {
                return Err(Error::new(Code::InvalidSql, format!("exec does not run {keyword} statements."))
                    .with_hint("exec runs INSERT, REPLACE, UPDATE, DELETE, MERGE, CREATE, ALTER, DROP, and TRUNCATE"));
            }
            Kind::Write(found) => {
                let merged = match combined.take() {
                    None => found,
                    Some(first) => WriteKind {
                        statement_type: first.statement_type,
                        unbounded: first.unbounded || found.unbounded,
                        destructive: first.destructive || found.destructive,
                    },
                };
                combined = Some(merged);
            }
        }
    }
    combined.ok_or_else(|| {
        Error::new(Code::InvalidSql, "exec runs only write statements.").with_hint("use query")
    })
}

fn write(statement_type: &str, unbounded: bool, destructive: bool) -> WriteKind {
    WriteKind {
        statement_type: statement_type.to_string(),
        unbounded,
        destructive,
    }
}

fn classify_one(statement: &Statement) -> Kind {
    match statement {
        Statement::Query(_) => Kind::Query,
        Statement::Insert(insert) => {
            let replace = insert.replace_into || insert.or == Some(SqliteOnConflict::Replace);
            let upsert_update = match &insert.on {
                Some(OnInsert::DuplicateKeyUpdate(_)) => true,
                Some(OnInsert::OnConflict(conflict)) => {
                    matches!(conflict.action, OnConflictAction::DoUpdate(_))
                }
                _ => false,
            };
            Kind::Write(write(
                if replace { "replace" } else { "insert" },
                false,
                replace || upsert_update,
            ))
        }
        Statement::Update(update) => {
            let replace = update.or == Some(SqliteOnConflict::Replace);
            Kind::Write(write("update", update.selection.is_none(), replace))
        }
        Statement::Delete(delete) => Kind::Write(write("delete", delete.selection.is_none(), true)),
        Statement::Merge(_) => Kind::Write(write("merge", false, true)),
        Statement::Truncate(_) => Kind::Write(write("truncate", true, true)),
        other => {
            // sqlparser has dozens of CREATE, ALTER, and DROP variants; their SQL text names the kind.
            let text = other.to_string().to_ascii_uppercase();
            let keyword = text
                .split_whitespace()
                .next()
                .unwrap_or_default()
                .to_string();
            match keyword.as_str() {
                "CREATE" => Kind::Write(write(
                    "create",
                    false,
                    text.starts_with("CREATE OR REPLACE"),
                )),
                "ALTER" => Kind::Write(write("alter", false, true)),
                "DROP" => Kind::Write(write("drop", false, true)),
                _ => Kind::Other(keyword),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_gate_table() {
        use Dialect::*;
        let cases: &[(Dialect, &str, &str, bool, bool)] = &[
            (
                Sqlite,
                "INSERT INTO t (a) VALUES (1)",
                "insert",
                false,
                false,
            ),
            (
                Sqlite,
                "INSERT OR REPLACE INTO t (a) VALUES (1)",
                "replace",
                false,
                true,
            ),
            (
                Sqlite,
                "REPLACE INTO t (a) VALUES (1)",
                "replace",
                false,
                true,
            ),
            (
                Sqlite,
                "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING",
                "insert",
                false,
                false,
            ),
            (
                Sqlite,
                "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2",
                "insert",
                false,
                true,
            ),
            (
                Sqlite,
                "UPDATE t SET a = 1 WHERE id = 2",
                "update",
                false,
                false,
            ),
            (Sqlite, "UPDATE t SET a = 1", "update", true, false),
            (
                Sqlite,
                "UPDATE OR REPLACE t SET a = 1 WHERE id = 2",
                "update",
                false,
                true,
            ),
            (Sqlite, "DELETE FROM t WHERE id = 1", "delete", false, true),
            (Sqlite, "DELETE FROM t", "delete", true, true),
            (Sqlite, "CREATE TABLE t (a INTEGER)", "create", false, false),
            (Sqlite, "CREATE INDEX i ON t (a)", "create", false, false),
            (Sqlite, "DROP TABLE t", "drop", false, true),
            (
                Sqlite,
                "ALTER TABLE t ADD COLUMN b TEXT",
                "alter",
                false,
                true,
            ),
            (
                Sqlite,
                "CREATE TRIGGER g AFTER INSERT ON t BEGIN DELETE FROM u; END",
                "create",
                true,
                true,
            ),
            (Postgres, "TRUNCATE t", "truncate", true, true),
            (
                Postgres,
                "WITH gone AS (DELETE FROM t WHERE a = 1 RETURNING *) SELECT * FROM gone",
                "delete",
                false,
                true,
            ),
            (
                Postgres,
                "WITH gone AS (DELETE FROM t RETURNING *) INSERT INTO u SELECT * FROM gone",
                "delete",
                true,
                true,
            ),
            (
                Postgres,
                "INSERT INTO t SELECT * FROM u",
                "insert",
                false,
                false,
            ),
            (
                Postgres,
                "UPDATE t SET a = u.a FROM u WHERE t.id = u.id",
                "update",
                false,
                false,
            ),
            (
                Postgres,
                "MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN DELETE",
                "merge",
                false,
                true,
            ),
            (
                Postgres,
                "CREATE OR REPLACE VIEW v AS SELECT 1",
                "create",
                false,
                true,
            ),
            (
                Postgres,
                "CREATE INDEX CONCURRENTLY i ON t (a)",
                "create",
                false,
                false,
            ),
            (
                Mysql,
                "INSERT INTO t (a) VALUES (1) ON DUPLICATE KEY UPDATE a = 2",
                "insert",
                false,
                true,
            ),
            (
                Mysql,
                "REPLACE INTO t (a) VALUES (1)",
                "replace",
                false,
                true,
            ),
            (Mysql, "DELETE FROM t LIMIT 1", "delete", true, true),
            (
                Postgres,
                "CREATE TABLE t (a int) PARTITION BY MAGIC (a) WITH NONSENSE",
                "unparsed",
                true,
                true,
            ),
        ];
        for (dialect, sql, statement_type, unbounded, destructive) in cases {
            let kind = classify(*dialect, sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(
                (
                    kind.statement_type.as_str(),
                    kind.unbounded,
                    kind.destructive
                ),
                (*statement_type, *unbounded, *destructive),
                "{sql}"
            );
        }
    }

    #[test]
    fn exec_rejects_reads_other_kinds_and_several_statements() {
        for sql in [
            "SELECT 1",
            "WITH a AS (SELECT 1) SELECT * FROM a",
            "VALUES (1)",
        ] {
            let error = classify(Dialect::Postgres, sql).unwrap_err();
            assert_eq!(
                (error.code, error.hint.as_deref()),
                (Code::InvalidSql, Some("use query")),
                "{sql}"
            );
        }
        for sql in [
            "GRANT SELECT ON t TO u",
            "SET x = 1",
            "VACUUM",
            "EXPLAIN DELETE FROM t",
            "BEGIN",
            "COPY t FROM STDIN",
        ] {
            assert_eq!(
                classify(Dialect::Postgres, sql).unwrap_err().code,
                Code::InvalidSql,
                "{sql}"
            );
        }
        assert_eq!(
            classify(Dialect::Sqlite, "DELETE FROM t; DELETE FROM u")
                .unwrap_err()
                .code,
            Code::InvalidSql
        );
    }

    #[test]
    fn read_keyword_skips_comments_per_dialect() {
        let allowed = [
            (Dialect::Sqlite, "/* note */ (SELECT 1)"),
            (Dialect::Sqlite, "-- note\nPRAGMA table_info(t)"),
            (Dialect::Postgres, "/* a /* nested */ still */ SELECT 1"),
            (Dialect::Mysql, "# note\nSHOW TABLES"),
            (Dialect::Mysql, "-- note\nDESC t"),
        ];
        for (dialect, sql) in allowed {
            assert!(check_read(dialect, sql).is_ok(), "{sql}");
        }
        let blocked = [
            (
                Dialect::Postgres,
                "/* a /* nested */ SELECT */ COPY (SELECT 1) TO PROGRAM 'x'",
            ),
            (Dialect::Postgres, "PRAGMA x"),
            (Dialect::Mysql, "--1\nSELECT 1"),
            (Dialect::Mysql, "SELECT 1 INTO OUTFILE '/tmp/x'"),
            (Dialect::Mysql, "SELECT 1 INTO/**/DUMPFILE '/tmp/x'"),
            (Dialect::Mysql, "SELECT /*!50000 1 */"),
            (Dialect::Sqlite, "SELECTX 1"),
            (Dialect::Sqlite, "KILL 1"),
            (Dialect::Mysql, "SET GLOBAL x = 1"),
        ];
        for (dialect, sql) in blocked {
            assert_eq!(
                check_read(dialect, sql).unwrap_err().code,
                Code::ReadOnly,
                "{sql}"
            );
        }
    }
}
