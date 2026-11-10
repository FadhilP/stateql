use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::error::{Error, Result};

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct Response {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub handle: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub statement_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub affected_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows: Option<Vec<Vec<Value>>>,
    #[serde(flatten)]
    pub data: Map<String, Value>,
}

impl Response {
    pub fn with(mut self, key: &str, value: impl Serialize) -> Self {
        let value = json!(value);
        let omitted = matches!(value, Value::Null | Value::Bool(false));
        if !omitted {
            self.data.insert(key.to_string(), value);
        }
        self
    }
}

pub fn json_line(result: &Result<Response>) -> String {
    let value = match result {
        Ok(response) => {
            let mut object = Map::new();
            object.insert("ok".into(), Value::Bool(true));
            if let Value::Object(fields) = json!(response) {
                object.extend(fields);
            }
            Value::Object(object)
        }
        Err(error) => json!({"ok": false, "error": error}),
    };
    value.to_string()
}

pub fn text(result: &Result<Response>) -> String {
    let response = match result {
        Ok(response) => response,
        Err(error) => return error_text(error),
    };
    let mut out = String::new();
    if let (Some(columns), Some(rows)) = (&response.columns, &response.rows) {
        out.push_str(&table(columns, rows));
    }
    let mut fields = json!(response);
    if let Value::Object(object) = &mut fields {
        object.remove("columns");
        object.remove("rows");
        object.remove("types");
        for (key, value) in object.iter() {
            let shown = match value {
                Value::String(text) => text.clone(),
                other => other.to_string(),
            };
            out.push_str(&format!("{key}: {shown}\n"));
        }
    }
    out
}

fn error_text(error: &Error) -> String {
    let code = json!(error.code);
    let mut out = format!(
        "error {}: {}\n",
        code.as_str().unwrap_or_default(),
        error.message
    );
    if let Some(hint) = &error.hint {
        out.push_str(&format!("hint: {hint}\n"));
    }
    if let Some(notice) = &error.notice {
        out.push_str(&format!("notice: {notice}\n"));
    }
    out
}

fn table(columns: &[String], rows: &[Vec<Value>]) -> String {
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|value| match value {
                    Value::String(text) => text.replace(['\n', '\t'], " "),
                    Value::Null => "NULL".to_string(),
                    other => other.to_string(),
                })
                .collect()
        })
        .collect();
    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let widest = cells
                .iter()
                .map(|row| row[index].chars().count())
                .max()
                .unwrap_or(0);
            widest.max(name.chars().count())
        })
        .collect();
    let line = |values: &[String]| {
        let padded: Vec<String> = values
            .iter()
            .zip(&widths)
            .map(|(value, width)| format!("{value:<width$}"))
            .collect();
        format!("{}\n", padded.join("  ").trim_end())
    };
    let mut out = line(columns);
    for row in &cells {
        out.push_str(&line(row));
    }
    out
}
