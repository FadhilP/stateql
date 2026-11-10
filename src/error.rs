use std::fmt;

use serde::{Serialize, Serializer};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Code {
    InvalidCommand,
    InvalidSql,
    ConnectionFailed,
    NotConnected,
    CredentialMissing,
    CredentialMismatch,
    QueryFailed,
    DeadlineExceeded,
    ReadOnly,
    UnboundedMutation,
    DestructiveOperation,
    OutcomeUnknown,
    IdempotencyConflict,
    RowCountMismatch,
    ResultNotFound,
    PermissionDenied,
    Unsupported,
    ResultTooLarge,
    InternalError,
}

impl Code {
    pub fn exit_code(self) -> i32 {
        use Code::*;
        match self {
            InvalidCommand | InvalidSql => 2,
            ConnectionFailed | NotConnected | CredentialMissing | CredentialMismatch => 3,
            QueryFailed | DeadlineExceeded => 4,
            ReadOnly | UnboundedMutation | DestructiveOperation => 5,
            OutcomeUnknown | IdempotencyConflict => 6,
            RowCountMismatch | ResultNotFound => 7,
            PermissionDenied => 8,
            Unsupported => 9,
            ResultTooLarge | InternalError => 1,
        }
    }

    fn retryable(self) -> bool {
        matches!(self, Code::ConnectionFailed | Code::DeadlineExceeded)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Executed {
    No,
    Yes,
    Unknown,
}

impl Serialize for Executed {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Executed::No => serializer.serialize_bool(false),
            Executed::Yes => serializer.serialize_bool(true),
            Executed::Unknown => serializer.serialize_str("unknown"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Error {
    pub code: Code,
    pub message: String,
    pub retryable: bool,
    pub executed: Executed,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_operation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_rows: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notice: Option<String>,
}

impl Error {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Error {
            code,
            message: message.into(),
            retryable: code.retryable(),
            executed: Executed::No,
            hint: None,
            previous_operation: None,
            actual_rows: None,
            notice: None,
        }
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_executed(mut self, executed: Executed) -> Self {
        self.executed = executed;
        self
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Error::new(Code::InvalidCommand, message)
    }

    pub fn internal(message: impl fmt::Display) -> Self {
        Error::new(Code::InternalError, message.to_string())
    }

    pub fn query(message: impl fmt::Display) -> Self {
        Error::new(Code::QueryFailed, message.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        Error::internal(format!("state store: {error}"))
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::internal(error)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
