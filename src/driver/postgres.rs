use std::fmt::Write as _;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Instant;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::BytesMut;
use postgres::config::SslMode;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::Format;
use postgres::types::{FromSql, IsNull, Kind, ToSql, Type, to_sql_checked};
use postgres::{Client, Config, NoTls};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{CertificateError, DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde_json::{Value, json};

use super::{
    Cell, Column, Driver, ObjectQuery, Statement, WriteKind, format_date, format_time,
    format_timestamp, remaining,
};
use crate::command::Isolation;
use crate::error::{Code, Error, Executed, Result};
use crate::guard::{self, Dialect};
use crate::results::RowSink;

const PG_EPOCH_DAYS: i64 = 10_957;
const PG_EPOCH_MICROS: i64 = PG_EPOCH_DAYS * 86_400_000_000;
const TEXT_TYPES: [&str; 11] = [
    "text", "varchar", "bpchar", "name", "unknown", "xml", "citext", "json", "char", "jsonb",
    "void",
];
const OTHER_TYPES: [&str; 21] = [
    "bool",
    "int2",
    "int4",
    "int8",
    "oid",
    "float4",
    "float8",
    "numeric",
    "bytea",
    "uuid",
    "date",
    "time",
    "timestamp",
    "interval",
    "xid",
    "cid",
    "pg_lsn",
    "inet",
    "cidr",
    "timetz",
    "macaddr",
];

const RELEASE_READ: &str = "ROLLBACK TO SAVEPOINT stql_read; RELEASE SAVEPOINT stql_read";

// An open read ends with the next batch, or the disconnect at exit ends it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transaction {
    Idle,
    OpenRead,
    Atomic,
    // The savepoint of the last read inside pipe --atomic.
    AtomicRead,
}

pub struct PostgresDriver {
    client: Client,
    read_only: bool,
    transaction: Transaction,
}

impl PostgresDriver {
    pub fn open(url: &str, read_only: bool, deadline: Instant) -> Result<PostgresDriver> {
        let (url, tls) = split_ssl_mode(url)?;
        let tls = tls_config(&tls)?;
        let mut config: Config = url.parse().map_err(super::connection_failed)?;
        config.connect_timeout(remaining(deadline)?);
        let client = match tls {
            None => config.ssl_mode(SslMode::Disable).connect(NoTls),
            Some(tls) => config
                .ssl_mode(SslMode::Require)
                .connect(tokio_postgres_rustls::MakeRustlsConnect::new(tls)),
        };
        // The postgres error text hides the cause, for example an unknown certificate issuer.
        let client = client.map_err(|error| match std::error::Error::source(&error) {
            Some(cause) => super::connection_failed(format!("{error}: {cause}")),
            None => super::connection_failed(error),
        })?;
        Ok(PostgresDriver {
            client,
            read_only,
            transaction: Transaction::Idle,
        })
    }

    fn batch(&mut self, sql: &str) -> std::result::Result<(), postgres::Error> {
        let (end, after) = match self.transaction {
            Transaction::Idle | Transaction::Atomic => return self.client.batch_execute(sql),
            Transaction::OpenRead => ("ROLLBACK", Transaction::Idle),
            Transaction::AtomicRead => (RELEASE_READ, Transaction::Atomic),
        };
        self.client.batch_execute(&format!("{end}; {sql}"))?;
        self.transaction = after;
        Ok(())
    }

    fn read_only_scope<T>(
        &mut self,
        deadline: Instant,
        work: impl FnOnce(&mut Client) -> Result<T>,
    ) -> Result<T> {
        let timeout = remaining(deadline)?.as_millis();
        if let Transaction::Atomic | Transaction::AtomicRead = self.transaction {
            // Inside pipe --atomic a savepoint makes the read read-only; its release undoes the setting.
            self.batch(&format!(
                "SAVEPOINT stql_read; SET LOCAL transaction_read_only = on; SET LOCAL statement_timeout = {timeout}"
            ))
            .map_err(read_error)?;
            self.transaction = Transaction::AtomicRead;
            return work(&mut self.client);
        }
        let opened = self.batch(&format!(
            "BEGIN READ ONLY; SET LOCAL statement_timeout = {timeout}"
        ));
        self.transaction = Transaction::OpenRead;
        opened.map_err(read_error)?;
        work(&mut self.client)
    }

    fn text_rows(
        &mut self,
        deadline: Instant,
        sql: &str,
        params: &[&str],
    ) -> Result<Vec<Vec<Option<String>>>> {
        self.read_only_scope(deadline, |client| {
            let params: Vec<&(dyn ToSql + Sync)> = params
                .iter()
                .map(|param| param as &(dyn ToSql + Sync))
                .collect();
            let rows = client.query(sql, &params).map_err(read_error)?;
            Ok(rows
                .iter()
                .map(|row| (0..row.len()).map(|index| row.get(index)).collect())
                .collect())
        })
    }
}

// The libpq meanings. A root CA path is Some only when sslrootcert is set; else the webpki roots apply.
#[derive(Debug, PartialEq)]
enum Tls {
    Off,
    Unverified,
    VerifyCa(Option<String>),
    VerifyFull(Option<String>),
}

fn split_ssl_mode(target: &str) -> Result<(String, Tls)> {
    let (url, mode) = super::take_query_param(target, "sslmode")?;
    let (url, root_cert) = super::take_query_param(&url, "sslrootcert")?;
    let tls = match mode.as_deref() {
        None | Some("disable") => Tls::Off,
        // libpq: require with a root CA file acts as verify-ca.
        Some("require") if root_cert.is_some() => Tls::VerifyCa(root_cert),
        Some("require") => Tls::Unverified,
        Some("verify-ca") => Tls::VerifyCa(root_cert),
        Some("verify-full") => Tls::VerifyFull(root_cert),
        Some(fallback @ ("allow" | "prefer")) => {
            return Err(Error::invalid(format!(
                "sslmode \"{fallback}\" can fall back to plain TCP, so stql does not support it."
            ))
            .with_hint("use sslmode=disable or sslmode=require"));
        }
        Some(other) => {
            return Err(Error::invalid(format!(
                "sslmode \"{other}\" is not supported."
            )));
        }
    };
    Ok((url, tls))
}

fn tls_config(tls: &Tls) -> Result<Option<rustls::ClientConfig>> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("ring supports the default TLS versions");
    let config = match tls {
        Tls::Off => return Ok(None),
        Tls::Unverified => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(LibpqVerifier {
                chain: None,
                provider,
            }))
            .with_no_client_auth(),
        Tls::VerifyCa(root_cert) => {
            let chain = WebPkiServerVerifier::builder_with_provider(
                Arc::new(root_store(root_cert.as_deref())?),
                provider.clone(),
            )
            .build()
            .map_err(super::connection_failed)?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(LibpqVerifier {
                    chain: Some(chain),
                    provider,
                }))
                .with_no_client_auth()
        }
        Tls::VerifyFull(root_cert) => builder
            .with_root_certificates(root_store(root_cert.as_deref())?)
            .with_no_client_auth(),
    };
    Ok(Some(config))
}

fn root_store(root_cert: Option<&str>) -> Result<RootCertStore> {
    let Some(path) = root_cert else {
        return Ok(RootCertStore::from_iter(
            webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
        ));
    };
    let mut store = RootCertStore::empty();
    for cert in super::read_ca_file(path)? {
        store.add(cert).map_err(super::connection_failed)?;
    }
    Ok(store)
}

// rustls has no switch for these libpq modes: verify-ca checks the chain but not the host
// name, and require checks neither. Only those two sslmode values build this verifier.
#[derive(Debug)]
struct LibpqVerifier {
    chain: Option<Arc<WebPkiServerVerifier>>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for LibpqVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        let Some(chain) = &self.chain else {
            return Ok(ServerCertVerified::assertion());
        };
        match chain.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now) {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[derive(Debug)]
struct TextParam(Option<String>);

// Text format lets the server parse each value for the type it infers, as libpq clients do.
impl ToSql for TextParam {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        match &self.0 {
            None => Ok(IsNull::Yes),
            Some(text) => {
                out.extend_from_slice(text.as_bytes());
                Ok(IsNull::No)
            }
        }
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }

    to_sql_checked!();
}

fn text_params(params: &[Value]) -> Vec<TextParam> {
    params
        .iter()
        .map(|value| match value {
            Value::Null => TextParam(None),
            Value::String(text) => TextParam(Some(text.clone())),
            other => TextParam(Some(other.to_string())),
        })
        .collect()
}

struct Raw<'a>(Option<&'a [u8]>);

impl<'a> FromSql<'a> for Raw<'a> {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(Some(raw)))
    }

    fn from_sql_null(
        _ty: &Type,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Raw(None))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}

fn read_error(error: postgres::Error) -> Error {
    let Some(db) = error.as_db_error() else {
        return Error::query(error);
    };
    let code = db.code().code();
    let message = db.message().to_string();
    let error = match code {
        "25006" => super::read_only_error(),
        "57014" => Error::new(
            Code::DeadlineExceeded,
            "The statement passed its deadline and was cancelled.",
        ),
        "42501" => Error::new(Code::PermissionDenied, message),
        _ if code.starts_with("42") => Error::new(Code::InvalidSql, message),
        _ => Error::query(message),
    };
    match (db.hint(), code) {
        (Some(hint), _) => error.with_hint(hint),
        (None, "42703") => error.with_hint(super::MISSING_COLUMN_HINT),
        (None, "42P01") => error.with_hint(super::MISSING_TABLE_HINT),
        _ => error,
    }
}

fn write_error(error: postgres::Error) -> Error {
    if error.as_db_error().is_some() {
        return read_error(error);
    }
    Error::query(error).with_executed(Executed::Unknown)
}

fn type_label(ty: &Type) -> String {
    match ty.kind() {
        Kind::Array(inner) => format!("{}[]", inner.name()),
        _ => ty.name().to_string(),
    }
}

fn supported(ty: &Type) -> bool {
    match ty.kind() {
        Kind::Enum(_) => true,
        Kind::Domain(inner) => supported(inner),
        Kind::Array(inner) => !matches!(inner.kind(), Kind::Array(_)) && supported(inner),
        _ => {
            TEXT_TYPES.contains(&ty.name())
                || OTHER_TYPES.contains(&ty.name())
                || ty.name() == "timestamptz"
        }
    }
}

fn is_json(ty: &Type) -> bool {
    match ty.kind() {
        Kind::Array(_) => true,
        Kind::Domain(inner) => is_json(inner),
        _ => matches!(ty.name(), "json" | "jsonb"),
    }
}

fn read_columns(statement: &postgres::Statement) -> Result<Vec<Column>> {
    statement
        .columns()
        .iter()
        .map(|column| {
            if !supported(column.type_()) {
                return Err(Error::query(format!(
                    "Column \"{}\" has type {}, which stql cannot store.",
                    column.name(),
                    column.type_()
                ))
                .with_hint("cast the column to text (::text)"));
            }
            Ok(Column {
                name: column.name().to_string(),
                type_name: type_label(column.type_()),
                json: is_json(column.type_()),
            })
        })
        .collect()
}

fn read_into(client: &mut Client, sql: &str, params: &[Value], sink: &mut RowSink) -> Result<()> {
    let statement = client.prepare(sql).map_err(read_error)?;
    check_param_count(&statement, params)?;
    sink.set_columns(read_columns(&statement)?)?;
    let types: Vec<Type> = statement
        .columns()
        .iter()
        .map(|column| column.type_().clone())
        .collect();
    let mut rows = client
        .query_raw(&statement, text_params(params))
        .map_err(read_error)?;
    while let Some(row) = rows.next().map_err(read_error)? {
        let mut cells = Vec::with_capacity(types.len());
        for (index, ty) in types.iter().enumerate() {
            let raw: Raw<'_> = row.try_get(index).map_err(read_error)?;
            cells.push(match raw.0 {
                None => Cell::Null,
                Some(bytes) => decode(ty, bytes)?,
            });
        }
        sink.push(cells)?;
    }
    Ok(())
}

fn check_param_count(statement: &postgres::Statement, params: &[Value]) -> Result<()> {
    let expected = statement.params().len();
    if expected == params.len() {
        return Ok(());
    }
    Err(Error::invalid(format!(
        "The statement takes {expected} parameters, but {} were given.",
        params.len()
    )))
}

fn malformed(ty: &Type) -> Error {
    Error::query(format!("PostgreSQL sent a malformed {} value.", ty.name()))
}

fn be<const N: usize>(ty: &Type, raw: &[u8], at: usize) -> Result<[u8; N]> {
    raw.get(at..at + N)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or_else(|| malformed(ty))
}

fn text(ty: &Type, raw: &[u8]) -> Result<Cell> {
    String::from_utf8(raw.to_vec())
        .map(Cell::Text)
        .map_err(|_| malformed(ty))
}

fn decode(ty: &Type, raw: &[u8]) -> Result<Cell> {
    match ty.kind() {
        Kind::Enum(_) => return text(ty, raw),
        Kind::Domain(inner) => return decode(inner, raw),
        Kind::Array(inner) => return Ok(Cell::Text(array_json(inner, raw)?.to_string())),
        _ => {}
    }
    let cell = match ty.name() {
        "bool" => Cell::Integer(i64::from(be::<1>(ty, raw, 0)?[0] != 0)),
        "int2" => Cell::Integer(i16::from_be_bytes(be(ty, raw, 0)?).into()),
        "int4" => Cell::Integer(i32::from_be_bytes(be(ty, raw, 0)?).into()),
        "oid" => Cell::Integer(u32::from_be_bytes(be(ty, raw, 0)?).into()),
        "int8" => Cell::Integer(i64::from_be_bytes(be(ty, raw, 0)?)),
        "float4" => Cell::Real(f32::from_be_bytes(be(ty, raw, 0)?).into()),
        "float8" => Cell::Real(f64::from_be_bytes(be(ty, raw, 0)?)),
        "numeric" => Cell::Text(numeric_text(ty, raw)?),
        "jsonb" => return text(ty, raw.get(1..).ok_or_else(|| malformed(ty))?),
        "bytea" => Cell::Blob(raw.to_vec()),
        "uuid" => Cell::Text(uuid_text(ty, raw)?),
        "date" => Cell::Text(date_text(i32::from_be_bytes(be(ty, raw, 0)?))),
        "time" => Cell::Text(format_time(i64::from_be_bytes(be(ty, raw, 0)?))),
        "timestamp" => Cell::Text(timestamp_text(i64::from_be_bytes(be(ty, raw, 0)?), "")),
        "timestamptz" => Cell::Text(timestamp_text(i64::from_be_bytes(be(ty, raw, 0)?), "Z")),
        "interval" => Cell::Text(interval_text(ty, raw)?),
        "timetz" => Cell::Text(timetz_text(ty, raw)?),
        "xid" | "cid" => Cell::Text(u32::from_be_bytes(be(ty, raw, 0)?).to_string()),
        "pg_lsn" => Cell::Text(lsn_text(u64::from_be_bytes(be(ty, raw, 0)?))),
        "inet" | "cidr" => Cell::Text(inet_text(ty, raw)?),
        "macaddr" => {
            let bytes: [u8; 6] = be(ty, raw, 0)?;
            let parts: Vec<String> = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
            Cell::Text(parts.join(":"))
        }
        name if TEXT_TYPES.contains(&name) => return text(ty, raw),
        _ => {
            return Err(
                Error::query(format!("Type {} is not supported.", ty.name()))
                    .with_hint("cast the column to text (::text)"),
            );
        }
    };
    Ok(cell)
}

fn date_text(days: i32) -> String {
    match days {
        i32::MAX => "infinity".to_string(),
        i32::MIN => "-infinity".to_string(),
        days => format_date(i64::from(days) + PG_EPOCH_DAYS),
    }
}

fn timestamp_text(micros: i64, suffix: &str) -> String {
    match micros {
        i64::MAX => "infinity".to_string(),
        i64::MIN => "-infinity".to_string(),
        micros => {
            let mut text = format_timestamp(micros + PG_EPOCH_MICROS);
            text.push_str(suffix);
            text
        }
    }
}

// The text of intervalstyle = postgres, the default, as EncodeInterval in PostgreSQL writes it.
fn interval_text(ty: &Type, raw: &[u8]) -> Result<String> {
    let micros = i64::from_be_bytes(be(ty, raw, 0)?);
    let days = i32::from_be_bytes(be(ty, raw, 8)?);
    let months = i32::from_be_bytes(be(ty, raw, 12)?);
    match (months, days, micros) {
        (i32::MAX, i32::MAX, i64::MAX) => return Ok("infinity".to_string()),
        (i32::MIN, i32::MIN, i64::MIN) => return Ok("-infinity".to_string()),
        _ => {}
    }
    let mut out = String::new();
    // PostgreSQL writes "+" before a positive part that follows a negative one.
    let mut after_negative = false;
    let parts = [(months / 12, "year"), (months % 12, "mon"), (days, "day")];
    for (value, unit) in parts {
        if value == 0 {
            continue;
        }
        let space = if out.is_empty() { "" } else { " " };
        let plus = if after_negative && value > 0 { "+" } else { "" };
        let plural = if value == 1 { "" } else { "s" };
        let _ = write!(out, "{space}{plus}{value} {unit}{plural}");
        after_negative = value < 0;
    }
    if out.is_empty() || micros != 0 {
        let space = if out.is_empty() { "" } else { " " };
        let sign = match micros {
            ..0 => "-",
            _ if after_negative => "+",
            _ => "",
        };
        let total = micros.unsigned_abs();
        let _ = write!(
            out,
            "{space}{sign}{:02}:{:02}:{:02}",
            total / 3_600_000_000,
            total / 60_000_000 % 60,
            total / 1_000_000 % 60
        );
        let fraction = total % 1_000_000;
        if fraction != 0 {
            let digits = format!("{fraction:06}");
            let _ = write!(out, ".{}", digits.trim_end_matches('0'));
        }
    }
    Ok(out)
}

fn timetz_text(ty: &Type, raw: &[u8]) -> Result<String> {
    let micros = i64::from_be_bytes(be(ty, raw, 0)?);
    // PostgreSQL keeps the zone as seconds west of UTC.
    let zone = i32::from_be_bytes(be(ty, raw, 8)?);
    let sign = if zone <= 0 { '+' } else { '-' };
    let seconds = zone.unsigned_abs();
    let (hours, minutes, rest) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    let offset = if rest != 0 {
        format!("{hours:02}:{minutes:02}:{rest:02}")
    } else if minutes != 0 {
        format!("{hours:02}:{minutes:02}")
    } else {
        format!("{hours:02}")
    };
    Ok(format!("{}{sign}{offset}", format_time(micros)))
}

fn lsn_text(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF)
}

// std writes an IPv4-compatible address (::a.b.c.d) as hex words; PostgreSQL keeps the dots.
fn inet_text(ty: &Type, raw: &[u8]) -> Result<String> {
    let [_family, bits, is_cidr, _length] = be::<4>(ty, raw, 0)?;
    let address = raw.get(4..).ok_or_else(|| malformed(ty))?;
    let (address, all_bits) = if let Ok(octets) = <[u8; 4]>::try_from(address) {
        (IpAddr::from(octets), 32)
    } else if let Ok(octets) = <[u8; 16]>::try_from(address) {
        (IpAddr::from(octets), 128)
    } else {
        return Err(malformed(ty));
    };
    if bits == all_bits && is_cidr == 0 {
        return Ok(address.to_string());
    }
    Ok(format!("{address}/{bits}"))
}

fn uuid_text(ty: &Type, raw: &[u8]) -> Result<String> {
    let bytes: [u8; 16] = be(ty, raw, 0)?;
    let hex = crate::secret::hex(&bytes);
    Ok(format!(
        "{}-{}-{}-{}-{}",
        &hex[..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..]
    ))
}

fn numeric_text(ty: &Type, raw: &[u8]) -> Result<String> {
    let ndigits = i16::from_be_bytes(be(ty, raw, 0)?);
    let weight = i16::from_be_bytes(be(ty, raw, 2)?);
    let sign = u16::from_be_bytes(be(ty, raw, 4)?);
    let scale = usize::from(u16::from_be_bytes(be(ty, raw, 6)?));
    let ndigits = usize::try_from(ndigits).unwrap_or(0);
    let digits = raw.get(8..8 + 2 * ndigits).ok_or_else(|| malformed(ty))?;
    match sign {
        0xC000 => return Ok("NaN".to_string()),
        0xD000 => return Ok("Infinity".to_string()),
        0xF000 => return Ok("-Infinity".to_string()),
        _ => {}
    }
    let digit = |index: i32| {
        usize::try_from(index)
            .ok()
            .and_then(|index| digits.get(2 * index..2 * index + 2))
            .map_or(0, |pair| i16::from_be_bytes([pair[0], pair[1]]))
    };
    let mut out = String::with_capacity(scale + 16);
    if sign == 0x4000 {
        out.push('-');
    }
    if weight < 0 {
        out.push('0');
    }
    for index in 0..=i32::from(weight) {
        let _ = if index == 0 {
            write!(out, "{}", digit(0))
        } else {
            write!(out, "{:04}", digit(index))
        };
    }
    if scale > 0 {
        out.push('.');
        let end = out.len() + scale;
        let mut index = i32::from(weight) + 1;
        while out.len() < end {
            let _ = write!(out, "{:04}", digit(index));
            index += 1;
        }
        out.truncate(end);
    }
    Ok(out)
}

fn array_json(element: &Type, raw: &[u8]) -> Result<Value> {
    let int = |at: usize| be::<4>(element, raw, at).map(i32::from_be_bytes);
    let dimensions = usize::try_from(int(0)?).map_err(|_| malformed(element))?;
    if dimensions == 0 {
        return Ok(json!([]));
    }
    let lengths = (0..dimensions)
        .map(|index| {
            int(12 + 8 * index)
                .and_then(|length| usize::try_from(length).map_err(|_| malformed(element)))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut at = 12 + 8 * dimensions;
    let mut flat = Vec::new();
    for _ in 0..lengths.iter().product::<usize>() {
        let length = int(at)?;
        at += 4;
        if length < 0 {
            flat.push(Value::Null);
            continue;
        }
        let end = at + length as usize;
        let bytes = raw.get(at..end).ok_or_else(|| malformed(element))?;
        at = end;
        flat.push(element_json(element, decode(element, bytes)?));
    }
    Ok(nest(&flat, &lengths))
}

fn element_json(element: &Type, cell: Cell) -> Value {
    match cell {
        Cell::Null => Value::Null,
        Cell::Integer(number) => json!(number),
        Cell::Real(number) => json!(number),
        Cell::Text(text) if is_json(element) => {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        }
        Cell::Text(text) => Value::String(text),
        Cell::Blob(bytes) => Value::String(BASE64.encode(bytes)),
    }
}

fn nest(flat: &[Value], lengths: &[usize]) -> Value {
    let Some((_, inner)) = lengths.split_first() else {
        return Value::Array(flat.to_vec());
    };
    if inner.is_empty() {
        return Value::Array(flat.to_vec());
    }
    let chunk = inner.iter().product::<usize>().max(1);
    Value::Array(flat.chunks(chunk).map(|part| nest(part, inner)).collect())
}

impl Driver for PostgresDriver {
    fn read(
        &mut self,
        stmt: &Statement,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        guard::check_read(Dialect::Postgres, &stmt.text)?;
        self.read_only_scope(deadline, |client| {
            read_into(client, &stmt.text, &stmt.params, sink)
        })?;
        Ok(None)
    }

    fn classify(&self, stmt: &Statement) -> Result<WriteKind> {
        guard::classify(Dialect::Postgres, &stmt.text)
    }

    fn execute(&mut self, stmt: &Statement, deadline: Instant) -> Result<u64> {
        let timeout = remaining(deadline)?.as_millis();
        let mode = if self.read_only {
            "SET default_transaction_read_only = on; "
        } else {
            ""
        };
        self.batch(&format!("{mode}SET statement_timeout = {timeout}"))
            .map_err(read_error)?;
        let statement = self.client.prepare(&stmt.text).map_err(read_error)?;
        check_param_count(&statement, &stmt.params)?;
        let params = text_params(&stmt.params);
        let refs: Vec<&(dyn ToSql + Sync)> = params
            .iter()
            .map(|param| param as &(dyn ToSql + Sync))
            .collect();
        self.client.execute(&statement, &refs).map_err(write_error)
    }

    fn begin(&mut self, isolation: Option<Isolation>) -> Result<()> {
        let level = match isolation {
            None => "",
            Some(Isolation::ReadCommitted) => " ISOLATION LEVEL READ COMMITTED",
            Some(Isolation::RepeatableRead) => " ISOLATION LEVEL REPEATABLE READ",
            Some(Isolation::Serializable) => " ISOLATION LEVEL SERIALIZABLE",
        };
        let mode = if self.read_only { " READ ONLY" } else { "" };
        self.batch(&format!("BEGIN{mode}{level}"))
            .map_err(read_error)?;
        self.transaction = Transaction::Atomic;
        Ok(())
    }

    // execute leaves statement_timeout at the time its own write had left; COMMIT gets a fresh one.
    fn commit(&mut self, deadline: Instant) -> Result<()> {
        let timeout = match remaining(deadline) {
            Ok(left) => left.as_millis(),
            // A transaction left open here would take the next writes with it, and they would never commit.
            Err(late) => {
                let _ = self.rollback();
                return Err(late);
            }
        };
        let committed = self.batch(&format!("SET statement_timeout = {timeout}; COMMIT"));
        self.transaction = Transaction::Idle;
        committed.map_err(write_error)
    }

    fn rollback(&mut self) -> Result<()> {
        self.transaction = Transaction::Idle;
        self.client.batch_execute("ROLLBACK").map_err(read_error)
    }

    fn in_transaction(&self) -> bool {
        matches!(
            self.transaction,
            Transaction::Atomic | Transaction::AtomicRead
        )
    }

    fn close_read(&mut self) -> Result<()> {
        if matches!(self.transaction, Transaction::Idle | Transaction::Atomic) {
            return Ok(());
        }
        self.batch("").map_err(read_error)
    }

    fn objects(
        &mut self,
        query: &ObjectQuery,
        sink: &mut RowSink,
        deadline: Instant,
    ) -> Result<Option<String>> {
        if let Some(kind) = &query.kind
            && !["table", "view", "function", "trigger", "enum"].contains(&kind.as_str())
        {
            return Err(Error::invalid(
                "PostgreSQL object kinds are table, view, function, trigger, and enum.",
            ));
        }
        let offset = super::offset_cursor(query.cursor.as_deref())?;
        let sql = "SELECT kind, schema, name FROM (
              SELECT CASE WHEN table_type = 'VIEW' THEN 'view' ELSE 'table' END AS kind, table_schema::text AS schema, table_name::text AS name
              FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              UNION ALL
              SELECT 'function', n.nspname::text, p.proname::text FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname NOT IN ('pg_catalog', 'information_schema') AND p.prokind = 'f'
              UNION ALL
              SELECT DISTINCT 'trigger', event_object_schema::text, trigger_name::text FROM information_schema.triggers
              UNION ALL
              SELECT 'enum', n.nspname::text, t.typname::text FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.typtype = 'e'
            ) objects
            WHERE ($1::text IS NULL OR kind = $1::text) AND ($2::text IS NULL OR name ILIKE $2::text ESCAPE '\\')
            ORDER BY schema, kind, name LIMIT $3::int8 OFFSET $4::int8";
        let params = vec![
            json!(query.kind),
            json!(query.search.as_deref().map(super::like_pattern)),
            json!(query.limit),
            json!(offset),
        ];
        self.read_only_scope(deadline, |client| read_into(client, sql, &params, sink))?;
        Ok(super::next_offset_cursor(offset, query.limit, sink.total()))
    }

    fn describe(&mut self, name: &str, deadline: Instant) -> Result<Value> {
        let found = self.text_rows(
            deadline,
            "SELECT c.oid::text, n.nspname::text, c.relname::text, c.relkind::text
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = to_regclass($1)",
            &[name],
        )?;
        let Some([Some(oid), Some(schema), Some(table), Some(kind)]) = found
            .into_iter()
            .next()
            .map(<[_; 4]>::try_from)
            .and_then(|row| row.ok())
        else {
            return Err(
                Error::new(Code::ResultNotFound, format!("No object named \"{name}\"."))
                    .with_hint("use objects"),
            );
        };
        let columns = self.text_rows(
            deadline,
            "SELECT a.attname::text, format_type(a.atttypid, a.atttypmod), (NOT a.attnotnull)::text, pg_get_expr(d.adbin, d.adrelid),
                    COALESCE(a.attnum = ANY(i.indkey), false)::text
             FROM pg_attribute a
             LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
             LEFT JOIN pg_index i ON i.indrelid = a.attrelid AND i.indisprimary
             WHERE a.attrelid = $1::text::oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            &[&oid],
        )?;
        let indexes = self.text_rows(
            deadline,
            "SELECT indexname::text, indexdef::text FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 ORDER BY indexname",
            &[&schema, &table],
        )?;
        let constraints = self.text_rows(
            deadline,
            // Not information_schema: it lists one CHECK per NOT NULL column, which the columns already show.
            "SELECT conname::text, CASE contype WHEN 'c' THEN 'CHECK' WHEN 'f' THEN 'FOREIGN KEY'
                    WHEN 'p' THEN 'PRIMARY KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'x' THEN 'EXCLUDE' ELSE contype::text END
             FROM pg_constraint WHERE conrelid = $1::text::oid AND contype <> 'n' ORDER BY conname",
            &[&oid],
        )?;
        let ddl = if matches!(kind.as_str(), "v" | "m") {
            let rows = self.text_rows(
                deadline,
                "SELECT pg_get_viewdef($1::text::oid, true)",
                &[&oid],
            )?;
            rows.into_iter()
                .next()
                .and_then(|row| row.into_iter().next().flatten())
        } else {
            None
        };
        Ok(json!({
            "name": table,
            "schema": schema,
            "kind": match kind.as_str() { "v" => "view", "m" => "materialized view", "S" => "sequence", "i" => "index", _ => "table" },
            "columns": columns.iter().map(|row| super::column_json(
                row[0].as_deref().unwrap_or_default(),
                row[1].as_deref().unwrap_or_default(),
                row[2].as_deref() == Some("true"),
                row[3].as_deref(),
                row[4].as_deref() == Some("true"),
            )).collect::<Vec<_>>(),
            "indexes": indexes.iter().map(|row| json!({"name": row[0], "definition": row[1]})).collect::<Vec<_>>(),
            "constraints": constraints.iter().map(|row| json!({"name": row[0], "type": row[1]})).collect::<Vec<_>>(),
            "ddl": ddl,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_numeric_binary_exactly() {
        let encode = |weight: i16, sign: u16, scale: u16, digits: &[i16]| {
            let mut raw = Vec::new();
            for part in [digits.len() as i16, weight] {
                raw.extend_from_slice(&part.to_be_bytes());
            }
            raw.extend_from_slice(&sign.to_be_bytes());
            raw.extend_from_slice(&scale.to_be_bytes());
            for digit in digits {
                raw.extend_from_slice(&digit.to_be_bytes());
            }
            numeric_text(&Type::NUMERIC, &raw).unwrap()
        };
        assert_eq!(encode(1, 0, 2, &[12, 3456, 7800]), "123456.78");
        assert_eq!(encode(-1, 0x4000, 4, &[1]), "-0.0001");
        assert_eq!(encode(-2, 0, 8, &[1]), "0.00000001");
        assert_eq!(encode(2, 0, 0, &[9]), "900000000");
        assert_eq!(encode(0, 0xC000, 0, &[]), "NaN");
        assert_eq!(encode(0, 0, 0, &[]), "0");
    }

    #[test]
    fn ssl_modes_follow_libpq() {
        let ca = || Some("/ca.pem".to_string());
        let cases = [
            ("", Tls::Off),
            ("?sslmode=disable&sslrootcert=/ca.pem", Tls::Off),
            ("?sslmode=require", Tls::Unverified),
            ("?sslmode=require&sslrootcert=/ca.pem", Tls::VerifyCa(ca())),
            ("?sslmode=verify-ca", Tls::VerifyCa(None)),
            (
                "?sslmode=verify-ca&sslrootcert=/ca.pem",
                Tls::VerifyCa(ca()),
            ),
            ("?sslmode=verify-full", Tls::VerifyFull(None)),
            (
                "?sslrootcert=/ca.pem&sslmode=verify-full",
                Tls::VerifyFull(ca()),
            ),
            (
                "?sslmode=disable&sslmode=verify-full",
                Tls::VerifyFull(None),
            ),
        ];
        for (query, tls) in cases {
            let target = format!("postgres://u@h/db{query}");
            assert_eq!(
                split_ssl_mode(&target).unwrap(),
                ("postgres://u@h/db".to_string(), tls),
                "{query}"
            );
        }
        assert_eq!(
            split_ssl_mode("postgres://u@h/db?sslmode=require&application_name=a")
                .unwrap()
                .0,
            "postgres://u@h/db?application_name=a"
        );
        for mode in ["allow", "prefer", "verify", "REQUIRE"] {
            let error = split_ssl_mode(&format!("postgres://u@h/db?sslmode={mode}")).unwrap_err();
            assert_eq!(error.code, Code::InvalidCommand, "{mode}");
        }
    }

    #[test]
    fn tls_configs_use_ring_and_check_the_ca_file_first() {
        for tls in [Tls::Unverified, Tls::VerifyCa(None), Tls::VerifyFull(None)] {
            let config = tls_config(&tls).unwrap().expect("a TLS mode has a config");
            assert!(!config.crypto_provider().cipher_suites.is_empty());
        }
        assert!(tls_config(&Tls::Off).unwrap().is_none());
        let missing = std::env::temp_dir().join("stql-no-such-ca.pem");
        let empty = std::env::temp_dir().join(format!("stql-empty-ca-{}.pem", std::process::id()));
        std::fs::write(&empty, "not a certificate\n").unwrap();
        for path in [&missing, &empty] {
            let path = path.display().to_string();
            for tls in [
                Tls::VerifyCa(Some(path.clone())),
                Tls::VerifyFull(Some(path.clone())),
            ] {
                let error = tls_config(&tls).unwrap_err();
                assert_eq!(error.code, Code::ConnectionFailed, "{path}");
                assert!(error.message.contains("CA file"), "{}", error.message);
            }
        }
        let _ = std::fs::remove_file(empty);
    }
}
