use std::cmp::Ordering;
use std::path::Path;
use std::process::Command as Process;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::command::ProfileCommand;
use crate::config::{self, Profile, Profiles};
use crate::driver::Backend;
use crate::error::{Code, Error, Result};
use crate::response::Response;

pub const SERVICE: &str = "stql";
const MAX_PASSWORD_CHARS: usize = 4_096;
const MAX_TARGET_UTF16: usize = 8_192;
const MAX_PAYLOAD_UTF16: usize = 16_384;
const FINGERPRINT_PROTOCOLS: [&str; 6] = [
    "postgres:",
    "mysql:",
    "mongodb:",
    "mongodb+srv:",
    "redis:",
    "rediss:",
];
const TARGET_OVERRIDE_KEYS: [&str; 8] = [
    "password", "pwd", "user", "username", "host", "port", "database", "dbname",
];
const TLS_QUERY_KEYS: [&str; 9] = [
    "sslmode",
    "ssl",
    "sslrootcert",
    "sslcert",
    "sslkey",
    "rejectunauthorized",
    "uselibpqcompat",
    "sslnegotiation",
    "pylon_tls_ca",
];
// ICU root collation of printable ASCII, from Node's String.prototype.localeCompare. Letters share
// a primary weight with their uppercase form; lowercase sorts first only when all else is equal.
const COLLATION_ORDER: &str =
    " _-,;:!?.'\"()[]{}@*/\\&#%`^+<=>|~$0123456789abcdefghijklmnopqrstuvwxyz";
const PASSWORD_DIALOG_SCRIPT: &str = r#"$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false
Add-Type -AssemblyName System.Windows.Forms
Add-Type -Namespace StateQL -Name Win32 -MemberDefinition '[DllImport("user32.dll")] public static extern bool ShowWindow(IntPtr window, int command);'
[Windows.Forms.Application]::EnableVisualStyles()
$form = New-Object Windows.Forms.Form -Property @{ Text = 'StateQL'; TopMost = $true; StartPosition = 'CenterScreen'; FormBorderStyle = 'FixedDialog'; MaximizeBox = $false; MinimizeBox = $false; AutoSize = $true; AutoSizeMode = 'GrowAndShrink'; Padding = 12 }
$layout = New-Object Windows.Forms.FlowLayoutPanel -Property @{ FlowDirection = 'TopDown'; AutoSize = $true }
$label = New-Object Windows.Forms.Label -Property @{ Text = $env:STQL_PROMPT_MESSAGE; AutoSize = $true; MaximumSize = '360,0' }
$box = New-Object Windows.Forms.TextBox -Property @{ UseSystemPasswordChar = $true; Width = 360 }
$buttons = New-Object Windows.Forms.FlowLayoutPanel -Property @{ FlowDirection = 'RightToLeft'; Width = 366; Height = 34 }
$cancel = New-Object Windows.Forms.Button -Property @{ Text = 'Cancel'; DialogResult = 'Cancel' }
$ok = New-Object Windows.Forms.Button -Property @{ Text = 'OK'; DialogResult = 'OK' }
$buttons.Controls.AddRange(@($cancel, $ok))
$layout.Controls.AddRange(@($label, $box, $buttons))
$form.Controls.Add($layout)
$form.AcceptButton = $ok
$form.CancelButton = $cancel
$form.Add_Shown({ [void][StateQL.Win32]::ShowWindow($form.Handle, 5); $form.Activate(); [void]$box.Focus() })
if ($form.ShowDialog() -ne 'OK') { exit 1 }
[Console]::Out.Write($box.Text)"#;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    host: String,
    port: Option<u16>,
    user: String,
    password: String,
}

#[derive(Debug, PartialEq)]
struct Destination {
    host: String,
    port: Option<u16>,
    user: String,
}

impl Credential {
    fn destination(&self) -> Destination {
        Destination {
            host: self.host.clone(),
            port: self.port,
            user: self.user.clone(),
        }
    }
}

fn destination(target: &str) -> Result<Destination> {
    let url = url::Url::parse(target).map_err(|_| {
        Error::invalid("A profile URL with a password must be a valid URL with one host.")
    })?;
    let host = url
        .host_str()
        .filter(|host| !host.is_empty())
        .ok_or_else(|| Error::invalid("The profile URL has no host."))?;
    let default_port = match Backend::of(target) {
        Backend::Postgres => Some(5432),
        Backend::Mysql => Some(3306),
        Backend::Mongo if url.scheme() == "mongodb" => Some(27017),
        Backend::Redis => Some(6379),
        _ => None,
    };
    let user = percent_encoding::percent_decode_str(url.username())
        .decode_utf8_lossy()
        .into_owned();
    Ok(Destination {
        host: host.to_ascii_lowercase(),
        port: url.port().or(default_port),
        user,
    })
}

fn keyring_error(error: keyring::Error) -> Error {
    Error::new(
        Code::CredentialMissing,
        format!("The OS keyring is not available: {error}"),
    )
    .with_hint("use a profile with --env VAR")
}

fn entry(service: &str, account: &str) -> Result<keyring::Entry> {
    keyring::Entry::new(service, account).map_err(keyring_error)
}

fn read_entry(service: &str, account: &str) -> Result<Option<String>> {
    match entry(service, account)?.get_password() {
        Ok(secret) => Ok(Some(secret)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(error) => Err(keyring_error(error)),
    }
}

fn write_entry(service: &str, account: &str, credential: &Credential) -> Result<()> {
    let payload = serde_json::to_string(credential).map_err(Error::internal)?;
    entry(service, account)?
        .set_password(&payload)
        .map_err(keyring_error)
}

fn delete_entry(service: &str, account: &str) -> Result<bool> {
    match entry(service, account)?.delete_credential() {
        Ok(()) => Ok(true),
        Err(keyring::Error::NoEntry) => Ok(false),
        Err(error) => Err(keyring_error(error)),
    }
}

fn has_control_characters(text: &str) -> bool {
    text.chars()
        .any(|char| char <= '\u{1f}' || char == '\u{7f}')
}

fn valid_password(password: &str) -> bool {
    password.chars().count() <= MAX_PASSWORD_CHARS && !has_control_characters(password)
}

pub fn url_has_secret(target: &str) -> bool {
    let rest = target.split_once("://").map_or(target, |(_, rest)| rest);
    let authority = &rest[..rest.find(['/', '?', '#']).unwrap_or(rest.len())];
    if authority
        .rsplit_once('@')
        .is_some_and(|(userinfo, _)| userinfo.contains(':'))
    {
        return true;
    }
    let Some((_, query)) = rest.split_once('?') else {
        return false;
    };
    let query = query.split('#').next().unwrap_or_default();
    url::form_urlencoded::parse(query.as_bytes()).any(|(key, _)| {
        let key = key.to_ascii_lowercase();
        [
            "pass",
            "token",
            "secret",
            "privatekey",
            "private_key",
            "private-key",
            "apikey",
            "api_key",
            "api-key",
        ]
        .iter()
        .any(|word| key.contains(word))
    })
}

fn valid_profile_name(name: &str) -> bool {
    let mut chars = name.chars();
    let first_ok = chars
        .next()
        .is_some_and(|char| char.is_ascii_alphanumeric());
    first_ok
        && name.len() <= 64
        && chars.all(|char| char.is_ascii_alphanumeric() || "._-".contains(char))
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    let first_ok = chars
        .next()
        .is_some_and(|char| char.is_ascii_alphabetic() || char == '_');
    first_ok && chars.all(|char| char.is_ascii_alphanumeric() || char == '_')
}

pub fn profile_command(home: &Path, service: &str, action: ProfileCommand) -> Result<Response> {
    match action {
        ProfileCommand::Add {
            name,
            url,
            env,
            env_file,
            ask_password,
            read_write,
            force,
        } => {
            let env_file = match env_file {
                Some(path) => Some(std::path::absolute(path)?.display().to_string()),
                None => None,
            };
            add_profile(
                home,
                service,
                &name,
                url,
                env,
                env_file,
                ask_password,
                !read_write,
                force,
            )
        }
        ProfileCommand::List => {
            let profiles = config::load_profiles(home)?;
            let listed: Vec<Value> = profiles
                .profiles
                .iter()
                .map(|(name, profile)| {
                    let mut listed = json!({"name": name});
                    if let (Value::Object(listed), Value::Object(fields)) =
                        (&mut listed, json!(profile))
                    {
                        listed.extend(fields);
                    }
                    listed
                })
                .collect();
            Ok(Response::default().with("profiles", listed))
        }
        ProfileCommand::Remove { name } => {
            let mut profiles = config::load_profiles(home)?;
            if profiles.profiles.remove(&name).is_none() {
                return Err(Error::invalid(format!("No profile named \"{name}\"."))
                    .with_hint("stql profile list"));
            }
            config::save_profiles(home, &profiles)?;
            let password_deleted = delete_entry(service, &name)?;
            Ok(Response::default()
                .with("profile", &name)
                .with("removed", true)
                .with("password_deleted", password_deleted))
        }
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "one call site maps the clap fields one to one"
)]
fn add_profile(
    home: &Path,
    service: &str,
    name: &str,
    url: Option<String>,
    env: Option<String>,
    env_file: Option<String>,
    ask_password: bool,
    read_only: bool,
    force: bool,
) -> Result<Response> {
    if !valid_profile_name(name) {
        return Err(Error::invalid(
            "A profile name is 1-64 letters, digits, dots, underscores, or hyphens.",
        ));
    }
    let mut profiles = config::load_profiles(home)?;
    if profiles.profiles.contains_key(name) && !force {
        return Err(
            Error::invalid(format!("Profile \"{name}\" already exists."))
                .with_hint("add --force to replace it"),
        );
    }
    let profile = match (url, env) {
        (Some(url), None) => Profile::Url {
            url: profile_target(&url)?,
            read_only,
        },
        (None, Some(env)) if valid_env_name(&env) => Profile::Env {
            env,
            read_only,
            env_file,
        },
        (None, Some(_)) => {
            return Err(Error::invalid(
                "The environment variable name is not valid.",
            ));
        }
        _ => return Err(Error::invalid("Give a URL or --env VAR.")),
    };
    let network_url = match &profile {
        Profile::Url { url, .. } if Backend::of(url) != Backend::Sqlite => Some(url.as_str()),
        _ => None,
    };
    let password = match (ask_password, network_url) {
        (false, _) => None,
        (true, Some(url)) => Some(ask_for_password(name, url)?),
        (true, None) => {
            return Err(Error::invalid(
                "--ask-password needs a database server URL.",
            ));
        }
    };
    let password_saved = password.is_some();
    match (password, network_url) {
        (Some(password), Some(url)) => {
            let Destination { host, port, user } = destination(url)?;
            write_entry(
                service,
                name,
                &Credential {
                    host,
                    port,
                    user,
                    password,
                },
            )?;
        }
        _ => forget_password_for_other_destination(service, name, network_url),
    }
    let summary = json!(&profile);
    profiles.profiles.insert(name.to_string(), profile);
    config::save_profiles(home, &profiles)?;
    Ok(Response::default()
        .with("profile", name)
        .with("saved", summary)
        .with("password_saved", password_saved))
}

// An unreadable keyring is skipped: connect still refuses a password saved for another destination.
fn forget_password_for_other_destination(service: &str, name: &str, url: Option<&str>) {
    let Ok(Some(payload)) = read_entry(service, name) else {
        return;
    };
    let saved = serde_json::from_str::<Credential>(&payload)
        .ok()
        .map(|credential| credential.destination());
    let current = url.and_then(|url| destination(url).ok());
    if saved.is_none() || saved != current {
        let _ = delete_entry(service, name);
    }
}

fn profile_target(url: &str) -> Result<String> {
    if Backend::of(url) == Backend::Sqlite {
        return Ok(crate::driver::sqlite_path(url)?.display().to_string());
    }
    if url_has_secret(url) {
        return Err(Error::new(
            Code::PermissionDenied,
            "A profile URL must not contain a password or token.",
        )
        .with_hint("add --ask-password, or use --env VAR"));
    }
    Ok(url.to_string())
}

pub fn profile_url(service: &str, name: &str, url: &str) -> Result<String> {
    if Backend::of(url) == Backend::Sqlite {
        return Ok(url.to_string());
    }
    let Some(payload) = read_entry(service, name)? else {
        return Ok(url.to_string());
    };
    let credential: Credential = serde_json::from_str(&payload).map_err(|_| {
        Error::new(
            Code::CredentialMissing,
            format!("The saved password of profile \"{name}\" is not readable."),
        )
        .with_hint(format!(
            "stql profile add {name} <url> --ask-password --force"
        ))
    })?;
    let current = destination(url)?;
    if credential.destination() != current {
        return Err(Error::new(
            Code::CredentialMismatch,
            format!(
                "The saved password of profile \"{name}\" belongs to {}@{}, but the profile now points to {}@{}.",
                credential.user, credential.host, current.user, current.host
            ),
        )
        .with_hint(format!("stql profile add {name} <url> --ask-password --force")));
    }
    let mut parsed = url::Url::parse(url).map_err(Error::internal)?;
    parsed
        .set_password(Some(&credential.password))
        .map_err(|()| Error::invalid("The profile URL cannot carry a password."))?;
    Ok(parsed.to_string())
}

fn prompt_message(profile: &str, url: &url::Url) -> String {
    let user = percent_encoding::percent_decode_str(url.username()).decode_utf8_lossy();
    let user = if user.is_empty() {
        String::new()
    } else {
        format!("{user}@")
    };
    let host = url.host_str().unwrap_or_default();
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    format!("Password for {user}{host}{port} (StateQL profile \"{profile}\")")
}

fn dialog_process(message: &str) -> Process {
    if cfg!(windows) {
        let utf16: Vec<u8> = PASSWORD_DIALOG_SCRIPT
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, utf16);
        let mut process = Process::new("powershell.exe");
        process
            .args(["-NoProfile", "-EncodedCommand", &encoded])
            .env("STQL_PROMPT_MESSAGE", message);
        #[cfg(windows)]
        std::os::windows::process::CommandExt::creation_flags(&mut process, 0x0800_0000);
        return process;
    }
    if cfg!(target_os = "macos") {
        let mut process = Process::new("osascript");
        process.args([
            "-e",
            "on run argv",
            "-e",
            r#"text returned of (display dialog (item 1 of argv) default answer "" with hidden answer with title "StateQL")"#,
            "-e",
            "end run",
            message,
        ]);
        return process;
    }
    let mut process = Process::new("zenity");
    process.args(["--password", "--title", message]);
    process
}

fn dialog_password(output: &str) -> &str {
    if cfg!(windows) {
        return output;
    }
    output
        .strip_suffix('\n')
        .map_or(output, |text| text.strip_suffix('\r').unwrap_or(text))
}

fn ask_for_password(profile: &str, url: &str) -> Result<String> {
    let parsed =
        url::Url::parse(url).map_err(|_| Error::invalid("The profile URL is not valid."))?;
    let output = match dialog_process(&prompt_message(profile, &parsed)).output() {
        Ok(output) => output,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::invalid(
                "--ask-password needs a password dialog: zenity on Linux, osascript on macOS.",
            ));
        }
        Err(error) => return Err(Error::internal(error)),
    };
    if !output.status.success() {
        return Err(Error::invalid("Password entry was cancelled."));
    }
    let text = String::from_utf8(output.stdout)
        .map_err(|_| Error::invalid("The password is not valid UTF-8."))?;
    let password = dialog_password(&text);
    if password.is_empty() {
        return Err(Error::invalid("No password was entered."));
    }
    if !valid_password(password) {
        return Err(Error::invalid(
            "The password is too long or contains control characters.",
        ));
    }
    Ok(password.to_string())
}

fn collation_key(text: &str) -> (Vec<u32>, Vec<bool>) {
    text.chars()
        .map(|char| {
            let lower = char.to_ascii_lowercase();
            let primary = COLLATION_ORDER
                .find(lower)
                .map_or(1_000 + u32::from(char), |index| index as u32);
            (primary, char.is_ascii_uppercase())
        })
        .unzip()
}

fn locale_compare(left: &str, right: &str) -> Ordering {
    collation_key(left).cmp(&collation_key(right))
}

fn decoded_part(part: &str) -> Option<String> {
    let decoded = percent_encoding::percent_decode_str(part)
        .decode_utf8()
        .ok()?;
    (!has_control_characters(&decoded)).then(|| decoded.into_owned())
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn bad_percent_escape(target: &str) -> bool {
    let bytes = target.as_bytes();
    bytes.iter().enumerate().any(|(index, byte)| {
        *byte == b'%'
            && !(bytes.get(index + 1).is_some_and(u8::is_ascii_hexdigit)
                && bytes.get(index + 2).is_some_and(u8::is_ascii_hexdigit))
    })
}

// Port of targetFingerprint from StateQL 0.20 (TypeScript), used only to read its keyring entries.
fn target_fingerprint(target: &str) -> Option<String> {
    let unsafe_target =
        target.is_empty() || utf16_len(target) > MAX_TARGET_UTF16 || has_control_characters(target);
    if unsafe_target || bad_percent_escape(target) {
        return None;
    }
    let identity = match url::Url::parse(target) {
        Ok(url) => destination_identity(&url)?,
        Err(_) => target.to_string(),
    };
    Some(hex(&Sha256::digest(identity.as_bytes())))
}

fn destination_identity(url: &url::Url) -> Option<String> {
    let scheme = url.scheme().to_ascii_lowercase();
    let protocol = if scheme == "postgresql" {
        "postgres:".to_string()
    } else {
        format!("{scheme}:")
    };
    let hostname = url.host_str().unwrap_or_default();
    let has_fragment = url.fragment().is_some_and(|fragment| !fragment.is_empty());
    let has_password = url.password().is_some_and(|password| !password.is_empty());
    if !FINGERPRINT_PROTOCOLS.contains(&protocol.as_str())
        || hostname.is_empty()
        || has_fragment
        || has_password
    {
        return None;
    }
    let username = decoded_part(url.username())?;
    let database = decoded_part(url.path())?;
    let mut query: Vec<(String, String)> = Vec::new();
    for (key, value) in url.query_pairs() {
        if has_control_characters(&key) || has_control_characters(&value) {
            return None;
        }
        let normalized = key.to_lowercase();
        if TARGET_OVERRIDE_KEYS.contains(&normalized.as_str()) {
            return None;
        }
        if !TLS_QUERY_KEYS.contains(&normalized.as_str()) {
            query.push((key.into_owned(), value.into_owned()));
        }
    }
    query.sort_by(|(left_key, left_value), (right_key, right_value)| {
        if left_key == right_key {
            locale_compare(left_value, right_value)
        } else {
            locale_compare(left_key, right_key)
        }
    });
    let default_port = match protocol.as_str() {
        "postgres:" => "5432",
        "mysql:" => "3306",
        "mongodb:" => "27017",
        "redis:" | "rediss:" => "6379",
        _ => "",
    };
    let hostname = hostname.to_lowercase();
    let identity = json!({
        "protocol": protocol,
        "username": username,
        "hostname": hostname.strip_suffix('.').unwrap_or(&hostname),
        "port": url.port().map_or(default_port.to_string(), |port| port.to_string()),
        "database": database,
        "query": query.iter().map(|(key, value)| json!([key, value])).collect::<Vec<_>>(),
    });
    Some(identity.to_string())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

enum TsPassword {
    Copied(String),
    Missing(&'static str),
}

// TS entries are a raw password, or a JSON v2 record bound to the target by a fingerprint.
fn ts_password(service: &str, password_ref: &str, target: &str) -> TsPassword {
    let Some(account) = password_ref.strip_prefix("os:") else {
        return TsPassword::Missing("the password reference is not an os: reference");
    };
    let payload = match read_entry(service, account) {
        Ok(Some(payload)) => payload,
        Ok(None) => return TsPassword::Missing("the keyring entry is missing"),
        Err(_) => return TsPassword::Missing("the keyring cannot be read"),
    };
    let record = serde_json::from_str::<Value>(&payload)
        .ok()
        .filter(|value| value.get("version").is_some());
    let Some(record) = record else {
        return TsPassword::Copied(payload);
    };
    let Some(stored) = stored_password(&payload, &record) else {
        return TsPassword::Missing("the keyring entry is not valid");
    };
    if stored.stale {
        return TsPassword::Missing("the keyring entry is marked stale");
    }
    if target_fingerprint(target).as_deref() != Some(stored.fingerprint.as_str()) {
        return TsPassword::Missing("the keyring entry belongs to another target");
    }
    TsPassword::Copied(stored.password)
}

struct StoredPassword {
    fingerprint: String,
    password: String,
    stale: bool,
}

fn stored_password(payload: &str, record: &Value) -> Option<StoredPassword> {
    let object = record.as_object()?;
    let known = object
        .keys()
        .all(|key| ["version", "fingerprint", "password", "stale"].contains(&key.as_str()));
    if utf16_len(payload) > MAX_PAYLOAD_UTF16 || !known || object.get("version") != Some(&json!(2))
    {
        return None;
    }
    let fingerprint = object.get("fingerprint")?.as_str()?;
    let fingerprint_ok = fingerprint.len() == 64
        && fingerprint
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    let password = object.get("password")?.as_str()?;
    let stale = match object.get("stale") {
        None => false,
        Some(Value::Bool(true)) => true,
        Some(_) => return None,
    };
    let password_ok =
        utf16_len(password) <= MAX_PASSWORD_CHARS && !has_control_characters(password);
    (fingerprint_ok && password_ok).then(|| StoredPassword {
        fingerprint: fingerprint.to_string(),
        password: password.to_string(),
        stale,
    })
}

struct TsProfile {
    name: String,
    target: Option<String>,
    secret_env: Option<String>,
    credential_ref: Option<String>,
    password_ref: Option<String>,
    read_only: bool,
}

fn read_ts_profiles(path: &Path) -> rusqlite::Result<Vec<TsProfile>> {
    let conn =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let columns: Vec<String> = conn
        .prepare("SELECT name FROM pragma_table_info('profiles')")?
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    let column = |name: &str| {
        if columns.iter().any(|column| column == name) {
            name.to_string()
        } else {
            "NULL".to_string()
        }
    };
    let sql = format!(
        "SELECT name, target, secret_env, {}, {}, read_only FROM profiles ORDER BY name",
        column("credential_ref"),
        column("password_ref")
    );
    let mut statement = conn.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(TsProfile {
            name: row.get(0)?,
            target: row.get(1)?,
            secret_env: row.get(2)?,
            credential_ref: row.get(3)?,
            password_ref: row.get(4)?,
            read_only: row.get::<_, i64>(5)? != 0,
        })
    })?;
    rows.collect()
}

pub fn migrate_ts_profiles(home: &Path, service: &str) -> Result<Option<String>> {
    let old_store = home.join("state.sqlite");
    if config::profiles_path(home).exists() || !old_store.exists() {
        return Ok(None);
    }
    let rows = match read_ts_profiles(&old_store) {
        Ok(rows) => rows,
        Err(error) => {
            return Ok(Some(format!(
                "stql: cannot read the profiles in {}: {error}",
                old_store.display()
            )));
        }
    };
    let mut profiles = Profiles::default();
    let mut migrated = Vec::new();
    let mut skipped = Vec::new();
    let mut without_password = Vec::new();
    for row in rows {
        let read_only = row.read_only;
        let profile = match (row.target, row.secret_env, row.credential_ref) {
            (Some(url), _, _) => {
                if let Some(password_ref) = &row.password_ref
                    && let Err(reason) = copy_ts_password(service, &row.name, password_ref, &url)
                {
                    without_password.push(format!("{} ({reason})", row.name));
                }
                Profile::Url { url, read_only }
            }
            (None, Some(env), _) => Profile::Env {
                env,
                read_only,
                env_file: None,
            },
            _ => {
                skipped.push(row.name);
                continue;
            }
        };
        migrated.push(row.name.clone());
        profiles.profiles.insert(row.name, profile);
    }
    config::save_profiles(home, &profiles)?;
    let mut report = format!(
        "stql: migrated {} profiles from {}",
        migrated.len(),
        old_store.display()
    );
    if !migrated.is_empty() {
        report.push_str(&format!(": {}", migrated.join(", ")));
    }
    report.push('.');
    if !skipped.is_empty() {
        report.push_str(&format!(
            " Skipped (credential_ref is not supported): {}.",
            skipped.join(", ")
        ));
    }
    if !without_password.is_empty() {
        report.push_str(&format!(
            " Migrated without a password, run profile add NAME URL --ask-password --force: {}.",
            without_password.join(", ")
        ));
    }
    Ok(Some(report))
}

fn copy_ts_password(
    service: &str,
    name: &str,
    password_ref: &str,
    target: &str,
) -> std::result::Result<(), &'static str> {
    let password = match ts_password(service, password_ref, target) {
        TsPassword::Copied(password) => password,
        TsPassword::Missing(reason) => return Err(reason),
    };
    let Destination { host, port, user } =
        destination(target).map_err(|_| "the target has no single host")?;
    write_entry(
        service,
        name,
        &Credential {
            host,
            port,
            user,
            password,
        },
    )
    .map_err(|_| "the keyring cannot be written")
}

pub fn doctor_checks(home: &Path, service: &str) -> Vec<Value> {
    let keyring = match read_entry(service, "stql-doctor-probe") {
        Ok(_) => json!({"name": "keyring", "ok": true, "detail": "available"}),
        Err(error) => {
            json!({"name": "keyring", "ok": false, "detail": error.message, "hint": "only --env profiles work without a keyring"})
        }
    };
    let ts_store = home.join("state.sqlite");
    let detail = match (ts_store.exists(), config::profiles_path(home).exists()) {
        (false, _) => "absent",
        (true, true) => "present, profiles migrated",
        (true, false) => "present, profiles not migrated",
    };
    vec![
        keyring,
        json!({"name": "ts_store", "ok": true, "detail": detail}),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    // Generated by targetFingerprint of StateQL 0.20 (TypeScript, Node 24, ICU 78.2).
    const FINGERPRINTS: &[(&str, Option<&str>)] = &[
        (
            "postgres://app@db.example.com/sales",
            Some("1afbada60274dbebd3a36d39142f9e11a439237cf1b7b7959f40d4e697645bbb"),
        ),
        (
            "postgresql://app@DB.Example.COM.:5432/sales",
            Some("1afbada60274dbebd3a36d39142f9e11a439237cf1b7b7959f40d4e697645bbb"),
        ),
        (
            "postgres://app@db.example.com:6543/sales?sslmode=require&application_name=stql",
            Some("2aee91ab02a994dd228e509d19a51ff339c2e5386f565e5fe2a8d028061638f9"),
        ),
        (
            "postgres://app@db.example.com/sales?b=2&a=1&A=0&a=0&_x=1&a-b=1",
            Some("a77fe8c086fad1cc805f905d2121dde47fb55d7d7ad3e4909679a1efde21a8eb"),
        ),
        (
            "postgres://ap%40p@db.example.com/s%20ales",
            Some("f41656bbbd0cdc1327e20aeab0b450e4a8984aca636209cec0c11252d49731b9"),
        ),
        (
            "postgres://app@db.example.com/sales?x=a+b&y=%E2%9C%93",
            Some("3ead8c02d375099fd7f7769bdb1c652c21097261121091f7e8325631070fa633"),
        ),
        (
            "mysql://root@127.0.0.1/shop",
            Some("f156ba9664a1743b5844c60c0828e9e1ffd2baa6e978989fefd0f6f4aedb167d"),
        ),
        (
            "mysql://root@[::1]:3307/shop",
            Some("a3da4dba6018ca1c198583ae8c3cfdd7f78f8036310c0f020e77efd4362660fa"),
        ),
        (
            "mongodb://u@h/app?replicaSet=rs0&authSource=admin",
            Some("952037cea5a410eb74b382a9dc9c346300d117f7f46f0a54ca0751ea69525656"),
        ),
        (
            "mongodb+srv://u@cluster.example.net/app",
            Some("1c611b6573fb4f473ee6507e29feadcfde051938e16f0905c93a6c3f34e8e5c9"),
        ),
        (
            "mongodb://u@h1:1,h2:2/app",
            Some("0278c388d7e3b92637b23329bb8b2c4c9494579d4c1fff494a49a9f5c5127846"),
        ),
        (
            "redis://default@cache:6380/0",
            Some("f60fef9d37483f37fe7795e4b3261c84b1754f12bd56e858bfb47c1170d23070"),
        ),
        (
            "rediss://cache",
            Some("e24fdbe4a5ba5990fa0c4961f7e030af735ce021cfba07dd0b0909cd5ba0bb76"),
        ),
        (
            "postgres://app@db.example.com/sales?k=Zeta&k=alpha&k=Alpha&k=beta",
            Some("f81aea06aefb2be5ca910d420e6621a73bb536c7dbd06e6e74fdf1e20d9e861c"),
        ),
        ("postgres://app@db.example.com/sales?user=x", None),
        ("postgres://app:pw@db.example.com/sales", None),
        ("postgres://app@db.example.com/sales#frag", None),
        ("postgres://app@db.example.com/%zz", None),
        ("postgres://app@db.example.com/%E2%28", None),
        ("sqlite:///tmp/x.db", None),
        ("http://h/x", None),
        ("", None),
    ];

    #[test]
    fn target_fingerprint_matches_the_ts_vectors() {
        for (target, expected) in FINGERPRINTS {
            assert_eq!(target_fingerprint(target).as_deref(), *expected, "{target}");
        }
    }

    #[test]
    fn locale_compare_follows_icu_for_ascii() {
        let mut words = vec![
            "b", "A", "a", "a-b", "ab", "Aa", "aA", "_x", "10", "9", "a b",
        ];
        words.sort_by(|left, right| locale_compare(left, right));
        assert_eq!(
            words,
            [
                "_x", "10", "9", "a", "A", "a b", "a-b", "aA", "Aa", "ab", "b"
            ]
        );
    }

    #[test]
    fn secrets_in_urls_are_detected() {
        assert!(url_has_secret("postgres://u:p@h/db"));
        assert!(url_has_secret("postgres://u:@h/db"));
        assert!(url_has_secret("mongodb://h1,h2/db?authToken=x"));
        assert!(url_has_secret("redis://h/0?api-key=x"));
        assert!(!url_has_secret(
            "postgres://u@h/db?sslmode=verify-full&sslrootcert=/ca.pem"
        ));
        assert!(!url_has_secret(
            "mysql://u@h/db?ssl-mode=VERIFY_CA&ssl-ca=/ca.pem"
        ));
        assert!(!url_has_secret("mongodb://h/db?tls=true&tlsCAFile=/ca.pem"));
        assert!(url_has_secret(
            "mongodb://h/db?tlsCertificateKeyFilePassword=x"
        ));
        assert!(!url_has_secret("mysql://h/db#p:a@x"));
    }

    #[test]
    fn dialog_prompt_and_output_are_plain_text() {
        let url = url::Url::parse("postgres://ap%40p@db.example.com:6543/sales").unwrap();
        assert_eq!(
            prompt_message("prod", &url),
            "Password for ap@p@db.example.com:6543 (StateQL profile \"prod\")"
        );
        if !cfg!(windows) {
            assert_eq!(dialog_password("secret\r\n"), "secret");
            assert_eq!(dialog_password("secret\n\n"), "secret\n");
        }
        assert!(valid_password("pässwörd"));
        assert!(!valid_password("tab\tinside"));
    }

    #[test]
    fn destinations_use_default_ports_and_lowercase_hosts() {
        let found = destination("postgres://App@DB.example.com/x").unwrap();
        assert_eq!(
            found,
            Destination {
                host: "db.example.com".into(),
                port: Some(5432),
                user: "App".into()
            }
        );
        assert_eq!(
            destination("mongodb+srv://u@c.example.net/x").unwrap().port,
            None
        );
        assert!(destination("mongodb://h1:1,h2:2/x").is_err());
    }
}
