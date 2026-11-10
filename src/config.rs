use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Code, Error, Result};

pub fn default_home() -> PathBuf {
    if let Some(home) = std::env::var_os("STQL_HOME").filter(|home| !home.is_empty()) {
        return std::path::absolute(PathBuf::from(home)).unwrap_or_default();
    }
    if cfg!(windows) {
        let local = std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| user_home().join("AppData").join("Local"));
        return local.join("stql");
    }
    let data = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| user_home().join(".local").join("share"));
    data.join("stql")
}

fn user_home() -> PathBuf {
    std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .unwrap_or_default()
}

pub fn create_private_dir(path: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(path)?;
    Ok(())
}

pub fn restrict_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum Profile {
    Url {
        url: String,
        read_only: bool,
    },
    Env {
        env: String,
        read_only: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        env_file: Option<String>,
    },
}

impl Profile {
    pub fn read_only(&self) -> bool {
        match self {
            Profile::Url { read_only, .. } | Profile::Env { read_only, .. } => *read_only,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profiles {
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

pub fn profiles_path(home: &Path) -> PathBuf {
    home.join("profiles.toml")
}

pub fn load_profiles(home: &Path) -> Result<Profiles> {
    let path = profiles_path(home);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Profiles::default());
        }
        Err(error) => return Err(error.into()),
    };
    toml::from_str(&text)
        .map_err(|error| Error::invalid(format!("{} is invalid: {error}", path.display())))
}

pub fn save_profiles(home: &Path, profiles: &Profiles) -> Result<()> {
    let text = toml::to_string(profiles).map_err(Error::internal)?;
    let path = profiles_path(home);
    let temp = home.join(format!("profiles.toml.{}.tmp", std::process::id()));
    fs::write(&temp, text)?;
    restrict_file(&temp)?;
    fs::rename(&temp, &path).inspect_err(|_| {
        let _ = fs::remove_file(&temp);
    })?;
    Ok(())
}

// The process environment wins; the file is read only when it does not set the variable.
pub fn env_value(var: &str, env_file: Option<&str>) -> Result<String> {
    if let Ok(value) = std::env::var(var) {
        return Ok(value);
    }
    let missing = |place: String| {
        Error::new(
            Code::CredentialMissing,
            format!("The environment variable {var} is not set{place}."),
        )
        .with_hint("export the full database URL in it")
    };
    let Some(path) = env_file else {
        return Err(missing(String::new()));
    };
    let text = fs::read_to_string(path).map_err(|error| {
        Error::new(
            Code::CredentialMissing,
            format!(
                "The environment variable {var} is not set, and {path} cannot be read: {error}"
            ),
        )
    })?;
    let value = text.lines().rev().find_map(|line| {
        let line = line.trim();
        let line = line.strip_prefix("export ").unwrap_or(line);
        let (name, value) = line.split_once('=')?;
        (name.trim() == var).then(|| unquote(value.trim()).to_string())
    });
    value.ok_or_else(|| missing(format!(" and is not in {path}")))
}

fn unquote(value: &str) -> &str {
    for quote in ['"', '\''] {
        if let Some(inner) = value
            .strip_prefix(quote)
            .and_then(|rest| rest.strip_suffix(quote))
        {
            return inner;
        }
    }
    value
}

#[cfg(unix)]
pub fn home_is_private(home: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(home).is_ok_and(|meta| meta.permissions().mode() & 0o077 == 0)
}

#[cfg(not(unix))]
pub fn home_is_private(home: &Path) -> bool {
    home.is_dir()
}
