import { execFile } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { env, platform } from "node:process";
import { promisify } from "node:util";
import type { CredentialRequest } from "./types.js";

const SERVICE = "stql";
const PREFIX = "os:";
const MAX_TARGET_LENGTH = 8_192;
const MAX_PAYLOAD_LENGTH = 16_384;
const MAX_PASSWORD_LENGTH = 4_096;
const FINGERPRINT_PATTERN = /^[0-9a-f]{64}$/u;
const PROTOCOLS = ["postgres:", "mysql:", "mongodb:", "mongodb+srv:", "redis:", "rediss:"];
const TARGET_OVERRIDE_KEYS = new Set(["password", "pwd", "user", "username", "host", "port", "database", "dbname"]);
// TLS settings are connection options, not destination identity. pylon_tls_ca is Pylon's CA preset.
const TLS_QUERY_KEYS = new Set([
  "sslmode",
  "ssl",
  "sslrootcert",
  "sslcert",
  "sslkey",
  "rejectunauthorized",
  "uselibpqcompat",
  "sslnegotiation",
  "pylon_tls_ca",
]);
const run = promisify(execFile);

// Same record as Pylon's v2 vault entries, so both tools read one format.
interface StoredPassword {
  version: 2;
  fingerprint: string;
  password: string;
  stale?: true;
}

export function isOsPasswordRef(reference: string | null | undefined): reference is string {
  return reference?.startsWith(PREFIX) ?? false;
}

export function createOsPasswordRef(): string {
  return `${PREFIX}${randomUUID()}`;
}

export async function cliCredentialResolver(request: CredentialRequest): Promise<string | undefined> {
  if (request.source === "credential_ref") return undefined;
  if (request.source !== "password_ref") return env[request.reference];
  return readOsPassword(request.reference, request.target);
}

export async function saveOsPassword(reference: string, target: string, password: string): Promise<void> {
  if (!isOsPasswordRef(reference)) throw new Error("Password reference must start with os:.");
  if (!validPassword(password)) throw new Error("Password is too long or contains control characters.");
  const fingerprint = targetFingerprint(target);
  if (!fingerprint) throw new Error("Cannot save a password for this target.");
  const stored: StoredPassword = { version: 2, fingerprint, password };
  await (await osEntry(reference)).setPassword(JSON.stringify(stored));
}

/** Returns the password only when it was saved for the same destination as `target`. */
export async function readOsPassword(reference: string, target: string): Promise<string | undefined> {
  if (!isOsPasswordRef(reference)) return undefined;
  const value = await (await osEntry(reference)).getPassword();
  if (value === null) return undefined;
  if (!isRecord(value)) return value; // Raw password saved before passwords were bound to a target.
  const stored = storedPassword(value);
  if (!stored || stored.stale) return undefined;
  return stored.fingerprint === targetFingerprint(target) ? stored.password : undefined;
}

/** Marks a saved password as rejected by the database, so it is not used again. */
export async function invalidateOsPassword(reference: string): Promise<boolean> {
  if (!isOsPasswordRef(reference)) return false;
  const entry = await osEntry(reference);
  const stored = storedPassword((await entry.getPassword()) ?? "");
  if (!stored) return false;
  await entry.setPassword(JSON.stringify({ ...stored, stale: true }));
  return true;
}

export async function deleteOsPassword(reference: string | null | undefined): Promise<void> {
  if (!isOsPasswordRef(reference)) return;
  await (await osEntry(reference)).deleteCredential();
}

async function osEntry(reference: string) {
  const { AsyncEntry } = await import("@napi-rs/keyring");
  // The keyutils fallback loses saved passwords after reboot.
  return new AsyncEntry(SERVICE, reference.slice(PREFIX.length), { linux: { store: "secret-service" } });
}

function isRecord(value: string): boolean {
  try {
    const parsed: unknown = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null && "version" in parsed;
  } catch {
    return false;
  }
}

function storedPassword(value: string): StoredPassword | undefined {
  if (!value || value.length > MAX_PAYLOAD_LENGTH) return undefined;
  let record: Record<string, unknown>;
  try {
    const parsed: unknown = JSON.parse(value);
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return undefined;
    record = parsed as Record<string, unknown>;
  } catch {
    return undefined;
  }
  const knownKeys = Object.keys(record).every((key) => ["version", "fingerprint", "password", "stale"].includes(key));
  if (!knownKeys || record.version !== 2) return undefined;
  if (typeof record.fingerprint !== "string" || !FINGERPRINT_PATTERN.test(record.fingerprint)) return undefined;
  if (!validPassword(record.password)) return undefined;
  if (record.stale !== undefined && record.stale !== true) return undefined;
  return {
    version: 2,
    fingerprint: record.fingerprint,
    password: record.password,
    ...(record.stale ? { stale: true as const } : {}),
  };
}

function validPassword(password: unknown): password is string {
  return typeof password === "string" && password.length <= MAX_PASSWORD_LENGTH && !hasControlCharacters(password);
}

function hasControlCharacters(value: string): boolean {
  return /[\u0000-\u001f\u007f]/u.test(value);
}

function targetFingerprint(target: string): string | undefined {
  const unsafeTarget = !target || target.length > MAX_TARGET_LENGTH || hasControlCharacters(target);
  if (unsafeTarget || /%(?![0-9a-f]{2})/iu.test(target)) return undefined;
  // WHATWG URL rejects multi-host MongoDB targets; bind those to the exact target instead.
  const identity = parsesAsUrl(target) ? destinationIdentity(target) : target;
  return identity ? createHash("sha256").update(identity).digest("hex") : undefined;
}

function parsesAsUrl(target: string): boolean {
  try {
    new URL(target);
    return true;
  } catch {
    return false;
  }
}

function destinationIdentity(target: string): string | undefined {
  const url = new URL(target);
  const protocol = url.protocol.toLowerCase() === "postgresql:" ? "postgres:" : url.protocol.toLowerCase();
  if (!PROTOCOLS.includes(protocol) || !url.hostname || url.hash || url.password) return undefined;
  const username = decodedPart(url.username);
  const database = decodedPart(url.pathname);
  if (username === undefined || database === undefined) return undefined;

  const query: [string, string][] = [];
  for (const [key, value] of url.searchParams.entries()) {
    const normalizedKey = key.toLowerCase();
    if (hasControlCharacters(key) || hasControlCharacters(value)) return undefined;
    if (TARGET_OVERRIDE_KEYS.has(normalizedKey)) return undefined;
    if (!TLS_QUERY_KEYS.has(normalizedKey)) query.push([key, value]);
  }
  query.sort(([leftKey, leftValue], [rightKey, rightValue]) =>
    leftKey === rightKey ? leftValue.localeCompare(rightValue) : leftKey.localeCompare(rightKey));
  return JSON.stringify({
    protocol,
    username,
    hostname: url.hostname.toLowerCase().replace(/\.$/u, ""),
    port: url.port || defaultPort(protocol),
    database,
    query,
  });
}

function decodedPart(value: string): string | undefined {
  try {
    const decoded = decodeURIComponent(value);
    return hasControlCharacters(decoded) ? undefined : decoded;
  } catch {
    return undefined;
  }
}

function defaultPort(protocol: string): string {
  if (protocol === "postgres:") return "5432";
  if (protocol === "mysql:") return "3306";
  if (protocol === "mongodb:") return "27017";
  if (protocol === "redis:" || protocol === "rediss:") return "6379";
  return "";
}

export async function askPassword(profile: string, target: string): Promise<string> {
  const url = new URL(target);
  const user = decodeURIComponent(url.username);
  const message = `Password for ${user ? `${user}@` : ""}${url.host} (StateQL profile "${profile}")`;
  let output: string;
  try {
    output = await passwordDialog(message);
  } catch (error) {
    if (isMissingCommand(error)) {
      throw new Error("--ask-password needs a password dialog: zenity on Linux, osascript on macOS.");
    }
    throw new Error("Password entry was cancelled.");
  }
  const password = platform === "win32" ? output : output.replace(/\r?\n$/, "");
  if (password === "") throw new Error("No password was entered.");
  return password;
}

async function passwordDialog(message: string): Promise<string> {
  if (platform === "win32") {
    // Get-Credential opens behind the active window; a TopMost form stays visible.
    // windowsHide makes the first ShowWindow hide the form, so show it again explicitly.
    const script = [
      "$ErrorActionPreference = 'Stop'",
      "[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false",
      "Add-Type -AssemblyName System.Windows.Forms",
      "Add-Type -Namespace StateQL -Name Win32 -MemberDefinition '[DllImport(\"user32.dll\")] public static extern bool ShowWindow(IntPtr window, int command);'",
      "[Windows.Forms.Application]::EnableVisualStyles()",
      "$form = New-Object Windows.Forms.Form -Property @{ Text = 'StateQL'; TopMost = $true; StartPosition = 'CenterScreen'; FormBorderStyle = 'FixedDialog'; MaximizeBox = $false; MinimizeBox = $false; AutoSize = $true; AutoSizeMode = 'GrowAndShrink'; Padding = 12 }",
      "$layout = New-Object Windows.Forms.FlowLayoutPanel -Property @{ FlowDirection = 'TopDown'; AutoSize = $true }",
      "$label = New-Object Windows.Forms.Label -Property @{ Text = $env:STQL_PROMPT_MESSAGE; AutoSize = $true; MaximumSize = '360,0' }",
      "$box = New-Object Windows.Forms.TextBox -Property @{ UseSystemPasswordChar = $true; Width = 360 }",
      "$buttons = New-Object Windows.Forms.FlowLayoutPanel -Property @{ FlowDirection = 'RightToLeft'; Width = 366; Height = 34 }",
      "$cancel = New-Object Windows.Forms.Button -Property @{ Text = 'Cancel'; DialogResult = 'Cancel' }",
      "$ok = New-Object Windows.Forms.Button -Property @{ Text = 'OK'; DialogResult = 'OK' }",
      "$buttons.Controls.AddRange(@($cancel, $ok))",
      "$layout.Controls.AddRange(@($label, $box, $buttons))",
      "$form.Controls.Add($layout)",
      "$form.AcceptButton = $ok",
      "$form.CancelButton = $cancel",
      "$form.Add_Shown({ [void][StateQL.Win32]::ShowWindow($form.Handle, 5); $form.Activate(); [void]$box.Focus() })",
      "if ($form.ShowDialog() -ne 'OK') { exit 1 }",
      "[Console]::Out.Write($box.Text)",
    ].join("\n");
    const { stdout } = await run(
      "powershell.exe",
      ["-NoProfile", "-EncodedCommand", Buffer.from(script, "utf16le").toString("base64")],
      { env: { ...env, STQL_PROMPT_MESSAGE: message }, windowsHide: true },
    );
    return stdout;
  }
  if (platform === "darwin") {
    const { stdout } = await run("osascript", [
      "-e", "on run argv",
      "-e", 'text returned of (display dialog (item 1 of argv) default answer "" with hidden answer with title "StateQL")',
      "-e", "end run",
      message,
    ]);
    return stdout;
  }
  const { stdout } = await run("zenity", ["--password", "--title", message]);
  return stdout;
}

function isMissingCommand(error: unknown): boolean {
  return (error as NodeJS.ErrnoException).code === "ENOENT";
}
