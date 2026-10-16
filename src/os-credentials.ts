import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { env, platform } from "node:process";
import { promisify } from "node:util";
import type { CredentialRequest } from "./types.js";

const SERVICE = "stql";
const PREFIX = "os:";
const run = promisify(execFile);

export function isOsPasswordRef(reference: string | null | undefined): reference is string {
  return reference?.startsWith(PREFIX) ?? false;
}

export async function cliCredentialResolver(request: CredentialRequest): Promise<string | undefined> {
  if (request.source === "credential_ref") return undefined;
  if (request.source !== "password_ref") return env[request.reference];
  if (!isOsPasswordRef(request.reference)) return undefined;
  return (await osEntry(request.reference)).getPassword() ?? undefined;
}

export async function saveOsPassword(password: string): Promise<string> {
  const reference = `${PREFIX}${randomUUID()}`;
  (await osEntry(reference)).setPassword(password);
  return reference;
}

export async function deleteOsPassword(reference: string | null | undefined): Promise<void> {
  if (!isOsPasswordRef(reference)) return;
  (await osEntry(reference)).deletePassword();
}

async function osEntry(reference: string) {
  const { Entry } = await import("@napi-rs/keyring");
  return new Entry(SERVICE, reference.slice(PREFIX.length));
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
