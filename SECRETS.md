# Secrets (sops + age)

This repo uses `sops` with `age` to store an encrypted `.env.sops` file in git.

Guardrails:
- Never commit plaintext `.env`.
- Never print the contents of `%APPDATA%\\sops\\age\\keys.txt` (it contains your private key).
- Avoid commands like `type %APPDATA%\\sops\\age\\keys.txt` or `cat` on decrypted `.env` output.

## One-time Setup (Windows)

1. Install tools (requires winget):

```powershell
winget install --id Mozilla.SOPS -e --source winget --accept-package-agreements --accept-source-agreements
winget install --id FiloSottile.age -e --source winget --accept-package-agreements --accept-source-agreements
```

2. Ensure your age key exists (do not print it):

`%APPDATA%\\sops\\age\\keys.txt`

Generate it if missing (do not print output):

```powershell
$keysDir = Join-Path $env:APPDATA 'sops\\age'
$keysPath = Join-Path $keysDir 'keys.txt'
New-Item -ItemType Directory -Force -Path $keysDir | Out-Null
age-keygen -o $keysPath *> $null
```

## Encrypting `.env` to `.env.sops`

`sops` dotenv parsing is strict; comments/blank lines are not supported as input.
When creating `.env.sops`, only `KEY=VALUE` assignment lines should be included.

Current workflow used in this repo:
1. Create/update `.env` locally (plaintext, untracked/ignored).
2. Create `.env.sops` from the assignment lines and encrypt it in-place with `sops`.

## Decrypt Test / Verification

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\\verify_secrets.ps1
```

This script:
- Verifies `sops` and `age` are present.
- Verifies `%APPDATA%\\sops\\age\\keys.txt` exists (without printing it).
- Decrypt-tests `.env.sops` in-memory (never prints decrypted env).
- Checks that all keys in `.env.example` exist in the decrypted env (only key names are reported).

## Using Multiple Machines (PC + Laptop)

`.env.sops` can only be decrypted by a machine that has the matching **age private key** for one of its recipients.

Options:
1. Use the same key on both machines:
   - Securely copy `%APPDATA%\\sops\\age\\keys.txt` from the PC to the laptop (do not print it; do not commit it).
2. Use separate keys per machine:
   - Generate a key on the laptop, extract its **public** recipient, then add it to `.env.sops`:

```powershell
sops --add-age <LAPTOP_AGE_RECIPIENT> --in-place .env.sops
```


