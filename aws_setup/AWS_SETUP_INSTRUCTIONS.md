# Setup: AWS CLI, credentials, and S3 bucket

Setup for using the project S3 bucket **swb-321-irs990-teos** (list, download, upload, run project scripts) with the **IAM user swb-321-team** access key.

**Credentials:** The project admin will give you **everything in the `secrets` folder** (e.g. via a zip or shared folder). Those files are **not in the repo** (they are gitignored). Copy the contents into your repo’s **`secrets/`** folder at the repo root. The **`.env`** file in there has the access key for the bucket; do not commit it.

**Outcome:** AWS CLI installed; credentials in **`secrets/.env`** (for scripts) and optionally in AWS CLI default profile; ability to run `aws s3` and 990_irs scripts.

---

## Setup checklist

1. **Clone the repo** (section 1).
2. **Get the secrets folder** from the project admin (they will give you everything in the `secrets` folder).
3. **Put the secrets in place:** Copy the contents of the folder the admin gave you into your repo’s **`secrets/`** folder (the folder at the repo root, next to `python/` and `aws_setup/`). You should have **`secrets/.env`** there so project scripts can find the access key.
4. **Optional for CLI:** Run **`aws configure`** and enter the Access key ID and Secret access key from **`secrets/.env`** (or from the CSV in `secrets/`); region **us-east-2** (section 6 Option B).
5. **Verify:** Run **`aws sts get-caller-identity`** and **`aws s3 ls s3://swb-321-irs990-teos/`** (section 7).

---

## Table of contents

- [1. Get the repo locally](#1-get-the-repo-locally)
- [2. Where to run commands](#2-where-to-run-commands)
- [3. Prerequisites](#3-prerequisites)
- [4. Install the AWS CLI](#4-install-the-aws-cli)
- [5. Credentials (secrets folder from admin)](#5-credentials-secrets-folder-from-admin)
- [6. Set up credentials](#6-set-up-credentials)
- [7. Verify](#7-verify)
- [8. Using the S3 bucket](#8-using-the-s3-bucket)
- [9. Troubleshooting](#9-troubleshooting)
- [10. Security](#10-security)
- [Quick reference](#quick-reference)

---

## 1. Get the repo locally

Clone or pull the GitHub repo to your machine first. The `aws_setup` folder and these instructions are in the repo. Credentials are **not** in the repo; you receive them from the project admin (see section 5).

**Option A – GitHub Desktop**

- **First time:** Open GitHub Desktop → **File** → **Clone repository** → choose the repo (or enter the URL) and a local path → **Clone**. Then open the repo folder in File Explorer or Finder.
- **Already cloned:** Open GitHub Desktop, select the repo, click **Fetch origin** then **Pull origin** to get the latest (including any updates to `aws_setup`).

**Option B – Command line (git)**

- **First time:** `git clone <repo-url>` then `cd` into the repo folder.
- **Already cloned:** `git pull` from the repo folder to get the latest (including any updates to `aws_setup`).

---

## 2. Where to run commands

**Repo root** = the folder that contains `python/`, `secrets/`, and `aws_setup/`. In a terminal, `cd` into that folder before running the steps below.

- **AWS CLI** (`aws s3 ls`, `aws s3 cp`, etc.): Can be run from **any directory** if credentials are set with `aws configure`. Commands that use local paths (e.g. `./myfile.csv`) must be run from the folder that has that file, or use the full path to the file.
- **Project Python scripts** (e.g. `python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py`): Must be run from **repo root**, so the script finds `secrets/.env` and other project paths.

To get to repo root: open a terminal, `cd` to the project folder (e.g. `cd C:\Users\...\swb_project_321` or `cd ~/swb_project_321`).

---

## 3. Prerequisites

- Repo clone or pull.
- **Secrets folder** from the project admin (contents of `secrets/`, including `.env` with the **swb-321-team** access key).
- Python 3 for project scripts; `pip install boto3 pandas requests` for 990_irs.

---

## 4. Install the AWS CLI

Install AWS CLI v2. Then run `aws --version` (expect `aws-cli/2.x.x`). Restart the terminal after install.

### Windows

**MSI (recommended):** Download https://awscli.amazonaws.com/AWSCLIV2.msi, run installer, accept defaults.

**winget:** `winget install Amazon.AWSCLI`

### macOS

**pkg:** Download https://awscli.amazonaws.com/AWSCLIV2.pkg, run installer.

**Homebrew:** `brew install awscli`

### Linux

```bash
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip -q awscliv2.zip
sudo ./aws/install
```

**Package manager:** Ubuntu/Debian `sudo apt install awscli`; Fedora `sudo dnf install awscli` (may be v1; v2 preferred).

---

## 5. Credentials (secrets folder from admin)

The project admin will give you **everything in the `secrets` folder** (e.g. a zip or a copy of the folder). It includes:

- **`.env`** – contains `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_DEFAULT_REGION` for the IAM user **swb-321-team**. Project scripts read this from **`secrets/.env`** at the repo root.
- Optionally a **CSV** with the same access key (e.g. `swb-321-team_accessKeys.csv`), if you prefer to run **`aws configure`** and type the values.

**What you do:** Copy the contents of the folder the admin gave you into your repo’s **`secrets/`** folder (at the repo root, next to `python/` and `aws_setup/`). After that, **`secrets/.env`** should exist and scripts will use it.

Do not commit the contents of **`secrets/`** or paste the secret key into chat, email, or git. Use them only for this project and for the bucket **`swb-321-irs990-teos`**.

---

## 6. Set up credentials

Use one or both options. **Option A** for project Python scripts; **Option B** for AWS CLI. Both is recommended.

### Option A: `secrets/.env` (for project scripts)

Scripts in `python/ingest/990_irs/` read **`secrets/.env`** at the repo root.

1. **Copy the secrets folder** the admin gave you into your repo so that **`secrets/.env`** exists at the repo root (folder **`secrets/`** next to `python/` and `aws_setup/`). The admin’s folder should already contain **`.env`**.
2. **Confirm gitignore.** From repo root run `git status`; files under `secrets/` (e.g. `secrets/.env`) must not appear. If they do, do not commit.

### Option B: AWS CLI (`aws configure`)

Configures the default profile so `aws` commands use the key without loading `secrets/.env`.

1. Run `aws configure`.
2. When prompted: enter the **Access key ID** and **Secret access key** from **`secrets/.env`** (or from the CSV in **`secrets/`**). Default region: `us-east-2`. Output format: Enter to skip or `json`.

Stored in `%USERPROFILE%\.aws\` (Windows) or `~/.aws/` (macOS/Linux). Do not commit the `.aws` folder.

### Using both

- Project scripts load `secrets/.env`; they do not require `aws configure`.
- AWS CLI uses the default profile from `aws configure` (or env vars in the same shell).
- Recommended: `secrets/.env` for scripts, `aws configure` for CLI.

---

## 7. Verify

From repo root for Python commands.

**Identity:** `aws sts get-caller-identity`  
Expect JSON with `UserId`, `Account`, `Arn`. If "Unable to locate credentials", run `aws configure` or ensure `secrets/.env` is present and script is run from repo root.

**List bucket:** `aws s3 ls s3://swb-321-irs990-teos/`  
Expect top-level prefixes (e.g. `bronze/`). "Access Denied" or "NoSuchBucket" means wrong credentials or bucket name; verify key and region.

**List prefix (optional):** `aws s3 ls s3://swb-321-irs990-teos/bronze/irs990/teos_xml/`

**Script sees credentials:** `python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py --help`  
Expect help. On a real run the script prints that it loaded from `secrets/.env`. "No secrets/.env found" means copy the secrets folder from the admin into your repo’s **`secrets/`** folder (section 6 Option A).

---

## 8. Using the S3 bucket

### AWS CLI

- List: `aws s3 ls s3://swb-321-irs990-teos/` or add `--recursive` and a prefix.
- Download: `aws s3 cp s3://swb-321-irs990-teos/path/to/file.csv ./`
- Upload: `aws s3 cp ./myfile.csv s3://swb-321-irs990-teos/bronze/irs990/teos_xml/myfile.csv`
- Sync: `aws s3 sync ./local_folder s3://swb-321-irs990-teos/some/prefix/`

Region `us-east-2` is used when set in `.env` or `aws configure`.

### Project scripts

From repo root. Scripts load `secrets/.env` automatically.

- Upload index: `python python/ingest/990_irs/01_upload_irs_990_index_to_s3.py`
- Upload ZIPs: `python python/ingest/990_irs/02_upload_irs_990_zips_to_s3.py`
- Parse to staging: `python python/ingest/990_irs/03_parse_irs_990_zips_to_staging.py` (after index and ZIPs; see 990_irs README for order and GEOID).

Default bucket **`swb-321-irs990-teos`**, prefix **`bronze/irs990/teos_xml`**. Override via env or script args. Full pipeline: **`python/ingest/990_irs/README.md`**.

---

## 9. Troubleshooting

| Problem | Action |
|--------|--------|
| `aws: command not found` | Install AWS CLI (section 4). Restart terminal. |
| `Unable to locate credentials` | Run `aws configure` or ensure `secrets/.env` exists and script runs from repo root. |
| `Access Denied` on S3 | Verify shared key for **swb-321-irs990-teos**, region **us-east-2**. |
| `NoSuchBucket` | Check bucket name and region. |
| "No secrets/.env found" | Copy the secrets folder from the admin into your repo’s **`secrets/`** folder (section 6 Option A); run from repo root. |
| Wrong region | Set `AWS_DEFAULT_REGION=us-east-2` in `secrets/.env` and/or `aws configure`. |

---

## 10. Security

- Do not commit `secrets/.env` or any file containing the Secret access key. Repo ignores `secrets/.env` and `secrets/*` except allowed files.
- Do not paste the Secret access key or full `.env` in chat, email, or Slack. Use a secure channel only.
- One shared key. After rotation or reissue, update `secrets/.env` and/or `aws configure` with the new credentials.
- Keys do not expire by default; they work until deactivated or rotated. If access stops, check whether the key was rotated.

---

## Quick reference

| Item | Value |
|------|--------|
| Bucket | `swb-321-irs990-teos` |
| Region | `us-east-2` |
| Credentials for scripts | `secrets/.env` (from the secrets folder the admin gave you) |
| Env vars | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` |
| CLI | `aws configure` (default profile) |
| 990_irs | `python/ingest/990_irs/README.md` |

