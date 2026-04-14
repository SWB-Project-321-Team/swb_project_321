# Secrets

This folder holds credentials and is **gitignored** (except this README and `.gitkeep`). Do not commit real keys.

## Files

- **`.env.example`** – **Template for teammates.** Copy to `.env` and replace the placeholders with the Access key ID and Secret access key you received from the admin. Do not commit `.env`.
- **`.env`** – Your actual credentials (create from `.env.example`). Environment variables for AWS (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`). Use with scripts or load before running `aws` CLI.
- **`aws_credentials.ini`** – Standard AWS CLI credentials file. Optionally copy to `%USERPROFILE%\.aws\credentials` (Windows) or set `AWS_SHARED_CREDENTIALS_FILE` to this path.

## Using the credentials

**Option A – AWS CLI (one-time):** Run `aws configure` and paste the access key and secret from `.env` or the original CSV.

**Option B – Use `.env` in a shell (PowerShell):**
```powershell
Get-Content secrets\.env | ForEach-Object {
  if ($_ -match '^([^#=]+)=(.*)$') { [Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process') }
}
aws sts get-caller-identity
```

**Option C – Python:** Use `python-dotenv`: `load_dotenv("secrets/.env")` then read `os.environ["AWS_ACCESS_KEY_ID"]` etc., or rely on boto3’s default credential chain (after `aws configure` or with env vars set).

## Console sign-in

The CSV with **User name**, **Password**, and **Console sign-in URL** is for browser login only. Use the URL in that file to sign in to the AWS Management Console; no conversion needed.

## IAM Identity Center (SSO)

Organization instance ID: `668458fe89487567` (stored in `aws_account_info.txt`).

**Next steps to use SSO with the CLI:**

1. **Get your SSO start URL** – IAM Identity Center → Dashboard or Settings. It looks like `https://d-xxxxxxxxxx.awsapps.com/start` (the `d-...` is your instance).
2. **Assign access** – Identity Center → AWS accounts → your account (416754239293) → Assign users or groups. Add yourself (and teammates) and choose a permission set (e.g. AdministratorAccess or a custom S3 policy).
3. **Configure the CLI for SSO:**
   ```bash
   aws configure sso
   ```
   Enter the SSO start URL, SSO region (e.g. `us-east-2`), and when prompted sign in in the browser. After that, use the CLI as usual; run `aws sso login` when the session expires.
4. **Optional:** Once SSO is working, you can stop using long-term access keys for day-to-day use (keep them as a backup or for automation if needed).
