# Local OneDrive setup

1. Sync the shared Project 321 folder through OneDrive.
2. Keep the Git repository outside the synced data directory; code belongs in Git and active project data belongs in OneDrive. The former project S3 bucket was intentionally deleted and must not be recreated.
3. Copy `secrets/.env.example` to `secrets/.env` and set environment-specific paths locally. The resulting `.env` file is ignored by Git.
4. Confirm the configured data root follows the layout documented in [structure.md](structure.md).
5. Run a small pipeline or test command from the repository root before starting a full data refresh.

Do not commit local absolute paths, credentials, downloaded source data, or generated analysis extracts.
