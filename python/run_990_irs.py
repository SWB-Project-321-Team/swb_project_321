"""
Run 990_irs ingest steps in order: 01 (index) → 02 (ZIPs to S3) → 03 (parse to staging).

Run from repo root: python python/run_990_irs.py

Pass-through: any extra args are forwarded to step 02 and 03 where applicable
(e.g. --start-year, --end-year). Step 01 is run with defaults only.
For full control, run the three scripts manually; see ingest/990_irs/README.md.
"""

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = REPO_ROOT / "python"
INGEST_990 = PYTHON / "ingest" / "990_irs"

STEP_01 = INGEST_990 / "01_upload_irs_990_index_to_s3.py"
STEP_02 = INGEST_990 / "02_upload_irs_990_zips_to_s3.py"
STEP_03 = INGEST_990 / "03_parse_irs_990_zips_to_staging.py"


def main():
    # Pass-through args (e.g. --start-year 2022 --end-year 2024)
    passthrough = sys.argv[1:]

    for name, script in [
        ("01_upload_irs_990_index_to_s3", STEP_01),
        ("02_upload_irs_990_zips_to_s3", STEP_02),
        ("03_parse_irs_990_zips_to_staging", STEP_03),
    ]:
        if not script.exists():
            print(f"Script not found: {script}", file=sys.stderr)
            sys.exit(1)
        cmd = [sys.executable, str(script)]
        if name != "01_upload_irs_990_index_to_s3":
            cmd.extend(passthrough)
        print(f"Running {name}...")
        rc = subprocess.call(cmd, cwd=str(REPO_ROOT))
        if rc != 0:
            print(f"Step {name} failed with exit code {rc}", file=sys.stderr)
            sys.exit(rc)
    print("990_irs pipeline (01 → 02 → 03) completed.")


if __name__ == "__main__":
    main()
