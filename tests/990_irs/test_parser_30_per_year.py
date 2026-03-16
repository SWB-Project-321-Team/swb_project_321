"""
Fetch 30 sample XMLs from each year's first S3 ZIP and test the parser (regex + DOM) on them.
Reports pass/fail per year. Run from repo root.

  python tests/990_irs/test_parser_30_per_year.py
  python tests/990_irs/test_parser_30_per_year.py --years 2022 2023 2024
"""
import subprocess
import sys
from pathlib import Path

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))
from utils.paths import DATA

SAMPLE_DIR = DATA / "sample_990_xml"
FETCH_SCRIPT = _FILE_DIR / "fetch_sample_xml_and_test_parser.py"


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Test parser on 30 XMLs per year")
    parser.add_argument("--years", type=int, nargs="+", default=[2021, 2022, 2023, 2024], help="Years to test")
    parser.add_argument("--count", type=int, default=30, help="XMLs per year (default 30)")
    args = parser.parse_args()

    if not FETCH_SCRIPT.exists():
        print(f"Missing {FETCH_SCRIPT}", file=sys.stderr)
        sys.exit(1)

    sample_dir = str(SAMPLE_DIR)
    all_ok = True
    for year in args.years:
        print(f"\n--- Year {year}: fetch {args.count} XMLs ---")
        r = subprocess.run(
            [sys.executable, str(FETCH_SCRIPT), "--year", str(year), "--count", str(args.count), "--no-examine"],
            cwd=str(_REPO_ROOT),
        )
        if r.returncode != 0:
            print(f"  FAIL: fetch for {year}", file=sys.stderr)
            all_ok = False
            continue
        print(f"\n--- Year {year}: test parser on saved files ---")
        r2 = subprocess.run(
            [sys.executable, str(FETCH_SCRIPT), "--sample-dir", sample_dir],
            cwd=str(_REPO_ROOT),
        )
        if r2.returncode != 0:
            print(f"  FAIL: parser test for {year}", file=sys.stderr)
            all_ok = False
        else:
            print(f"  OK: {year} ({args.count} files)")
    print("\n" + ("All years passed." if all_ok else "Some years failed."))
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
