"""Run parser on each sample form XML and print extracted row. Run from repo root."""
import sys
import importlib.util
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "python"))

spec = importlib.util.spec_from_file_location(
    "parse_990_module",
    REPO_ROOT / "python" / "ingest" / "990_irs" / "03_parse_irs_990_zips_to_staging.py",
)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
parse_990_xml = m.parse_990_xml

SAMPLES = REPO_ROOT / "data" / "sample_990_xml_by_form"

def main():
    for form in ("990", "990EZ", "990PF", "990T"):
        path = SAMPLES / f"{form}.xml"
        if not path.exists():
            print(f"Skip {form}: not found")
            continue
        xml_bytes = path.read_bytes()
        row = parse_990_xml(xml_bytes, source_name=path.name, use_regex_only=False)
        print(f"\n=== {form} ({path.name}) ===")
        if not row:
            print("  Parser returned None")
            continue
        for k, v in sorted(row.items()):
            if v is not None and v != "":
                print(f"  {k}: {v}")
        blanks = [k for k, v in sorted(row.items()) if v is None or v == ""]
        if blanks:
            print(f"  (blank: {', '.join(blanks)})")

if __name__ == "__main__":
    main()
