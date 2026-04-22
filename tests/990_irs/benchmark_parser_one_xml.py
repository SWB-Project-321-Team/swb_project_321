"""
Benchmark: time to parse a single 990 XML with our parser (regex-only, default DOM, iterparse).

Usage (from repo root):
  python tests/990_irs/benchmark_parser_one_xml.py
  python tests/990_irs/benchmark_parser_one_xml.py --xml path/to/file.xml
  python tests/990_irs/benchmark_parser_one_xml.py --rounds 100
"""

import argparse
import sys
import time
from pathlib import Path

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_PYTHON = _REPO_ROOT / "python"
_990_IRS_SRC = _REPO_ROOT / "python" / "ingest" / "990_irs"
if str(_PYTHON) not in sys.path:
    sys.path.insert(0, str(_PYTHON))
from utils.paths import DATA

from importlib.util import spec_from_file_location, module_from_spec
_spec = spec_from_file_location("parse_990", _990_IRS_SRC / "03_parse_irs_990_zips_to_staging.py")
_mod = module_from_spec(_spec)
_spec.loader.exec_module(_mod)
parse_990_xml = _mod.parse_990_xml


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark parser time per XML")
    ap.add_argument("--xml", type=Path, help="Path to one XML file (default: first in DATA/sample_990_xml)")
    ap.add_argument("--rounds", type=int, default=50, help="Rounds per mode (default 50)")
    args = ap.parse_args()

    if args.xml is not None:
        xml_path = args.xml
        if not xml_path.is_file():
            print(f"Not a file: {xml_path}", file=sys.stderr)
            sys.exit(1)
    else:
        sample_dir = DATA / "sample_990_xml"
        if not sample_dir.is_dir():
            print(f"Sample dir not found: {sample_dir}. Use --xml path/to/file.xml", file=sys.stderr)
            sys.exit(1)
        xmls = list(sample_dir.glob("*.xml"))
        if not xmls:
            print(f"No .xml in {sample_dir}. Use --xml path/to/file.xml", file=sys.stderr)
            sys.exit(1)
        xml_path = xmls[0]

    xml_bytes = xml_path.read_bytes()
    name = xml_path.name
    size_kb = len(xml_bytes) / 1024
    print(f"File: {xml_path} ({size_kb:.1f} KB)")
    print(f"Rounds per mode: {args.rounds}\n")

    def run_mode(use_regex_only: bool, use_iterparse: bool) -> float:
        # warmup
        for _ in range(3):
            parse_990_xml(xml_bytes, source_name=name, use_regex_only=use_regex_only, use_iterparse=use_iterparse)
        start = time.perf_counter()
        for _ in range(args.rounds):
            parse_990_xml(xml_bytes, source_name=name, use_regex_only=use_regex_only, use_iterparse=use_iterparse)
        elapsed = time.perf_counter() - start
        return (elapsed / args.rounds) * 1000  # ms per file

    ms_regex = run_mode(use_regex_only=True, use_iterparse=False)
    ms_dom = run_mode(use_regex_only=False, use_iterparse=False)
    ms_iter = run_mode(use_regex_only=False, use_iterparse=True)

    print("Mean time per XML (ms):")
    print(f"  --fast-parse (regex only): {ms_regex:.2f} ms")
    print(f"  default (DOM):             {ms_dom:.2f} ms")
    print(f"  --stream-parse (iterparse): {ms_iter:.2f} ms")


if __name__ == "__main__":
    main()
