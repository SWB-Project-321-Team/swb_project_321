"""
Tests for combined filtered 990 source-union helpers.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_COMBINED_DIR = _REPO_ROOT / "python" / "ingest" / "combined_990"
_COMMON_PATH = _COMBINED_DIR / "common.py"
_COMMON_SPEC = importlib.util.spec_from_file_location("combined_990_common", _COMMON_PATH)
if _COMMON_SPEC is None or _COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load combined common module from {_COMMON_PATH}")
combined_common = importlib.util.module_from_spec(_COMMON_SPEC)
sys.modules.setdefault("combined_990_common", combined_common)
_COMMON_SPEC.loader.exec_module(combined_common)

_BUILD_PATH = _COMBINED_DIR / "01_build_combined_filtered_local.py"
_BUILD_SPEC = importlib.util.spec_from_file_location("combined_build_local", _BUILD_PATH)
if _BUILD_SPEC is None or _BUILD_SPEC.loader is None:
    raise ImportError(f"Unable to load combined build module from {_BUILD_PATH}")
combined_build = importlib.util.module_from_spec(_BUILD_SPEC)
_previous_common = sys.modules.get("common")
sys.modules["common"] = combined_common
_BUILD_SPEC.loader.exec_module(combined_build)
if _previous_common is not None:
    sys.modules["common"] = _previous_common
else:
    sys.modules.pop("common", None)


def test_find_latest_core_inputs_excludes_private_foundation(tmp_path: Path) -> None:
    root = tmp_path / "core"
    year_dir = root / "year=2022"
    year_dir.mkdir(parents=True)
    for name in (
        "CORE-2022-501C3-CHARITIES-PC-HRMN__benchmark.csv",
        "CORE-2022-501C3-CHARITIES-PZ-HRMN__benchmark.csv",
        "CORE-2022-501CE-NONPROFIT-PC-HRMN__benchmark.csv",
        "CORE-2022-501CE-NONPROFIT-PZ-HRMN__benchmark.csv",
        "CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv",
    ):
        (year_dir / name).write_text("EIN2\n", encoding="utf-8")

    inputs = combined_common._find_latest_core_inputs(root)
    assert len(inputs) == 4
    assert all("privfound" not in item.source_variant for item in inputs)


def test_parse_numeric_string_handles_commas_parentheses_scientific_and_invalid() -> None:
    assert combined_common.parse_numeric_string("1,234.50") == (1234.5, True)
    assert combined_common.parse_numeric_string("(42)") == (-42.0, True)
    assert combined_common.parse_numeric_string("1.2E+03") == (1200.0, True)
    assert combined_common.parse_numeric_string("") == (None, True)
    assert combined_common.parse_numeric_string("not-a-number") == (None, False)


def test_transform_bmf_sets_harm_tax_year_from_snapshot_year() -> None:
    source = combined_common.SourceInput(
        source_family="nccs_bmf",
        source_variant="raw_bmf_2025",
        input_path=Path("dummy.parquet"),
        input_format="parquet",
        snapshot_year="2025",
        snapshot_month="",
        native_prefix=combined_common.SOURCE_PREFIXES["nccs_bmf"],
    )
    df = pd.DataFrame(
        [
            {
                "EIN": "123456789",
                "NAME": "Bench Org",
                "STATE": "SD",
                "ZIP": "57701",
                "SUBSECTION": "3",
                "NTEE_CD": "P20",
                "REVENUE_AMT": "100",
                "INCOME_AMT": "90",
                "ASSET_AMT": "80",
                "county_fips": "46093",
                "region": "BlackHills",
                "bmf_snapshot_year": "2025",
                "bmf_snapshot_month": "2025-12",
            }
        ]
    )
    transformed, manifest = combined_build._transform_bmf(source, df)
    assert transformed["harm_tax_year"].tolist() == ["2025"]
    assert transformed["row_source_time_basis"].tolist() == ["reference_snapshot_year"]
    assert manifest["min_tax_year"] == "2025"


def test_apply_bmf_overlay_only_on_exact_ein_and_tax_year_match() -> None:
    df = pd.DataFrame(
        [
            {
                "row_source_family": "nccs_core",
                "harm_ein": "123456789",
                "harm_tax_year": "2022",
                "harm_org_name": "",
                "harm_org_name__source_family": "",
                "harm_org_name__source_variant": "",
                "harm_org_name__source_column": "",
                "harm_state": "",
                "harm_state__source_family": "",
                "harm_state__source_variant": "",
                "harm_state__source_column": "",
                "harm_zip5": "",
                "harm_zip5__source_family": "",
                "harm_zip5__source_variant": "",
                "harm_zip5__source_column": "",
                "harm_ntee_code": "",
                "harm_ntee_code__source_family": "",
                "harm_ntee_code__source_variant": "",
                "harm_ntee_code__source_column": "",
                "harm_subsection_code": "",
                "harm_subsection_code__source_family": "",
                "harm_subsection_code__source_variant": "",
                "harm_subsection_code__source_column": "",
            },
            {
                "row_source_family": "nccs_core",
                "harm_ein": "123456789",
                "harm_tax_year": "2021",
                "harm_org_name": "",
                "harm_org_name__source_family": "",
                "harm_org_name__source_variant": "",
                "harm_org_name__source_column": "",
                "harm_state": "",
                "harm_state__source_family": "",
                "harm_state__source_variant": "",
                "harm_state__source_column": "",
                "harm_zip5": "",
                "harm_zip5__source_family": "",
                "harm_zip5__source_variant": "",
                "harm_zip5__source_column": "",
                "harm_ntee_code": "",
                "harm_ntee_code__source_family": "",
                "harm_ntee_code__source_variant": "",
                "harm_ntee_code__source_column": "",
                "harm_subsection_code": "",
                "harm_subsection_code__source_family": "",
                "harm_subsection_code__source_variant": "",
                "harm_subsection_code__source_column": "",
            },
            {
                "row_source_family": "nccs_bmf",
                "harm_ein": "123456789",
                "harm_tax_year": "2022",
                "harm_org_name": "Bench Org",
                "harm_org_name__source_family": "nccs_bmf",
                "harm_org_name__source_variant": "legacy_bmf_2022",
                "harm_org_name__source_column": "NAME",
                "harm_state": "SD",
                "harm_state__source_family": "nccs_bmf",
                "harm_state__source_variant": "legacy_bmf_2022",
                "harm_state__source_column": "STATE",
                "harm_zip5": "57701",
                "harm_zip5__source_family": "nccs_bmf",
                "harm_zip5__source_variant": "legacy_bmf_2022",
                "harm_zip5__source_column": "ZIP5",
                "harm_ntee_code": "P20",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "legacy_bmf_2022",
                "harm_ntee_code__source_column": "NTEEFINAL",
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "legacy_bmf_2022",
                "harm_subsection_code__source_column": "SUBSECCD",
            },
        ]
    )

    overlaid = combined_build._apply_bmf_overlay(df)
    assert overlaid.loc[0, "harm_org_name"] == "Bench Org"
    assert bool(overlaid.loc[0, "bmf_overlay_applied"]) is True
    assert overlaid.loc[0, "bmf_overlay_field_count"] >= 1
    assert overlaid.loc[1, "harm_org_name"] == ""
    assert bool(overlaid.loc[1, "bmf_overlay_applied"]) is False


def test_write_parquet_with_metadata_sets_schema_version(tmp_path: Path) -> None:
    out_path = tmp_path / "combined.parquet"
    combined_common.write_parquet_with_metadata(
        pd.DataFrame({"row_source_family": ["givingtuesday_datamart"]}),
        out_path,
    )
    meta = pq.read_schema(out_path).metadata
    assert meta[b"combined_schema_version"] == combined_common.COMBINED_SCHEMA_VERSION.encode("utf-8")
