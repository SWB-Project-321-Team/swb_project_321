"""
Tests for combined filtered 990 source-union helpers.
"""

from __future__ import annotations

import importlib.util
import json
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


def test_find_efile_inputs_discovers_annualized_outputs(tmp_path: Path) -> None:
    root = tmp_path / "nccs_efile"
    for tax_year in (2021, 2022, 2023, 2024):
        year_dir = root / f"tax_year={tax_year}"
        year_dir.mkdir(parents=True)
        (year_dir / f"nccs_efile_benchmark_tax_year={tax_year}.parquet").write_text("", encoding="utf-8")

    inputs = combined_common._find_efile_inputs(root)
    assert [item.source_variant for item in inputs] == [
        "efile_tax_year_2022",
        "efile_tax_year_2023",
        "efile_tax_year_2024",
    ]
    assert all(item.source_family == "nccs_efile" for item in inputs)


def test_find_latest_postcard_input_prefers_tax_year_window_derivative(tmp_path: Path) -> None:
    root = tmp_path / "postcard"
    old_dir = root / "snapshot_year=2025"
    new_dir = root / "snapshot_year=2026"
    old_dir.mkdir(parents=True)
    new_dir.mkdir(parents=True)
    pd.DataFrame([{"ein": "111111111"}]).to_parquet(
        old_dir / "nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2025.parquet",
        index=False,
    )
    pd.DataFrame([{"ein": "222222222"}]).to_parquet(
        new_dir / "nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.parquet",
        index=False,
    )

    source = combined_common._find_latest_postcard_input(root)
    assert source.source_family == "nccs_postcard"
    assert source.source_variant == "snapshot_year_2026_tax_year_start_2022"
    assert source.input_path.name == "nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.parquet"
    assert source.input_format == "parquet"


def test_discover_source_inputs_keeps_gt_on_filtered_silver_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    gt_filtered = tmp_path / "givingtuesday_990_filings_benchmark.parquet"
    gt_filtered.write_text("", encoding="utf-8")
    postcard_root = tmp_path / "postcard"
    efile_root = tmp_path / "efile"
    bmf_root = tmp_path / "bmf"

    (postcard_root / "snapshot_year=2026").mkdir(parents=True)
    pd.DataFrame([{"ein": "111111111"}]).to_parquet(
        postcard_root / "snapshot_year=2026" / "nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.parquet",
        index=False,
    )
    (efile_root / "tax_year=2022").mkdir(parents=True)
    pd.DataFrame([{"ein": "111111111"}]).to_parquet(
        efile_root / "tax_year=2022" / "nccs_efile_benchmark_tax_year=2022.parquet",
        index=False,
    )
    (bmf_root / "year=2022").mkdir(parents=True)
    pd.DataFrame([{"EIN": "111111111"}]).to_parquet(
        bmf_root / "year=2022" / "nccs_bmf_benchmark_year=2022.parquet",
        index=False,
    )

    monkeypatch.setattr(combined_common, "GT_FILTERED_PARQUET", gt_filtered)
    monkeypatch.setattr(combined_common, "POSTCARD_STAGING_ROOT", postcard_root)
    monkeypatch.setattr(combined_common, "EFILE_STAGING_ROOT", efile_root)
    monkeypatch.setattr(combined_common, "BMF_STAGING_ROOT", bmf_root)

    inputs = combined_common.discover_source_inputs()
    gt_inputs = [item for item in inputs if item.source_family == "givingtuesday_datamart"]

    assert len(gt_inputs) == 1
    assert gt_inputs[0].input_path == gt_filtered
    assert gt_inputs[0].input_path.name == "givingtuesday_990_filings_benchmark.parquet"


def test_discover_bmf_exact_year_lookup_inputs_discovers_all_supported_years(tmp_path: Path) -> None:
    root = tmp_path / "nccs_bmf"
    for year in (2021, 2022, 2024):
        year_dir = root / f"year={year}"
        year_dir.mkdir(parents=True)
        pd.DataFrame([{"harm_ein": "123456789", "harm_tax_year": str(year)}]).to_parquet(
            year_dir / f"nccs_bmf_exact_year_lookup_year={year}.parquet",
            index=False,
        )

    discovered = combined_common.discover_bmf_exact_year_lookup_inputs(root)
    assert list(discovered.keys()) == ["2022", "2024"]
    assert discovered["2022"].name == "nccs_bmf_exact_year_lookup_year=2022.parquet"


def test_parse_numeric_string_handles_commas_parentheses_scientific_and_invalid() -> None:
    assert combined_common.parse_numeric_string("1,234.50") == (1234.5, True)
    assert combined_common.parse_numeric_string("(42)") == (-42.0, True)
    assert combined_common.parse_numeric_string("1.2E+03") == (1200.0, True)
    assert combined_common.parse_numeric_string("NA") == (None, True)
    assert combined_common.parse_numeric_string("na") == (None, True)
    assert combined_common.parse_numeric_string(" NA ") == (None, True)
    assert combined_common.parse_numeric_string("") == (None, True)
    assert combined_common.parse_numeric_string("not-a-number") == (None, False)


def test_normalize_boolean_flags_maps_expected_tokens() -> None:
    series = pd.Series(["1", "0", "Y", "N", "true", "false", "Yes", "No", "", "unexpected"])
    assert combined_build._normalize_boolean_flags(series).tolist() == [
        "true",
        "false",
        "true",
        "false",
        "true",
        "false",
        "true",
        "false",
        "",
        "",
    ]


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


def test_transform_givingtuesday_normalizes_boolean_flags_and_derives_income() -> None:
    source = combined_common.SourceInput(
        source_family="givingtuesday_datamart",
        source_variant="benchmark_filings",
        input_path=Path("givingtuesday.parquet"),
        input_format="parquet",
        snapshot_year="",
        snapshot_month="",
        native_prefix=combined_common.SOURCE_PREFIXES["givingtuesday_datamart"],
    )
    df = pd.DataFrame(
        [
            {
                "ein": "123456789",
                "tax_year": "2023",
                "form_type": "990",
                "FILERNAME1": "Bench Org",
                "FILERUSSTATE": "SD",
                "zip5": "57701",
                "FILERUSZIP": "",
                "county_fips": "46093",
                "region": "BlackHills",
                "TOTREVCURYEA": "100",
                "TOTALRREVENU": "",
                "TOTEXPCURYEA": "90",
                "TOTALEEXPENS": "",
                "TOASEOOYY": "80",
                "ASSEOYOYY": "",
                "derived_income_amount": "10",
                "derived_income_derivation": "derived:TOTREVCURYEA-minus-TOTEXPCURYEA",
                "OPERATHOSPIT": "1",
                "OPERATSCHOOL": "false",
            },
            {
                "ein": "987654321",
                "tax_year": "2023",
                "form_type": "990EZ",
                "FILERNAME1": "Bench Org 2",
                "FILERUSSTATE": "MT",
                "zip5": "",
                "FILERUSZIP": "59801",
                "county_fips": "30063",
                "region": "Missoula",
                "TOTREVCURYEA": "",
                "TOTALRREVENU": "55",
                "TOTEXPCURYEA": "",
                "TOTALEEXPENS": "44",
                "TOASEOOYY": "33",
                "ASSEOYOYY": "",
                "derived_income_amount": "11",
                "derived_income_derivation": "derived:TOTALRREVENU-minus-TOTALEEXPENS",
                "OPERATHOSPIT": "N",
                "OPERATSCHOOL": "weird-token",
            },
        ]
    )
    transformed, _ = combined_build._transform_givingtuesday(source, df)
    assert transformed["harm_is_hospital"].tolist() == ["true", "false"]
    assert transformed["harm_is_university"].tolist() == ["false", ""]
    assert transformed["harm_income_amount"].tolist() == ["10", "11"]
    assert transformed["harm_income_amount__source_column"].tolist() == [
        "derived:TOTREVCURYEA-minus-TOTEXPCURYEA",
        "derived:TOTALRREVENU-minus-TOTALEEXPENS",
    ]


def test_transform_efile_normalizes_boolean_flags_and_maps_income() -> None:
    source = combined_common.SourceInput(
        source_family="nccs_efile",
        source_variant="efile_tax_year_2022",
        input_path=Path("efile.parquet"),
        input_format="parquet",
        snapshot_year="",
        snapshot_month="",
        native_prefix=combined_common.SOURCE_PREFIXES["nccs_efile"],
    )
    df = pd.DataFrame(
        [
            {
                "harm_ein": "123456789",
                "harm_tax_year": "2022",
                "harm_filing_form": "990",
                "harm_org_name": "Efile Org",
                "harm_state": "SD",
                "harm_zip5": "57701",
                "harm_county_fips": "46093",
                "harm_region": "BlackHills",
                "harm_revenue_amount": "100",
                "harm_expenses_amount": "90",
                "harm_assets_amount": "80",
                "harm_income_amount": "10",
                "harm_is_hospital": "true",
                "harm_is_university": "false",
            },
            {
                "harm_ein": "987654321",
                "harm_tax_year": "2022",
                "harm_filing_form": "990EZ",
                "harm_org_name": "Efile Org 2",
                "harm_state": "MT",
                "harm_zip5": "59801",
                "harm_county_fips": "30063",
                "harm_region": "Missoula",
                "harm_revenue_amount": "55",
                "harm_expenses_amount": "44",
                "harm_assets_amount": "33",
                "harm_income_amount": "11",
                "harm_is_hospital": "false",
                "harm_is_university": "",
            },
        ]
    )
    transformed, _ = combined_build._transform_efile(source, df)
    assert transformed["harm_is_hospital"].tolist() == ["true", "false"]
    assert transformed["harm_is_university"].tolist() == ["false", ""]
    assert transformed["harm_income_amount"].tolist() == ["10", "11"]


def test_transform_postcard_normalizes_gross_receipts_flag() -> None:
    source = combined_common.SourceInput(
        source_family="nccs_postcard",
        source_variant="snapshot_year_2026_tax_year_start_2022",
        input_path=Path("postcard.csv"),
        input_format="csv",
        snapshot_year="2026",
        snapshot_month="",
        native_prefix=combined_common.SOURCE_PREFIXES["nccs_postcard"],
    )
    df = pd.DataFrame(
        [
            {
                "ein": "123456789",
                "tax_year": "2024",
                "snapshot_year": "2026",
                "snapshot_month": "2026-03",
                "legal_name": "Postcard Org",
                "organization_state": "SD",
                "officer_state": "",
                "organization_zip": "57701",
                "officer_zip": "",
                "county_fips": "46093",
                "region": "BlackHills",
                "benchmark_match_source": "organization_zip",
                "gross_receipts_under_25000": "T",
            },
            {
                "ein": "987654321",
                "tax_year": "2024",
                "snapshot_year": "2026",
                "snapshot_month": "2026-03",
                "legal_name": "Postcard Org 2",
                "organization_state": "",
                "officer_state": "MT",
                "organization_zip": "",
                "officer_zip": "59801",
                "county_fips": "30063",
                "region": "Missoula",
                "benchmark_match_source": "officer_zip",
                "gross_receipts_under_25000": "F",
            },
        ]
    )
    transformed, _ = combined_build._transform_postcard(source, df)
    assert transformed["harm_gross_receipts_under_25000_flag"].tolist() == ["true", "false"]
    assert transformed["harm_state"].tolist() == ["SD", "MT"]
    assert transformed["harm_zip5"].tolist() == ["57701", "59801"]
    assert transformed["harm_state__source_column"].tolist() == ["organization_state", "officer_state"]
    assert transformed["harm_zip5__source_column"].tolist() == ["organization_zip", "officer_zip"]


def test_apply_bmf_overlay_only_on_exact_ein_and_tax_year_match(tmp_path: Path) -> None:
    df = pd.DataFrame(
        [
            {
                "row_source_family": "nccs_efile",
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
                "row_source_family": "nccs_efile",
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
        ]
    )
    lookup_path = tmp_path / "nccs_bmf_exact_year_lookup_year=2022.parquet"
    pd.DataFrame(
        [
            {
                "harm_ein": "123456789",
                "harm_tax_year": "2022",
                "harm_org_name": "Bench Org",
                "harm_org_name__source_family": "nccs_bmf",
                "harm_org_name__source_variant": "legacy_bmf_lookup_2022",
                "harm_org_name__source_column": "NAME",
                "harm_state": "SD",
                "harm_state__source_family": "nccs_bmf",
                "harm_state__source_variant": "legacy_bmf_lookup_2022",
                "harm_state__source_column": "STATE",
                "harm_zip5": "57701",
                "harm_zip5__source_family": "nccs_bmf",
                "harm_zip5__source_variant": "legacy_bmf_lookup_2022",
                "harm_zip5__source_column": "ZIP5",
                "harm_ntee_code": "P20",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "legacy_bmf_lookup_2022",
                "harm_ntee_code__source_column": "NTEEFINAL",
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "legacy_bmf_lookup_2022",
                "harm_subsection_code__source_column": "SUBSECCD",
            }
        ]
    ).to_parquet(lookup_path, index=False)

    overlaid, stats = combined_build._apply_bmf_overlay(df, lookup_paths={"2022": lookup_path})
    assert overlaid.loc[0, "harm_org_name"] == "Bench Org"
    assert overlaid.loc[0, "harm_org_name__source_variant"] == "legacy_bmf_lookup_2022"
    assert bool(overlaid.loc[0, "bmf_overlay_applied"]) is True
    assert overlaid.loc[0, "bmf_overlay_field_count"] >= 1
    assert overlaid.loc[1, "harm_org_name"] == ""
    assert bool(overlaid.loc[1, "bmf_overlay_applied"]) is False
    assert stats["bmf_exact_lookup_file_count"] == 1
    assert stats["bmf_exact_lookup_row_count"] == 1
    assert stats["bmf_overlay_row_count"] == 1


def test_load_bmf_exact_year_overlay_lookup_filters_to_needed_eins_by_year(tmp_path: Path) -> None:
    def _lookup_row(harm_ein: str, harm_tax_year: str, org_name: str) -> dict[str, str]:
        row = {"harm_ein": harm_ein, "harm_tax_year": harm_tax_year}
        for field in combined_common.OVERLAY_FIELDS:
            row[field] = ""
            row[f"{field}__source_family"] = "nccs_bmf"
            row[f"{field}__source_variant"] = f"raw_bmf_lookup_{harm_tax_year}"
            row[f"{field}__source_column"] = field.upper()
        row["harm_org_name"] = org_name
        row["harm_org_name__source_column"] = "NAME"
        return row

    lookup_2022 = tmp_path / "nccs_bmf_exact_year_lookup_year=2022.parquet"
    lookup_2023 = tmp_path / "nccs_bmf_exact_year_lookup_year=2023.parquet"
    pd.DataFrame(
        [
            _lookup_row("111111111", "2022", "Org 2022 Keep"),
            _lookup_row("222222222", "2022", "Org 2022 Drop"),
        ]
    ).to_parquet(lookup_2022, index=False)
    pd.DataFrame(
        [
            _lookup_row("333333333", "2023", "Org 2023 Keep"),
            _lookup_row("444444444", "2023", "Org 2023 Drop"),
        ]
    ).to_parquet(lookup_2023, index=False)

    filtered = combined_build._load_bmf_exact_year_overlay_lookup(
        {"2022": lookup_2022, "2023": lookup_2023},
        needed_eins_by_year={"2022": {"111111111"}, "2023": {"333333333"}},
    )
    assert sorted(filtered["harm_ein"].tolist()) == ["111111111", "333333333"]
    assert sorted(filtered["harm_org_name"].tolist()) == ["Org 2022 Keep", "Org 2023 Keep"]


def test_write_parquet_with_metadata_sets_schema_version(tmp_path: Path) -> None:
    out_path = tmp_path / "combined.parquet"
    combined_common.write_parquet_with_metadata(
        pd.DataFrame({"row_source_family": ["givingtuesday_datamart"]}),
        out_path,
    )
    meta = pq.read_schema(out_path).metadata
    assert meta[b"combined_schema_version"] == combined_common.COMBINED_SCHEMA_VERSION.encode("utf-8")


def _master_union_row(
    *,
    harm_ein: str,
    harm_tax_year: str,
    row_source_family: str,
    row_source_variant: str,
    row_source_file_name: str,
    row_source_row_number: int,
    row_source_time_basis: str,
    values: dict[str, str] | None = None,
    provenances: dict[str, tuple[str, str, str]] | None = None,
) -> dict[str, object]:
    row: dict[str, object] = {}
    for column in combined_build.HARMONIZED_COLUMNS:
        row[column] = ""
        row[f"{column}__source_family"] = ""
        row[f"{column}__source_variant"] = ""
        row[f"{column}__source_column"] = ""
    row.update(
        {
            "harm_ein": harm_ein,
            "harm_tax_year": harm_tax_year,
            "harm_ein__source_family": row_source_family,
            "harm_ein__source_variant": row_source_variant,
            "harm_ein__source_column": "harm_ein",
            "harm_tax_year__source_family": row_source_family,
            "harm_tax_year__source_variant": row_source_variant,
            "harm_tax_year__source_column": "harm_tax_year",
            "row_source_family": row_source_family,
            "row_source_variant": row_source_variant,
            "row_source_file_name": row_source_file_name,
            "row_source_file_path": row_source_file_name,
            "row_source_row_number": row_source_row_number,
            "row_source_time_basis": row_source_time_basis,
        }
    )
    if values:
        for field, value in values.items():
            row[field] = value
    if provenances:
        for field, (family, variant, column_name) in provenances.items():
            row[f"{field}__source_family"] = family
            row[f"{field}__source_variant"] = variant
            row[f"{field}__source_column"] = column_name
    return row


def test_build_master_from_union_uses_field_priority_and_unique_keys() -> None:
    union_df = pd.DataFrame(
        [
            _master_union_row(
                harm_ein="123456789",
                harm_tax_year="2024",
                row_source_family="givingtuesday_datamart",
                row_source_variant="benchmark_filings",
                row_source_file_name="gt.parquet",
                row_source_row_number=1,
                row_source_time_basis="tax_year",
                values={
                    "harm_org_name": "GT Org",
                    "harm_state": "SD",
                    "harm_zip5": "57701",
                    "harm_revenue_amount": "100",
                    "harm_income_amount": "12",
                    "harm_assets_amount": "500",
                    "harm_is_hospital": "true",
                },
                provenances={
                    "harm_org_name": ("givingtuesday_datamart", "benchmark_filings", "FILERNAME1"),
                    "harm_state": ("givingtuesday_datamart", "benchmark_filings", "FILERUSSTATE"),
                    "harm_zip5": ("givingtuesday_datamart", "benchmark_filings", "zip5"),
                    "harm_revenue_amount": ("givingtuesday_datamart", "benchmark_filings", "TOTREVCURYEA"),
                    "harm_income_amount": ("givingtuesday_datamart", "benchmark_filings", "derived:TOTREVCURYEA-minus-TOTEXPCURYEA"),
                    "harm_assets_amount": ("givingtuesday_datamart", "benchmark_filings", "TOASEOOYY"),
                    "harm_is_hospital": ("givingtuesday_datamart", "benchmark_filings", "OPERATHOSPIT"),
                },
            ),
            _master_union_row(
                harm_ein="123456789",
                harm_tax_year="2024",
                row_source_family="nccs_bmf",
                row_source_variant="raw_bmf_2024",
                row_source_file_name="bmf.parquet",
                row_source_row_number=1,
                row_source_time_basis="reference_snapshot_year",
                values={
                    "harm_org_name": "BMF Org",
                    "harm_ntee_code": "P20",
                    "harm_income_amount": "90",
                    "harm_assets_amount": "700",
                },
                provenances={
                    "harm_org_name": ("nccs_bmf", "raw_bmf_2024", "NAME"),
                    "harm_ntee_code": ("nccs_bmf", "raw_bmf_2024", "NTEE_CD"),
                    "harm_income_amount": ("nccs_bmf", "raw_bmf_2024", "INCOME_AMT"),
                    "harm_assets_amount": ("nccs_bmf", "raw_bmf_2024", "ASSET_AMT"),
                },
            ),
            _master_union_row(
                harm_ein="123456789",
                harm_tax_year="2024",
                row_source_family="nccs_postcard",
                row_source_variant="snapshot_year_2026_tax_year_start_2022",
                row_source_file_name="postcard.csv",
                row_source_row_number=1,
                row_source_time_basis="tax_year_in_snapshot",
                values={
                    "harm_gross_receipts_under_25000_flag": "true",
                    "harm_org_name": "Postcard Org",
                },
                provenances={
                    "harm_gross_receipts_under_25000_flag": ("nccs_postcard", "snapshot_year_2026_tax_year_start_2022", "gross_receipts_under_25000"),
                    "harm_org_name": ("nccs_postcard", "snapshot_year_2026_tax_year_start_2022", "legal_name"),
                },
            ),
            _master_union_row(
                harm_ein="123456789",
                harm_tax_year="2024",
                row_source_family="nccs_efile",
                row_source_variant="efile_tax_year_2024",
                row_source_file_name="efile.parquet",
                row_source_row_number=1,
                row_source_time_basis="tax_year",
                values={
                    "harm_subsection_code": "3",
                    "harm_expenses_amount": "88",
                    "harm_assets_amount": "650",
                    "harm_is_hospital": "false",
                },
                provenances={
                    "harm_subsection_code": ("nccs_bmf", "raw_bmf_2024", "SUBSECTION"),
                    "harm_expenses_amount": ("nccs_efile", "efile_tax_year_2024", "F9_01_EXP_TOT_CY"),
                    "harm_assets_amount": ("nccs_efile", "efile_tax_year_2024", "F9_01_NAFB_ASSET_TOT_EOY"),
                    "harm_is_hospital": ("nccs_efile", "efile_tax_year_2024", "SA_01_PCSTAT_HOSPITAL_X"),
                },
            ),
        ]
    )

    master_df, field_summary_df, conflict_summary_df, conflict_detail_df, stats = combined_build._build_master_from_union(union_df)
    assert len(master_df) == 1
    assert master_df[["harm_ein", "harm_tax_year"]].drop_duplicates().shape[0] == 1
    assert master_df.loc[0, "harm_org_name"] == "GT Org"
    assert master_df.loc[0, "harm_org_name__source_family"] == "givingtuesday_datamart"
    assert master_df.loc[0, "harm_org_name__source_time_basis"] == "tax_year"
    assert master_df.loc[0, "harm_org_name__selected_row_source_family"] == "givingtuesday_datamart"
    assert master_df.loc[0, "harm_org_name__selected_row_time_basis"] == "tax_year"
    assert master_df.loc[0, "harm_org_name__source_tax_year"] == "2024"
    assert master_df.loc[0, "harm_org_name__source_year_offset"] == 0
    assert master_df.loc[0, "harm_ntee_code"] == "P20"
    assert master_df.loc[0, "harm_ntee_code__source_family"] == "nccs_bmf"
    assert master_df.loc[0, "harm_ntee_code__source_time_basis"] == "reference_snapshot_year"
    assert master_df.loc[0, "harm_subsection_code"] == "3"
    assert master_df.loc[0, "harm_subsection_code__source_family"] == "nccs_bmf"
    assert master_df.loc[0, "harm_revenue_amount"] == "100"
    assert master_df.loc[0, "harm_expenses_amount"] == "88"
    assert master_df.loc[0, "harm_income_amount"] == "12"
    assert master_df.loc[0, "harm_income_amount__source_family"] == "givingtuesday_datamart"
    assert master_df.loc[0, "harm_gross_receipts_under_25000_flag"] == "true"
    assert master_df.loc[0, "master_group_row_count"] == 4
    assert master_df.loc[0, "master_source_family_count"] == 4
    assert bool(master_df.loc[0, "master_has_givingtuesday_datamart"]) is True
    assert bool(master_df.loc[0, "master_has_nccs_bmf"]) is True
    assert master_df.loc[0, "master_record_type"] == "filing_only"
    assert bool(master_df.loc[0, "master_has_any_selected_financial_values"]) is True
    assert bool(master_df.loc[0, "master_has_reference_selected_values"]) is False
    assert bool(master_df.loc[0, "master_has_filing_selected_values"]) is True
    assert master_df.loc[0, "master_primary_identity_source"] == "givingtuesday_datamart"
    assert master_df.loc[0, "master_primary_financial_source"] == "givingtuesday_datamart"
    assert master_df.loc[0, "master_primary_classification_source"] == "nccs_bmf"
    assert stats["excluded_blank_key_row_count"] == 0
    assert int(master_df.loc[0, "harm_revenue_amount_num"]) == 100
    assert bool(master_df.loc[0, "harm_revenue_amount_num_parse_ok"]) is True
    assert "harmonized_field" in field_summary_df.columns
    assert "harmonized_field" in conflict_summary_df.columns
    assert set(conflict_detail_df["field_name"]) == {
        "harm_org_name",
        "harm_assets_amount",
        "harm_income_amount",
        "harm_is_hospital",
    }


def test_build_master_from_union_tracks_conflicts_and_tie_breaks() -> None:
    union_df = pd.DataFrame(
        [
            _master_union_row(
                harm_ein="111111111",
                harm_tax_year="2023",
                row_source_family="nccs_efile",
                row_source_variant="a_variant",
                row_source_file_name="efile_a.parquet",
                row_source_row_number=2,
                row_source_time_basis="tax_year",
                values={"harm_assets_amount": "250"},
                provenances={"harm_assets_amount": ("nccs_efile", "a_variant", "F9_01_NAFB_ASSET_TOT_EOY")},
            ),
            _master_union_row(
                harm_ein="111111111",
                harm_tax_year="2023",
                row_source_family="nccs_efile",
                row_source_variant="b_variant",
                row_source_file_name="efile_b.parquet",
                row_source_row_number=1,
                row_source_time_basis="tax_year",
                values={"harm_assets_amount": "300"},
                provenances={"harm_assets_amount": ("nccs_efile", "b_variant", "F9_01_NAFB_ASSET_TOT_EOY")},
            ),
            _master_union_row(
                harm_ein="111111111",
                harm_tax_year="2023",
                row_source_family="nccs_bmf",
                row_source_variant="raw_bmf_2023",
                row_source_file_name="bmf.parquet",
                row_source_row_number=1,
                row_source_time_basis="reference_snapshot_year",
                values={"harm_income_amount": "NA"},
                provenances={"harm_income_amount": ("nccs_bmf", "raw_bmf_2023", "INCOME_AMT")},
            ),
        ]
    )

    master_df, _, conflict_summary_df, conflict_detail_df, _ = combined_build._build_master_from_union(union_df)
    assert len(master_df) == 1
    assert master_df.loc[0, "harm_assets_amount"] == "250"
    assert master_df.loc[0, "harm_assets_amount__source_variant"] == "a_variant"
    assert master_df.loc[0, "harm_assets_amount__selected_row_source_variant"] == "a_variant"
    assert bool(master_df.loc[0, "harm_assets_amount__conflict"]) is True
    assert master_df.loc[0, "harm_assets_amount__distinct_nonblank_value_count"] == 2
    assert master_df.loc[0, "harm_income_amount"] == "NA"
    assert pd.isna(master_df.loc[0, "harm_income_amount_num"])
    assert bool(master_df.loc[0, "harm_income_amount_num_parse_ok"]) is True
    assert int(conflict_summary_df.loc[conflict_summary_df["harmonized_field"] == "harm_assets_amount", "conflict_row_count"].iloc[0]) == 1
    detail_row = conflict_detail_df.loc[conflict_detail_df["field_name"] == "harm_assets_amount"].iloc[0]
    candidate_summary = json.loads(detail_row["candidate_summary_json"])
    assert detail_row["selected_source_variant"] == "a_variant"
    assert len(candidate_summary) == 2
    assert {item["value"] for item in candidate_summary} == {"250", "300"}


def test_build_master_from_union_tracks_value_origin_separately_from_selected_row() -> None:
    union_df = pd.DataFrame(
        [
            _master_union_row(
                harm_ein="333333333",
                harm_tax_year="2024",
                row_source_family="givingtuesday_datamart",
                row_source_variant="benchmark_filings",
                row_source_file_name="gt.parquet",
                row_source_row_number=1,
                row_source_time_basis="tax_year",
                values={"harm_org_name": "Overlay BMF Org"},
                provenances={"harm_org_name": ("nccs_bmf", "raw_bmf_2024", "NAME")},
            )
        ]
    )

    master_df, _, _, _, _ = combined_build._build_master_from_union(union_df)
    assert master_df.loc[0, "harm_org_name"] == "Overlay BMF Org"
    assert master_df.loc[0, "harm_org_name__source_family"] == "nccs_bmf"
    assert master_df.loc[0, "harm_org_name__source_time_basis"] == "reference_snapshot_year"
    assert master_df.loc[0, "harm_org_name__selected_row_source_family"] == "givingtuesday_datamart"
    assert master_df.loc[0, "harm_org_name__selected_row_time_basis"] == "tax_year"


def test_build_master_from_union_has_no_default_path_rescue_columns() -> None:
    union_df = pd.DataFrame(
        [
            _master_union_row(
                harm_ein="444444444",
                harm_tax_year="2022",
                row_source_family="nccs_efile",
                row_source_variant="efile_tax_year_2022",
                row_source_file_name="efile.parquet",
                row_source_row_number=1,
                row_source_time_basis="tax_year",
                values={
                    "harm_org_name": "Exact Year Efile",
                    "harm_state": "SD",
                    "harm_zip5": "57701",
                    "harm_ntee_code": "P20",
                    "harm_subsection_code": "3",
                },
                provenances={
                    "harm_org_name": ("nccs_efile", "efile_tax_year_2022", "F9_00_ORG_NAME_L1"),
                    "harm_state": ("nccs_efile", "efile_tax_year_2022", "F9_00_ORG_ADDR_STATE"),
                    "harm_zip5": ("nccs_efile", "efile_tax_year_2022", "F9_00_ORG_ADDR_ZIP"),
                    "harm_ntee_code": ("nccs_bmf", "legacy_bmf_lookup_2022", "NTEEFINAL"),
                    "harm_subsection_code": ("nccs_bmf", "legacy_bmf_lookup_2022", "SUBSECCD"),
                },
            )
        ]
    )

    master_df, _, _, _, stats = combined_build._build_master_from_union(union_df)
    assert "master_cross_year_identity_backfill_applied" not in master_df.columns
    assert "master_raw_bmf_identity_rescue_applied" not in master_df.columns
    assert "master_raw_bmf_classification_rescue_applied" not in master_df.columns
    assert not any(key.startswith("master_cross_year_identity_") for key in stats)
    assert not any(key.startswith("master_raw_bmf_identity_") for key in stats)
    assert not any(key.startswith("master_raw_bmf_classification_") for key in stats)


def test_build_master_from_union_marks_nonfinancial_only_rows() -> None:
    union_df = pd.DataFrame(
        [
            _master_union_row(
                harm_ein="555555555",
                harm_tax_year="2024",
                row_source_family="nccs_postcard",
                row_source_variant="snapshot_year_2026_tax_year_start_2022",
                row_source_file_name="postcard.csv",
                row_source_row_number=1,
                row_source_time_basis="tax_year_in_snapshot",
                values={"harm_org_name": "Postcard Only", "harm_gross_receipts_under_25000_flag": "true"},
                provenances={
                    "harm_org_name": ("nccs_postcard", "snapshot_year_2026_tax_year_start_2022", "legal_name"),
                    "harm_gross_receipts_under_25000_flag": ("nccs_postcard", "snapshot_year_2026_tax_year_start_2022", "gross_receipts_under_25000"),
                },
            )
        ]
    )

    master_df, _, _, _, _ = combined_build._build_master_from_union(union_df)
    assert bool(master_df.loc[0, "master_has_any_selected_financial_values"]) is False
    assert bool(master_df.loc[0, "master_has_reference_selected_values"]) is False
    assert bool(master_df.loc[0, "master_has_filing_selected_values"]) is False
    assert master_df.loc[0, "master_primary_financial_source"] == ""
    assert master_df.loc[0, "master_record_type"] == "nonfinancial_only"


def test_master_output_contains_no_source_native_columns_and_metadata_lists_include_master_files() -> None:
    union_df = pd.DataFrame(
        [
            _master_union_row(
                harm_ein="222222222",
                harm_tax_year="2024",
                row_source_family="givingtuesday_datamart",
                row_source_variant="benchmark_filings",
                row_source_file_name="gt.parquet",
                row_source_row_number=1,
                row_source_time_basis="tax_year",
                values={"harm_org_name": "GT Org"},
                provenances={"harm_org_name": ("givingtuesday_datamart", "benchmark_filings", "FILERNAME1")},
            )
        ]
    )

    master_df, _, _, _, _ = combined_build._build_master_from_union(union_df)
    assert not any(column.startswith(tuple(combined_common.SOURCE_PREFIXES.values())) for column in master_df.columns)
    assert combined_common.MASTER_OUTPUT_PARQUET.name.endswith("combined_990_master_ein_tax_year.parquet")
    assert combined_common.MASTER_COLUMN_DICTIONARY_CSV in combined_common.master_metadata_files()
    assert combined_common.MASTER_CONFLICT_DETAIL_CSV in combined_common.master_metadata_files()
    assert combined_common.MASTER_CONFLICT_DETAIL_PARQUET in combined_common.master_metadata_files()
    assert combined_common.MASTER_FIELD_SELECTION_SUMMARY_CSV in combined_common.all_metadata_files(include_verification=False)
