"""
Tests for NCCS BMF discovery and benchmark filtering helpers.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import importlib.util

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_BMF_DIR = _REPO_ROOT / "python" / "ingest" / "nccs_bmf"
if str(_BMF_DIR) not in sys.path:
    sys.path.insert(0, str(_BMF_DIR))
_COMMON_PATH = _BMF_DIR / "common.py"
_COMMON_SPEC = importlib.util.spec_from_file_location("nccs_bmf_common_for_tests", _COMMON_PATH)
if _COMMON_SPEC is None or _COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load BMF common module from {_COMMON_PATH}")
bmf_common = importlib.util.module_from_spec(_COMMON_SPEC)
sys.modules.setdefault("nccs_bmf_common_for_tests", bmf_common)
_COMMON_SPEC.loader.exec_module(bmf_common)
_ANALYSIS_PATH = _BMF_DIR / "07_extract_analysis_variables_local.py"

_ANALYSIS_SPEC = importlib.util.spec_from_file_location("nccs_bmf_analysis_for_tests", _ANALYSIS_PATH)
if _ANALYSIS_SPEC is None or _ANALYSIS_SPEC.loader is None:
    raise ImportError(f"Unable to load BMF analysis module from {_ANALYSIS_PATH}")
bmf_analysis = importlib.util.module_from_spec(_ANALYSIS_SPEC)
sys.modules.setdefault("nccs_bmf_analysis_for_tests", bmf_analysis)
_ANALYSIS_SPEC.loader.exec_module(bmf_analysis)


def test_parse_bmf_catalog_html_collects_raw_and_legacy_links() -> None:
    html = """
    <html><body>
      <a href="https://nccsdata.s3.us-east-1.amazonaws.com/raw/bmf/2025-12-BMF.csv">2025-12</a>
      <a href="https://nccsdata.s3.us-east-1.amazonaws.com/raw/bmf/2026-01-BMF.csv">2026-01</a>
      <a href="https://nccsdata.s3.us-east-1.amazonaws.com/legacy/bmf/BMF-2022-01-501CX-NONPROFIT-PX.csv">2022-01</a>
      <a href="https://nccsdata.s3.us-east-1.amazonaws.com/legacy/bmf/BMF-2022-08-501CX-NONPROFIT-PX.csv">2022-08</a>
    </body></html>
    """
    parsed = bmf_common.parse_bmf_catalog_html(html)
    assert parsed["raw_links_by_month"]["2025-12"].endswith("2025-12-BMF.csv")
    assert parsed["raw_links_by_month"]["2026-01"].endswith("2026-01-BMF.csv")
    assert parsed["legacy_links_by_period"]["2022-08"].endswith("BMF-2022-08-501CX-NONPROFIT-PX.csv")


def test_select_latest_legacy_asset_for_year_picks_latest_period() -> None:
    period, url = bmf_common._select_latest_legacy_asset_for_year(
        {
            "2021-02": "https://example.org/BMF-2021-02-501CX-NONPROFIT-PX.csv",
            "2021-11": "https://example.org/BMF-2021-11-501CX-NONPROFIT-PX.csv",
            "2022-01": "https://example.org/BMF-2022-01-501CX-NONPROFIT-PX.csv",
            "2022-08": "https://example.org/BMF-2022-08-501CX-NONPROFIT-PX.csv",
        },
        2021,
    )
    assert period == "2021-11"
    assert url.endswith("BMF-2021-11-501CX-NONPROFIT-PX.csv")


def test_select_latest_legacy_asset_for_year_raises_when_requested_year_missing() -> None:
    if pytest is None:
        return
    with pytest.raises(ValueError, match="2021 legacy"):
        bmf_common._select_latest_legacy_asset_for_year(
            {
                "2022-01": "https://example.org/BMF-2022-01-501CX-NONPROFIT-PX.csv",
            },
            2021,
        )


def test_select_raw_year_period_prefers_december_for_completed_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_probe(url: str, timeout: int = 60) -> dict[str, object] | None:
        if url.endswith("2024-12-BMF.csv"):
            return {"content_length": 123, "last_modified": "2024-12", "content_type": "text/csv"}
        return None

    monkeypatch.setattr(bmf_common, "_safe_probe", fake_probe)
    period, meta = bmf_common._select_raw_year_period(
        2024,
        latest_available_month="2026-03",
        raw_base_url=bmf_common.RAW_MONTHLY_BASE_URL,
    )
    assert period == "2024-12"
    assert meta["content_length"] == 123


def test_select_raw_year_period_uses_latest_available_for_current_year(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_probe(url: str, timeout: int = 60) -> dict[str, object] | None:
        if url.endswith("2026-03-BMF.csv"):
            return {"content_length": 321, "last_modified": "2026-03", "content_type": "text/csv"}
        return None

    monkeypatch.setattr(bmf_common, "_safe_probe", fake_probe)
    period, meta = bmf_common._select_raw_year_period(
        2026,
        latest_available_month="2026-03",
        raw_base_url=bmf_common.RAW_MONTHLY_BASE_URL,
    )
    assert period == "2026-03"
    assert meta["content_length"] == 321


def test_discover_release_supports_2021_legacy_plus_2022_legacy_and_2023_raw(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(bmf_common, "fetch_text", lambda url: "<html></html>")
    monkeypatch.setattr(
        bmf_common,
        "parse_bmf_dataset_page_html",
        lambda html, base_url: {
            "raw_base_url": bmf_common.RAW_MONTHLY_BASE_URL,
            "legacy_base_url": bmf_common.LEGACY_BASE_URL,
        },
    )
    monkeypatch.setattr(
        bmf_common,
        "parse_bmf_catalog_html",
        lambda html, base_url: {
            "legacy_links_by_period": {
                "2021-03": "https://example.org/BMF-2021-03-501CX-NONPROFIT-PX.csv",
                "2021-11": "https://example.org/BMF-2021-11-501CX-NONPROFIT-PX.csv",
                "2022-01": "https://example.org/BMF-2022-01-501CX-NONPROFIT-PX.csv",
                "2022-08": "https://example.org/BMF-2022-08-501CX-NONPROFIT-PX.csv",
            }
        },
    )
    monkeypatch.setattr(
        bmf_common,
        "discover_latest_available_raw_month",
        lambda today=None, raw_base_url=None: (
            "2023-12",
            {"content_length": 123, "last_modified": "2023-12", "content_type": "text/csv"},
        ),
    )
    monkeypatch.setattr(
        bmf_common,
        "_select_raw_year_period",
        lambda year, latest_available_month, raw_base_url, timeout=60: (
            "2023-12",
            {"content_length": 123, "last_modified": "2023-12", "content_type": "text/csv"},
        ),
    )
    monkeypatch.setattr(
        bmf_common,
        "_safe_probe",
        lambda url, timeout=60: {"content_length": 123, "last_modified": "stub", "content_type": "text/csv"},
    )

    release, _, _ = bmf_common.discover_release(start_year=2021)

    selected_years = [asset["snapshot_year"] for asset in release["selected_assets"]]
    selected_groups = [asset["asset_group"] for asset in release["selected_assets"]]
    assert selected_years == [2021, 2022, 2023]
    assert selected_groups == ["legacy_bmf_csv", "legacy_bmf_csv", "raw_bmf_csv"]


def test_filter_bmf_file_to_benchmark_raw_monthly(tmp_path: Path) -> None:
    raw_path = tmp_path / "2026-03-BMF.csv"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"
    out_path = tmp_path / "year=2026" / "nccs_bmf_benchmark_year=2026.parquet"

    pd.DataFrame(
        [
            {"EIN": "123456789", "NAME": "Bench Org", "STATE": "SD", "ZIP": "57701", "SUBSECTION": "3", "NTEE_CD": "P20", "ASSET_AMT": "100", "INCOME_AMT": "50", "REVENUE_AMT": "75"},
            {"EIN": "222222222", "NAME": "Outside Org", "STATE": "CA", "ZIP": "99999", "SUBSECTION": "3", "NTEE_CD": "P20", "ASSET_AMT": "200", "INCOME_AMT": "60", "REVENUE_AMT": "90"},
        ]
    ).to_csv(raw_path, index=False)
    pd.DataFrame({"GEOID": ["46093"], "State": ["SD"], "Cluster_name": ["BlackHills"]}).to_csv(geoid_path, index=False)
    pd.DataFrame({"ZIP": ["57701"], "FIPS": ["46093"]}).to_csv(zip_path, index=False)

    result = bmf_common.filter_bmf_file_to_benchmark(
        asset={
            "asset_group": "raw_bmf_csv",
            "snapshot_year": 2026,
            "snapshot_month": "2026-03",
        },
        local_bmf_path=raw_path,
        output_path=out_path,
        geoid_reference_path=geoid_path,
        zip_to_county_path=zip_path,
    )

    filtered = pd.read_parquet(out_path)
    assert result["input_row_count"] == 2
    assert result["rows_after_zip_geography_filter"] == 1
    assert result["output_row_count"] == 1
    assert result["matched_county_fips_count"] == 1
    assert result["state_validation_applied"] is True
    assert result["state_mismatch_dropped_count"] == 0
    assert filtered["county_fips"].tolist() == ["46093"]
    assert filtered["bmf_snapshot_year"].tolist() == ["2026"]
    assert filtered["bmf_snapshot_month"].tolist() == ["2026-03"]


def test_filter_bmf_file_to_benchmark_legacy_uses_zip5_and_blank_snapshot_month(tmp_path: Path) -> None:
    raw_path = tmp_path / "BMF-2022-08-501CX-NONPROFIT-PX.csv"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"
    out_path = tmp_path / "year=2022" / "nccs_bmf_benchmark_year=2022.parquet"

    pd.DataFrame(
        [
            {"EIN": "123456789", "NAME": "Bench Org", "STATE": "MN", "ZIP5": "55369", "SUBSECCD": "3", "NTEEFINAL": "P20", "ASSETS": "10", "INCOME": "5", "CTOTREV": "8"},
            {"EIN": "222222222", "NAME": "Outside Org", "STATE": "CA", "ZIP5": "99999", "SUBSECCD": "3", "NTEEFINAL": "P20", "ASSETS": "20", "INCOME": "6", "CTOTREV": "9"},
        ]
    ).to_csv(raw_path, index=False)
    pd.DataFrame({"GEOID": ["27053"], "State": ["MN"], "Cluster_name": ["TwinCities"]}).to_csv(geoid_path, index=False)
    pd.DataFrame({"ZIP": ["55369"], "FIPS": ["27053"]}).to_csv(zip_path, index=False)

    result = bmf_common.filter_bmf_file_to_benchmark(
        asset={
            "asset_group": "legacy_bmf_csv",
            "snapshot_year": 2022,
            "snapshot_month": "",
        },
        local_bmf_path=raw_path,
        output_path=out_path,
        geoid_reference_path=geoid_path,
        zip_to_county_path=zip_path,
    )

    filtered = pd.read_parquet(out_path)
    assert result["input_row_count"] == 2
    assert result["rows_after_zip_geography_filter"] == 1
    assert result["output_row_count"] == 1
    assert result["schema_variant"] == "legacy"
    assert filtered["county_fips"].tolist() == ["27053"]
    assert filtered["bmf_snapshot_year"].tolist() == ["2022"]
    assert filtered["bmf_snapshot_month"].tolist() == [""]


def test_filter_bmf_file_to_benchmark_drops_state_mismatch_after_zip_match(tmp_path: Path) -> None:
    raw_path = tmp_path / "2025-12-BMF.csv"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"
    out_path = tmp_path / "year=2025" / "nccs_bmf_benchmark_year=2025.parquet"

    pd.DataFrame(
        [
            {"EIN": "123456789", "NAME": "Good Org", "STATE": "SD", "ZIP": "57701", "SUBSECTION": "3", "NTEE_CD": "P20", "ASSET_AMT": "100", "INCOME_AMT": "50", "REVENUE_AMT": "75"},
            {"EIN": "987654321", "NAME": "Bad State Org", "STATE": "WY", "ZIP": "57701", "SUBSECTION": "3", "NTEE_CD": "P20", "ASSET_AMT": "200", "INCOME_AMT": "60", "REVENUE_AMT": "90"},
        ]
    ).to_csv(raw_path, index=False)
    pd.DataFrame({"GEOID": ["46093"], "State": ["SD"], "Cluster_name": ["BlackHills"]}).to_csv(geoid_path, index=False)
    pd.DataFrame({"ZIP": ["57701"], "FIPS": ["46093"]}).to_csv(zip_path, index=False)

    result = bmf_common.filter_bmf_file_to_benchmark(
        asset={
            "asset_group": "raw_bmf_csv",
            "snapshot_year": 2025,
            "snapshot_month": "2025-12",
        },
        local_bmf_path=raw_path,
        output_path=out_path,
        geoid_reference_path=geoid_path,
        zip_to_county_path=zip_path,
    )

    filtered = pd.read_parquet(out_path)
    assert result["rows_after_zip_geography_filter"] == 2
    assert result["output_row_count"] == 1
    assert result["state_mismatch_dropped_count"] == 1
    assert filtered["EIN"].tolist() == ["123456789"]


def test_filter_bmf_file_to_benchmark_rejects_ambiguous_benchmark_zip_assignments(tmp_path: Path) -> None:
    raw_path = tmp_path / "2025-12-BMF.csv"
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county_fips.csv"
    out_path = tmp_path / "year=2025" / "nccs_bmf_benchmark_year=2025.parquet"

    pd.DataFrame(
        [
            {"EIN": "123456789", "NAME": "Org", "STATE": "SD", "ZIP": "57701", "SUBSECTION": "3", "NTEE_CD": "P20", "ASSET_AMT": "100", "INCOME_AMT": "50", "REVENUE_AMT": "75"},
        ]
    ).to_csv(raw_path, index=False)
    pd.DataFrame(
        [
            {"GEOID": "46093", "State": "SD", "Cluster_name": "BlackHills"},
            {"GEOID": "46095", "State": "SD", "Cluster_name": "BlackHills"},
        ]
    ).to_csv(geoid_path, index=False)
    pd.DataFrame(
        [
            {"ZIP": "57701", "FIPS": "46093"},
            {"ZIP": "57701", "FIPS": "46095"},
        ]
    ).to_csv(zip_path, index=False)

    if pytest is None:
        return
    with pytest.raises(RuntimeError, match="ambiguous"):
        bmf_common.filter_bmf_file_to_benchmark(
            asset={
                "asset_group": "raw_bmf_csv",
                "snapshot_year": 2025,
                "snapshot_month": "2025-12",
            },
            local_bmf_path=raw_path,
            output_path=out_path,
            geoid_reference_path=geoid_path,
            zip_to_county_path=zip_path,
        )


def test_build_bmf_exact_year_lookup_writes_unfiltered_exact_year_overlay_artifact(tmp_path: Path) -> None:
    raw_path = tmp_path / "BMF-2022-08-501CX-NONPROFIT-PX.csv"
    output_path = tmp_path / "year=2022" / "nccs_bmf_exact_year_lookup_year=2022.parquet"

    pd.DataFrame(
        [
            {"EIN": "123456789", "NAME": "Bench Org", "STATE": "SD", "ZIP5": "57701", "SUBSECCD": "3", "NTEEFINAL": "P20"},
            {"EIN": "123456789", "NAME": "Bench Org Duplicate", "STATE": "SD", "ZIP5": "57701", "SUBSECCD": "3", "NTEEFINAL": "P20"},
            {"EIN": "222222222", "NAME": "Outside Org", "STATE": "CA", "ZIP5": "99999", "SUBSECCD": "", "NTEEFINAL": ""},
        ]
    ).to_csv(raw_path, index=False)

    result = bmf_common.build_bmf_exact_year_lookup(
        asset={
            "asset_group": "legacy_bmf_csv",
            "snapshot_year": 2022,
        },
        local_bmf_path=raw_path,
        output_path=output_path,
    )

    lookup = pd.read_parquet(output_path)
    assert result["lookup_pre_dedupe_row_count"] == 3
    assert result["lookup_duplicate_group_count"] == 1
    assert result["lookup_output_row_count"] == 2
    assert lookup["harm_ein"].tolist() == ["123456789", "222222222"]
    assert lookup["harm_tax_year"].tolist() == ["2022", "2022"]
    assert lookup["harm_org_name"].tolist()[0] == "Bench Org"
    assert lookup["harm_state"].tolist()[0] == "SD"
    assert lookup["harm_zip5"].tolist()[0] == "57701"
    assert lookup["harm_ntee_code"].tolist()[0] == "P20"
    assert lookup["harm_subsection_code"].tolist()[0] == "3"
    assert lookup["harm_ntee_code__source_column"].tolist()[0] == "NTEEFINAL"
    assert lookup["harm_subsection_code__source_column"].tolist()[0] == "SUBSECCD"


def test_build_bmf_analysis_outputs_dedupes_duplicate_ein_year_and_builds_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    docs_dir = tmp_path / "docs"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    year_2022_dir = staging_dir / "year=2022"
    year_2023_dir = staging_dir / "year=2023"
    year_2024_dir = staging_dir / "year=2024"
    year_2022_dir.mkdir(parents=True, exist_ok=True)
    year_2023_dir.mkdir(parents=True, exist_ok=True)
    year_2024_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"EIN": "111111111", "NAME": "Alpha Hospital", "STATE": "SD", "ZIP5": "57701", "CTOTREV": "100", "ASSETS": "300", "INCOME": "20", "county_fips": "46093", "region": "BlackHills"},
            {"EIN": "111111111", "NAME": "Alpha Hospital Duplicate", "STATE": "SD", "ZIP5": "57701", "CTOTREV": "150", "ASSETS": "280", "INCOME": "15", "county_fips": "46093", "region": "BlackHills"},
        ]
    ).to_parquet(year_2022_dir / "nccs_bmf_benchmark_year=2022.parquet", index=False)
    pd.DataFrame(
        [
            {"EIN": "222222222", "NAME": "Beta University", "STATE": "SD", "ZIP": "57702", "REVENUE_AMT": "200", "ASSET_AMT": "500", "INCOME_AMT": "50", "county_fips": "46093", "region": "BlackHills"},
        ]
    ).to_parquet(year_2023_dir / "nccs_bmf_benchmark_year=2023.parquet", index=False)
    pd.DataFrame(
        [
            {"EIN": "333333333", "NAME": "Gamma Civic Action", "STATE": "SD", "ZIP": "57703", "REVENUE_AMT": "300", "ASSET_AMT": "600", "INCOME_AMT": "70", "county_fips": "46093", "region": "BlackHills"},
        ]
    ).to_parquet(year_2024_dir / "nccs_bmf_benchmark_year=2024.parquet", index=False)

    for year, rows in (
        (
            2022,
            [{"harm_ein": "111111111", "harm_tax_year": "2022", "harm_ntee_code": "E22", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2022", "harm_ntee_code__source_column": "NTEEFINAL", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2022", "harm_subsection_code__source_column": "SUBSECCD"}],
        ),
        (
            2023,
            [{"harm_ein": "222222222", "harm_tax_year": "2023", "harm_ntee_code": "B43", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2023", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2023", "harm_subsection_code__source_column": "SUBSECTION"}],
        ),
        (
            2024,
            [{"harm_ein": "333333333", "harm_tax_year": "2024", "harm_ntee_code": "W24", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2024", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "527", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2024", "harm_subsection_code__source_column": "SUBSECTION"}],
        ),
    ):
        pd.DataFrame(rows).to_parquet((staging_dir / f"year={year}" / f"nccs_bmf_exact_year_lookup_year={year}.parquet"), index=False)

    monkeypatch.setattr(
        bmf_analysis,
        "resolve_release_and_write_metadata",
        lambda metadata_dir, start_year=2022: {
            "selected_assets": [
                {"snapshot_year": 2022},
                {"snapshot_year": 2023},
                {"snapshot_year": 2024},
            ]
        },
    )
    monkeypatch.setattr(bmf_analysis, "selected_assets", lambda release: release["selected_assets"])
    monkeypatch.setattr(bmf_analysis, "analysis_variable_mapping_path", lambda: docs_dir / "bmf_mapping.md")
    monkeypatch.setattr(bmf_analysis, "analysis_data_processing_doc_path", lambda: docs_dir / "bmf_doc.md")

    summary = bmf_analysis.build_analysis_outputs(
        metadata_dir=metadata_dir,
        staging_dir=staging_dir,
        bmf_staging_dir=staging_dir,
        irs_bmf_raw_dir=tmp_path / "missing_irs_bmf",
    )
    output_df = pd.read_parquet(staging_dir / "nccs_bmf_analysis_variables.parquet")
    geography_df = pd.read_parquet(staging_dir / "nccs_bmf_analysis_geography_metrics.parquet")
    field_df = pd.read_parquet(staging_dir / "nccs_bmf_analysis_field_metrics.parquet")
    coverage_df = pd.read_csv(metadata_dir / "nccs_bmf_analysis_variable_coverage.csv", dtype=str)

    assert summary["duplicate_group_count"] == 1
    assert len(output_df) == 3
    alpha = output_df.loc[output_df["ein"] == "111111111"].iloc[0]
    assert float(alpha["analysis_total_revenue_amount"]) == 150.0
    assert bool(alpha["analysis_is_hospital"]) is True
    gamma = output_df.loc[output_df["ein"] == "333333333"].iloc[0]
    assert bool(gamma["analysis_is_political_org"]) is True
    assert "analysis_total_nonprofit_count" in geography_df.columns
    assert "unique_nonprofit_ein_count" in geography_df.columns
    assert "analysis_calculated_normalized_total_revenue_per_unique_nonprofit" in geography_df.columns
    assert "analysis_calculated_normalized_total_assets_per_unique_nonprofit" in geography_df.columns
    assert "analysis_calculated_share_of_region_nonprofit_count" in field_df.columns
    assert "analysis_calculated_share_of_region_total_nonprofit_count" in field_df.columns
    assert "analysis_calculated_share_of_region_total_revenue" in field_df.columns
    assert "analysis_calculated_share_of_region_total_assets" in field_df.columns
    expense_coverage = coverage_df.loc[
        coverage_df["canonical_variable"] == "analysis_total_expense_amount"
    ].iloc[0]
    assert expense_coverage["availability_status"] == "unavailable"
    assert expense_coverage["analysis_requirement"] == "Total expense"
    assets_coverage = coverage_df.loc[
        coverage_df["canonical_variable"] == "analysis_total_assets_amount"
    ].iloc[0]
    assert assets_coverage["availability_status"] == "available"
    assert assets_coverage["variable_role"] == "direct"
    assert (metadata_dir / "nccs_bmf_analysis_variable_coverage.csv").exists()
    assert (docs_dir / "bmf_mapping.md").exists()


def test_build_bmf_analysis_outputs_applies_nearest_year_and_irs_fallbacks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    docs_dir = tmp_path / "docs"
    irs_bmf_raw_dir = tmp_path / "irs_bmf_raw"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)
    irs_bmf_raw_dir.mkdir(parents=True, exist_ok=True)

    year_2023_dir = staging_dir / "year=2023"
    year_2024_dir = staging_dir / "year=2024"
    year_2023_dir.mkdir(parents=True, exist_ok=True)
    year_2024_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {"EIN": "444444444", "NAME": "Nearest Year Org", "STATE": "SD", "ZIP": "57701", "REVENUE_AMT": "100", "ASSET_AMT": "200", "INCOME_AMT": "10", "county_fips": "46093", "region": "BlackHills"},
            {"EIN": "555555555", "NAME": "IRS Fallback Org", "STATE": "SD", "ZIP": "57702", "REVENUE_AMT": "120", "ASSET_AMT": "220", "INCOME_AMT": "12", "county_fips": "46093", "region": "BlackHills"},
        ]
    ).to_parquet(year_2024_dir / "nccs_bmf_benchmark_year=2024.parquet", index=False)

    pd.DataFrame(
        [
            {
                "harm_ein": "444444444",
                "harm_tax_year": "2023",
                "harm_ntee_code": "B43",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "2023",
                "harm_ntee_code__source_column": "NTEE_CD",
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "2023",
                "harm_subsection_code__source_column": "SUBSECTION",
            }
        ]
    ).to_parquet(year_2023_dir / "nccs_bmf_exact_year_lookup_year=2023.parquet", index=False)

    pd.DataFrame(
        [
            {
                "harm_ein": "444444444",
                "harm_tax_year": "2024",
                "harm_ntee_code": pd.NA,
                "harm_ntee_code__source_family": pd.NA,
                "harm_ntee_code__source_variant": pd.NA,
                "harm_ntee_code__source_column": pd.NA,
                "harm_subsection_code": pd.NA,
                "harm_subsection_code__source_family": pd.NA,
                "harm_subsection_code__source_variant": pd.NA,
                "harm_subsection_code__source_column": pd.NA,
            },
            {
                "harm_ein": "555555555",
                "harm_tax_year": "2024",
                "harm_ntee_code": pd.NA,
                "harm_ntee_code__source_family": pd.NA,
                "harm_ntee_code__source_variant": pd.NA,
                "harm_ntee_code__source_column": pd.NA,
                "harm_subsection_code": pd.NA,
                "harm_subsection_code__source_family": pd.NA,
                "harm_subsection_code__source_variant": pd.NA,
                "harm_subsection_code__source_column": pd.NA,
            },
        ]
    ).to_parquet(year_2024_dir / "nccs_bmf_exact_year_lookup_year=2024.parquet", index=False)

    pd.DataFrame(
        [
            {"EIN": "555555555", "NAME": "IRS Fallback Org", "STATE": "SD", "NTEE_CD": "E21", "SUBSECTION": "3"},
        ]
    ).to_csv(irs_bmf_raw_dir / "eo_sd.csv", index=False)

    monkeypatch.setattr(
        bmf_analysis,
        "resolve_release_and_write_metadata",
        lambda metadata_dir, start_year=2022: {
            "selected_assets": [
                {"snapshot_year": 2024},
            ]
        },
    )
    monkeypatch.setattr(bmf_analysis, "selected_assets", lambda release: release["selected_assets"])
    monkeypatch.setattr(bmf_analysis, "analysis_variable_mapping_path", lambda: docs_dir / "bmf_mapping.md")
    monkeypatch.setattr(bmf_analysis, "analysis_data_processing_doc_path", lambda: docs_dir / "bmf_doc.md")

    summary = bmf_analysis.build_analysis_outputs(
        metadata_dir=metadata_dir,
        staging_dir=staging_dir,
        bmf_staging_dir=staging_dir,
        irs_bmf_raw_dir=irs_bmf_raw_dir,
    )
    output_df = pd.read_parquet(staging_dir / "nccs_bmf_analysis_variables.parquet").sort_values("ein").reset_index(drop=True)

    assert summary["analysis_row_count"] == 2
    nearest = output_df.loc[output_df["ein"] == "444444444"].iloc[0]
    irs = output_df.loc[output_df["ein"] == "555555555"].iloc[0]
    assert nearest["analysis_ntee_code"] == "B43"
    assert "nearest_year_fallback_from_2024_to_2023" in str(nearest["analysis_ntee_code_source_variant"])
    assert nearest["analysis_subsection_code"] == "3"
    assert irs["analysis_ntee_code"] == "E21"
    assert irs["analysis_ntee_code_source_family"] == "irs_bmf"
    assert irs["analysis_subsection_code"] == "3"
