"""
Tests for NCCS e-Postcard discovery and benchmark filtering helpers.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import date
from pathlib import Path

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_POSTCARD_DIR = _REPO_ROOT / "python" / "ingest" / "nccs_990_postcard"
if str(_POSTCARD_DIR) not in sys.path:
    sys.path.insert(0, str(_POSTCARD_DIR))
_COMMON_PATH = _POSTCARD_DIR / "common.py"
_COMMON_SPEC = importlib.util.spec_from_file_location("nccs_postcard_common_for_tests", _COMMON_PATH)
if _COMMON_SPEC is None or _COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load postcard common module from {_COMMON_PATH}")
postcard_common = importlib.util.module_from_spec(_COMMON_SPEC)
sys.modules.setdefault("nccs_postcard_common_for_tests", postcard_common)
_COMMON_SPEC.loader.exec_module(postcard_common)
_ANALYSIS_PATH = _POSTCARD_DIR / "07_extract_analysis_variables_local.py"
_ANALYSIS_SPEC = importlib.util.spec_from_file_location("nccs_postcard_analysis_for_tests", _ANALYSIS_PATH)
if _ANALYSIS_SPEC is None or _ANALYSIS_SPEC.loader is None:
    raise ImportError(f"Unable to load postcard analysis module from {_ANALYSIS_PATH}")
postcard_analysis = importlib.util.module_from_spec(_ANALYSIS_SPEC)
sys.modules.setdefault("nccs_postcard_analysis_for_tests", postcard_analysis)
_ANALYSIS_SPEC.loader.exec_module(postcard_analysis)

FIXTURES = _FILE_DIR / "fixtures"
POSTCARD_HTML = (FIXTURES / "postcard_page.html").read_text(encoding="utf-8")


def test_parse_postcard_page_fixture() -> None:
    parsed = postcard_common.parse_postcard_page_html(POSTCARD_HTML)
    assert parsed["linked_snapshot_month"] == "2024-12"
    assert parsed["download_url"].endswith("2024-12-E-POSTCARD.csv")
    assert parsed["raw_base_url"] == "https://nccsdata.s3.us-east-1.amazonaws.com/raw/e-postcard/"


def test_discover_latest_available_snapshot_month_uses_backward_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_probe(url: str, timeout: int = 60) -> dict[str, object] | None:
        if url.endswith("2026-03-E-POSTCARD.csv"):
            return {
                "status_code": 200,
                "content_length": 123,
                "last_modified": "Sun, 01 Mar 2026 00:00:00 GMT",
                "content_type": "text/csv",
            }
        return None

    monkeypatch.setattr(postcard_common, "_head_or_get_snapshot", fake_probe)
    snapshot_month, meta = postcard_common.discover_latest_available_snapshot_month(
        today=date(2026, 3, 20),
        raw_base_url=postcard_common.RAW_BASE_URL,
        lookback_months=6,
    )
    assert snapshot_month == "2026-03"
    assert meta["content_length"] == 123


def test_discover_available_snapshot_months_collects_present_months(monkeypatch: pytest.MonkeyPatch) -> None:
    available = {
        "2026-01": 100,
        "2026-02": 200,
        "2026-03": 300,
    }

    def fake_probe(url: str, timeout: int = 60) -> dict[str, object] | None:
        for snapshot_month, byte_count in available.items():
            if url.endswith(f"{snapshot_month}-E-POSTCARD.csv"):
                return {
                    "status_code": 200,
                    "content_length": byte_count,
                    "last_modified": f"{snapshot_month}-modified",
                    "content_type": "text/csv",
                }
        return None

    monkeypatch.setattr(postcard_common, "_head_or_get_snapshot", fake_probe)
    assets = postcard_common.discover_available_snapshot_months(
        snapshot_year=2026,
        raw_base_url=postcard_common.RAW_BASE_URL,
        latest_snapshot_month="2026-03",
        snapshot_months_arg="all",
    )
    assert [asset["snapshot_month"] for asset in assets] == ["2026-01", "2026-02", "2026-03"]
    assert [asset["source_content_length_bytes"] for asset in assets] == [100, 200, 300]


def test_discover_available_snapshot_months_explicit_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_probe(url: str, timeout: int = 60) -> dict[str, object] | None:
        if url.endswith("2026-01-E-POSTCARD.csv"):
            return {
                "status_code": 200,
                "content_length": 100,
                "last_modified": "2026-01-modified",
                "content_type": "text/csv",
            }
        return None

    monkeypatch.setattr(postcard_common, "_head_or_get_snapshot", fake_probe)
    with pytest.raises(ValueError):
        postcard_common.discover_available_snapshot_months(
            snapshot_year=2026,
            raw_base_url=postcard_common.RAW_BASE_URL,
            latest_snapshot_month=None,
            snapshot_months_arg="2026-01,2026-03",
        )


def test_filter_postcard_year_to_benchmark_prefers_org_zip_then_dedupes_latest_month(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    metadata_dir = tmp_path / "metadata"
    output_path = tmp_path / "staging" / "nccs_990_postcard_benchmark_snapshot_year=2026.csv"

    assets = [
        {
            "asset_group": "postcard_csv",
            "asset_type": "postcard_snapshot_csv",
            "snapshot_year": 2026,
            "snapshot_month": "2026-02",
            "filename": "2026-02-E-POSTCARD.csv",
        },
        {
            "asset_group": "postcard_csv",
            "asset_type": "postcard_snapshot_csv",
            "snapshot_year": 2026,
            "snapshot_month": "2026-03",
            "filename": "2026-03-E-POSTCARD.csv",
        },
    ]

    for asset, rows in (
        (
            assets[0],
            [
                {
                    "ein": "123456789",
                    "tax_year": "2024",
                    "legal_name": "Bench Org",
                    "organization_zip": "57701",
                    "officer_zip": "57702",
                    "tax_period_end_date": "12-31-2024",
                },
                {
                    "ein": "222222222",
                    "tax_year": "2025",
                    "legal_name": "Officer Zip Org",
                    "organization_zip": "",
                    "officer_zip": "55369-1234",
                    "tax_period_end_date": "12-31-2025",
                },
            ],
        ),
        (
            assets[1],
            [
                {
                    "ein": "123456789",
                    "tax_year": "2025",
                    "legal_name": "Bench Org Updated",
                    "organization_zip": "57701",
                    "officer_zip": "57702",
                    "tax_period_end_date": "12-31-2025",
                },
                {
                    "ein": "333333333",
                    "tax_year": "2025",
                    "legal_name": "Outside Org",
                    "organization_zip": "99999",
                    "officer_zip": "",
                    "tax_period_end_date": "12-31-2025",
                },
            ],
        ),
    ):
        local_path = postcard_common.local_asset_path(raw_dir, metadata_dir, asset)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_csv(local_path, index=False)

    result = postcard_common.filter_postcard_year_to_benchmark(
        assets,
        raw_dir,
        metadata_dir,
        output_path,
        {"46093", "27053"},
        {"46093": "South Dakota Region", "27053": "Minnesota Region"},
        {"46093": "SD", "27053": "MN"},
        {"57701": "46093", "55369": "27053"},
        chunk_size=2,
    )

    filtered = pd.read_csv(output_path, dtype=str)
    assert result["input_row_count"] == 4
    assert result["matched_row_count"] == 3
    assert result["output_row_count"] == 2
    assert result["deduped_ein_count"] == 2
    assert result["matched_county_fips_count"] == 2
    assert result["zip_match_source_counts"] == {"officer_zip": 1, "organization_zip": 2}
    assert list(filtered.columns) == [
        "ein",
        "tax_year",
        "legal_name",
        "organization_zip",
        "officer_zip",
        "tax_period_end_date",
        "county_fips",
        "region",
        "benchmark_match_source",
        "snapshot_month",
        "snapshot_year",
        "is_benchmark_county",
    ]
    assert set(filtered["ein"]) == {"123456789", "222222222"}
    bench_org = filtered.loc[filtered["ein"] == "123456789"].iloc[0]
    assert bench_org["snapshot_month"] == "2026-03"
    assert bench_org["tax_year"] == "2025"
    officer_org = filtered.loc[filtered["ein"] == "222222222"].iloc[0]
    assert officer_org["benchmark_match_source"] == "officer_zip"
    assert officer_org["county_fips"] == "27053"
    assert result["state_mismatch_rejected_row_count"] == 0


def test_filter_postcard_year_to_benchmark_drops_state_mismatch_on_match_basis(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    metadata_dir = tmp_path / "metadata"
    output_path = tmp_path / "staging" / "nccs_990_postcard_benchmark_snapshot_year=2026.csv"

    asset = {
        "asset_group": "postcard_csv",
        "asset_type": "postcard_snapshot_csv",
        "snapshot_year": 2026,
        "snapshot_month": "2026-03",
        "filename": "2026-03-E-POSTCARD.csv",
    }
    local_path = postcard_common.local_asset_path(raw_dir, metadata_dir, asset)
    local_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "ein": "111111111",
                "tax_year": "2025",
                "legal_name": "Good Officer Match",
                "organization_state": "TX",
                "officer_state": "SD",
                "organization_zip": "75001",
                "officer_zip": "57701",
                "tax_period_end_date": "12-31-2025",
            },
            {
                "ein": "222222222",
                "tax_year": "2025",
                "legal_name": "Bad Officer Match",
                "organization_state": "TX",
                "officer_state": "AL",
                "organization_zip": "75001",
                "officer_zip": "57701",
                "tax_period_end_date": "12-31-2025",
            },
        ]
    ).to_csv(local_path, index=False)

    result = postcard_common.filter_postcard_year_to_benchmark(
        [asset],
        raw_dir,
        metadata_dir,
        output_path,
        {"46093"},
        {"46093": "South Dakota Region"},
        {"46093": "SD"},
        {"57701": "46093"},
        chunk_size=2,
    )

    filtered = pd.read_csv(output_path, dtype=str)
    assert result["matched_row_count"] == 1
    assert result["state_mismatch_rejected_row_count"] == 1
    assert filtered["ein"].tolist() == ["111111111"]


def test_cache_source_size_updates_release_assets() -> None:
    release = {
        "assets": [
            {
                "source_url": "https://example.org/2026-03-E-POSTCARD.csv",
                "source_last_modified": "Sun, 01 Mar 2026 00:00:00 GMT",
                "source_content_length_bytes": None,
            }
        ]
    }
    updated = postcard_common.cache_source_size(
        release,
        source_url="https://example.org/2026-03-E-POSTCARD.csv",
        source_last_modified="Sun, 01 Mar 2026 00:00:00 GMT",
        source_content_length_bytes=123456,
    )
    assert updated["assets"][0]["source_content_length_bytes"] == 123456


def test_build_tax_year_window_derivative_filters_older_tax_years(tmp_path: Path) -> None:
    source_output = tmp_path / "snapshot_year=2026" / "nccs_990_postcard_benchmark_snapshot_year=2026.csv"
    target_csv_output = (
        tmp_path
        / "snapshot_year=2026"
        / "nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.csv"
    )
    target_parquet_output = (
        tmp_path
        / "snapshot_year=2026"
        / "nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.parquet"
    )
    source_output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"ein": "111111111", "tax_year": "2021", "snapshot_year": "2026", "snapshot_month": "2026-03"},
            {"ein": "222222222", "tax_year": "2022", "snapshot_year": "2026", "snapshot_month": "2026-03"},
            {"ein": "333333333", "tax_year": "2024", "snapshot_year": "2026", "snapshot_month": "2026-03"},
        ]
    ).to_csv(source_output, index=False)

    result = postcard_common.build_tax_year_window_derivative(
        source_output,
        target_csv_output,
        output_parquet_path=target_parquet_output,
        tax_year_start=2022,
    )
    filtered = pd.read_csv(target_csv_output, dtype=str)
    filtered_parquet = pd.read_parquet(target_parquet_output)
    assert result["tax_year_start"] == 2022
    assert result["input_row_count"] == 3
    assert result["output_row_count"] == 2
    assert result["min_tax_year_in_output"] == "2022"
    assert result["max_tax_year_in_output"] == "2024"
    assert result["window_output_csv_path"].endswith(".csv")
    assert result["window_output_parquet_path"].endswith(".parquet")
    assert set(filtered["ein"]) == {"222222222", "333333333"}
    assert set(filtered_parquet["ein"]) == {"222222222", "333333333"}


def test_load_source_size_cache_from_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "release_manifest_snapshot_year=2026.csv"
    postcard_common.write_csv(
        manifest_path,
        [
            {
                "asset_group": "postcard_csv",
                "asset_type": "postcard_snapshot_csv",
                "snapshot_year": "2026",
                "snapshot_month": "2026-03",
                "source_url": "https://example.org/2026-03-E-POSTCARD.csv",
                "filename": "2026-03-E-POSTCARD.csv",
                "source_content_length_bytes": 999,
                "source_last_modified": "Sun, 01 Mar 2026 00:00:00 GMT",
                "local_path": "C:/tmp/2026-03-E-POSTCARD.csv",
                "local_bytes": 999,
                "s3_bucket": "swb-321-irs990-teos",
                "s3_key": "bronze/nccs_990/postcard/raw/snapshot_year=2026/snapshot_month=2026-03/2026-03-E-POSTCARD.csv",
                "s3_bytes": "",
                "size_match": "",
            }
        ],
        [
            "asset_group",
            "asset_type",
            "snapshot_year",
            "snapshot_month",
            "source_url",
            "filename",
            "source_content_length_bytes",
            "source_last_modified",
            "local_path",
            "local_bytes",
            "s3_bucket",
            "s3_key",
            "s3_bytes",
            "size_match",
        ],
    )
    cache = postcard_common.load_source_size_cache_from_manifest(manifest_path)
    cache_key = postcard_common.source_size_cache_key(
        "https://example.org/2026-03-E-POSTCARD.csv",
        "Sun, 01 Mar 2026 00:00:00 GMT",
    )
    assert cache[cache_key]["source_content_length_bytes"] == 999


def test_build_postcard_analysis_outputs_limits_scope_and_builds_geography_metrics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    docs_dir = tmp_path / "docs"
    snapshot_dir = staging_dir / "snapshot_year=2026"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "ein": "111111111",
                "tax_year": "2022",
                "legal_name": "Alpha University Club",
                "gross_receipts_under_25000": "Y",
                "terminated": "N",
                "organization_state": "SD",
                "organization_zip": "57701",
                "county_fips": "46093",
                "region": "BlackHills",
                "benchmark_match_source": "organization_zip",
                "snapshot_month": "2026-03",
                "snapshot_year": "2026",
            },
            {
                "ein": "222222222",
                "tax_year": "2024",
                "legal_name": "Beta Voters First",
                "gross_receipts_under_25000": "N",
                "terminated": "N",
                "organization_state": "SD",
                "organization_zip": "57702",
                "county_fips": "46093",
                "region": "BlackHills",
                "benchmark_match_source": "organization_zip",
                "snapshot_month": "2026-03",
                "snapshot_year": "2026",
            },
            {
                "ein": "333333333",
                "tax_year": "2025",
                "legal_name": "Outside Scope Org",
                "gross_receipts_under_25000": "Y",
                "terminated": "N",
                "organization_state": "SD",
                "organization_zip": "57703",
                "county_fips": "46093",
                "region": "BlackHills",
                "benchmark_match_source": "organization_zip",
                "snapshot_month": "2026-03",
                "snapshot_year": "2026",
            },
        ]
    ).to_parquet(
        snapshot_dir / "nccs_990_postcard_benchmark_tax_year_start=2022_snapshot_year=2026.parquet",
        index=False,
    )

    monkeypatch.setattr(
        postcard_analysis,
        "resolve_release_and_write_metadata",
        lambda snapshot_year, metadata_dir, snapshot_months_arg="all": {"snapshot_year": 2026},
    )
    monkeypatch.setattr(
        postcard_analysis,
        "load_bmf_classification_lookup",
        lambda required_tax_years: pd.DataFrame(
            [
                {
                    "ein": "111111111",
                    "source_tax_year": "2022",
                    "analysis_ntee_code": "B43",
                    "analysis_ntee_code_source_family": "nccs_bmf",
                    "analysis_ntee_code_source_variant": "2022",
                    "analysis_ntee_code_source_column": "NTEEFINAL",
                    "analysis_subsection_code": "3",
                    "analysis_subsection_code_source_family": "nccs_bmf",
                    "analysis_subsection_code_source_variant": "2022",
                    "analysis_subsection_code_source_column": "SUBSECCD",
                },
                {
                    "ein": "222222222",
                    "source_tax_year": "2024",
                    "analysis_ntee_code": "W24",
                    "analysis_ntee_code_source_family": "nccs_bmf",
                    "analysis_ntee_code_source_variant": "2024",
                    "analysis_ntee_code_source_column": "NTEE_CD",
                    "analysis_subsection_code": "527",
                    "analysis_subsection_code_source_family": "nccs_bmf",
                    "analysis_subsection_code_source_variant": "2024",
                    "analysis_subsection_code_source_column": "SUBSECTION",
                },
            ]
        ),
    )
    monkeypatch.setattr(
        postcard_analysis,
        "load_irs_bmf_classification_lookup",
        lambda: pd.DataFrame(
            columns=[
                "ein",
                "analysis_ntee_code",
                "analysis_ntee_code_source_family",
                "analysis_ntee_code_source_variant",
                "analysis_ntee_code_source_column",
                "analysis_subsection_code",
                "analysis_subsection_code_source_family",
                "analysis_subsection_code_source_variant",
                "analysis_subsection_code_source_column",
                "irs_bmf_state",
            ]
        ),
    )
    monkeypatch.setattr(postcard_analysis, "analysis_variable_mapping_path", lambda: docs_dir / "postcard_mapping.md")
    monkeypatch.setattr(postcard_analysis, "analysis_data_processing_doc_path", lambda: docs_dir / "postcard_doc.md")

    summary = postcard_analysis.build_analysis_outputs(metadata_dir=metadata_dir, staging_dir=staging_dir)
    output_df = pd.read_parquet(staging_dir / "nccs_990_postcard_analysis_variables.parquet")
    geography_df = pd.read_parquet(staging_dir / "nccs_990_postcard_analysis_geography_metrics.parquet")
    coverage_df = pd.read_csv(metadata_dir / "nccs_990_postcard_analysis_variable_coverage.csv", dtype=str)

    assert summary["analysis_row_count"] == 2
    assert set(output_df["tax_year"]) == {"2022", "2024"}
    beta = output_df.loc[output_df["ein"] == "222222222"].iloc[0]
    assert bool(beta["analysis_is_political_org"]) is True
    assert bool(beta["analysis_is_small_filer_support"]) is True
    assert set(geography_df["geography_level"]) == {"region", "county"}
    assert "analysis_total_nonprofit_count" in geography_df.columns
    assert "analysis_ntee_code_populated_count" in geography_df.columns
    assert "analysis_ntee_code_missing_count" in geography_df.columns
    assert "analysis_calculated_distinct_broad_ntee_category_count" in geography_df.columns
    revenue_coverage = coverage_df.loc[
        coverage_df["canonical_variable"] == "analysis_total_revenue_amount"
    ].iloc[0]
    assert revenue_coverage["availability_status"] == "unavailable"
    assert revenue_coverage["variable_role"] == "unavailable"
    small_filer_coverage = coverage_df.loc[
        coverage_df["canonical_variable"] == "analysis_is_small_filer_support"
    ].iloc[0]
    assert small_filer_coverage["availability_status"] == "available"
    assert small_filer_coverage["analysis_requirement"] == "Small-filer support flag"
    assert (metadata_dir / "nccs_990_postcard_analysis_variable_coverage.csv").exists()
    assert (docs_dir / "postcard_mapping.md").exists()


def test_live_latest_postcard_release_resolves_to_2026() -> None:
    if os.environ.get("NCCS_POSTCARD_LIVE_TESTS") != "1":
        if pytest is not None:
            pytest.skip("Set NCCS_POSTCARD_LIVE_TESTS=1 to run the live NCCS postcard discovery check.")
        return
    release, _ = postcard_common.discover_release("latest", "all", today=date(2026, 3, 20))
    assert release["snapshot_year"] == 2026
    assert "2026-03" in release["available_snapshot_months"]
