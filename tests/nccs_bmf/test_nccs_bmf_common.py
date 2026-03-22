"""
Tests for NCCS BMF discovery and benchmark filtering helpers.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_BMF_DIR = _REPO_ROOT / "python" / "ingest" / "nccs_bmf"
if str(_BMF_DIR) not in sys.path:
    sys.path.insert(0, str(_BMF_DIR))
import common as bmf_common  # noqa: E402


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


def test_select_legacy_2022_asset_picks_latest_period() -> None:
    period, url = bmf_common._select_legacy_2022_asset(
        {
            "2022-01": "https://example.org/BMF-2022-01-501CX-NONPROFIT-PX.csv",
            "2022-08": "https://example.org/BMF-2022-08-501CX-NONPROFIT-PX.csv",
        }
    )
    assert period == "2022-08"
    assert url.endswith("BMF-2022-08-501CX-NONPROFIT-PX.csv")


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
    pd.DataFrame({"GEOID": ["46093"], "Cluster_name": ["BlackHills"]}).to_csv(geoid_path, index=False)
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
    assert result["output_row_count"] == 1
    assert result["matched_county_fips_count"] == 1
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
    pd.DataFrame({"GEOID": ["27053"], "Cluster_name": ["TwinCities"]}).to_csv(geoid_path, index=False)
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
    assert result["output_row_count"] == 1
    assert result["schema_variant"] == "legacy"
    assert filtered["county_fips"].tolist() == ["27053"]
    assert filtered["bmf_snapshot_year"].tolist() == ["2022"]
    assert filtered["bmf_snapshot_month"].tolist() == [""]
