"""
Tests for IRS SOI county discovery and filtering helpers.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_SOI_DIR = _REPO_ROOT / "python" / "ingest" / "irs_soi"
if str(_SOI_DIR) not in sys.path:
    sys.path.insert(0, str(_SOI_DIR))
import common as soi_common  # noqa: E402

FIXTURES = _FILE_DIR / "fixtures"
LANDING_HTML = (FIXTURES / "county_landing.html").read_text(encoding="utf-8")
YEAR_HTML = (FIXTURES / "county_2022.html").read_text(encoding="utf-8")


def _fake_probe(url: str, timeout: int = 60) -> dict[str, object]:
    del timeout
    filename = Path(url).name
    sizes = {
        "22incyallagi.csv": 111,
        "22incyallnoagi.csv": 222,
        "22incydocguide.docx": 333,
    }
    return {
        "status_code": 200,
        "content_length": sizes.get(filename),
        "last_modified": "Tue, 18 Mar 2026 00:00:00 GMT",
        "content_type": "text/csv" if filename.endswith(".csv") else "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    }


def test_discover_latest_year_from_fixture() -> None:
    assert soi_common.discover_latest_year_from_html(LANDING_HTML) == 2022


def test_extract_assets_from_year_fixture() -> None:
    with patch.object(soi_common, "probe_url_head", side_effect=_fake_probe):
        assets = soi_common.extract_assets_for_year_page(
            YEAR_HTML,
            "https://www.irs.gov/statistics/soi-tax-stats-county-data-2022",
            2022,
        )
    by_type = {asset.asset_type: asset for asset in assets}
    assert list(by_type.keys()) == ["county_csv_agi", "county_csv_noagi", "users_guide_docx"]
    assert by_type["county_csv_agi"].filename == "22incyallagi.csv"
    assert by_type["county_csv_noagi"].filename == "22incyallnoagi.csv"
    assert by_type["users_guide_docx"].filename == "22incydocguide.docx"


def test_extract_assets_missing_required_raises() -> None:
    broken_html = YEAR_HTML.replace("22incydocguide.docx", "22incydocguide_missing.docx")
    with patch.object(soi_common, "probe_url_head", side_effect=_fake_probe):
        with pytest.raises(ValueError):
            soi_common.extract_assets_for_year_page(
                broken_html,
                "https://www.irs.gov/statistics/soi-tax-stats-county-data-2022",
                2022,
            )


def test_derive_county_fips() -> None:
    assert soi_common.derive_county_fips("1", "11") == "01011"
    assert soi_common.derive_county_fips("01", "000") == "01000"


def test_filter_county_dataframe_excludes_state_totals_and_adds_region() -> None:
    df = pd.DataFrame(
        [
            {"STATEFIPS": "01", "COUNTYFIPS": "000", "COUNTYNAME": "Alabama", "agi_stub": "1"},
            {"STATEFIPS": "01", "COUNTYFIPS": "011", "COUNTYNAME": "Bullock County", "agi_stub": "1"},
            {"STATEFIPS": "01", "COUNTYFIPS": "013", "COUNTYNAME": "Butler County", "agi_stub": "1"},
        ]
    )
    filtered, matched = soi_common.filter_county_dataframe(
        df,
        {"01011"},
        {"01011": "Test Region"},
    )
    assert matched == 1
    assert len(filtered) == 1
    assert filtered.iloc[0]["COUNTYNAME"] == "Bullock County"
    assert filtered.iloc[0]["county_fips"] == "01011"
    assert filtered.iloc[0]["region"] == "Test Region"
    assert bool(filtered.iloc[0]["is_benchmark_county"]) is True


def test_filter_county_dataframe_requires_columns() -> None:
    df = pd.DataFrame([{"STATEFIPS": "01"}])
    with pytest.raises(ValueError):
        soi_common.filter_county_dataframe(df, {"01011"}, {"01011": "Test Region"})


def test_compute_size_match_requires_all_equal() -> None:
    assert soi_common.compute_size_match(10, 10, 10) is True
    assert soi_common.compute_size_match(10, 9, 10) is False
    assert soi_common.compute_size_match(10, 10, None) is False


def test_compute_local_s3_match_requires_equality() -> None:
    assert soi_common.compute_local_s3_match(10, 10) is True
    assert soi_common.compute_local_s3_match(10, 11) is False
    assert soi_common.compute_local_s3_match(10, None) is False


def test_cache_source_size_updates_release_assets() -> None:
    release = {
        "assets": [
            {
                "source_url": "https://example.com/a.csv",
                "source_last_modified": "Mon, 01 Jan 2024 00:00:00 GMT",
                "source_content_length_bytes": None,
            }
        ]
    }
    updated = soi_common.cache_source_size(
        release,
        source_url="https://example.com/a.csv",
        source_last_modified="Mon, 01 Jan 2024 00:00:00 GMT",
        source_content_length_bytes=1234,
    )
    assert updated["assets"][0]["source_content_length_bytes"] == 1234
    cache_key = soi_common.source_size_cache_key("https://example.com/a.csv", "Mon, 01 Jan 2024 00:00:00 GMT")
    assert updated["source_size_cache"][cache_key]["source_content_length_bytes"] == 1234


def test_load_source_size_cache_from_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "release_manifest_tax_year=2022.csv"
    soi_common.write_csv(
        manifest_path,
        [
            {
                "tax_year": 2022,
                "asset_type": "county_csv_agi",
                "source_page_url": "https://www.irs.gov/statistics/soi-tax-stats-county-data-2022",
                "source_url": "https://www.irs.gov/pub/irs-soi/22incyallagi.csv",
                "filename": "22incyallagi.csv",
                "source_content_length_bytes": 35567992,
                "source_last_modified": "Fri, 14 Feb 2025 19:10:13 GMT",
                "local_path": "C:/tmp/22incyallagi.csv",
                "local_bytes": 35567992,
                "s3_bucket": "swb-321-irs990-teos",
                "s3_key": "bronze/irs_soi/county/raw/tax_year=2022/22incyallagi.csv",
                "s3_bytes": "",
                "size_match": "",
            }
        ],
        [
            "tax_year",
            "asset_type",
            "source_page_url",
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
    cache = soi_common.load_source_size_cache_from_manifest(manifest_path)
    cache_key = soi_common.source_size_cache_key(
        "https://www.irs.gov/pub/irs-soi/22incyallagi.csv",
        "Fri, 14 Feb 2025 19:10:13 GMT",
    )
    assert cache[cache_key]["source_content_length_bytes"] == 35567992


def test_live_latest_release_resolves_to_2022() -> None:
    if os.environ.get("IRS_SOI_LIVE_TESTS") != "1":
        if pytest is not None:
            pytest.skip("Set IRS_SOI_LIVE_TESTS=1 to run the live IRS discovery check.")
        return
    release = soi_common.discover_release("latest")
    assert release["tax_year"] == 2022
