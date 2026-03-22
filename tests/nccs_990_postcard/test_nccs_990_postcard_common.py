"""
Tests for NCCS e-Postcard discovery and benchmark filtering helpers.
"""

from __future__ import annotations

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
import common as postcard_common  # noqa: E402

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


def test_live_latest_postcard_release_resolves_to_2026() -> None:
    if os.environ.get("NCCS_POSTCARD_LIVE_TESTS") != "1":
        if pytest is not None:
            pytest.skip("Set NCCS_POSTCARD_LIVE_TESTS=1 to run the live NCCS postcard discovery check.")
        return
    release, _ = postcard_common.discover_release("latest", "all", today=date(2026, 3, 20))
    assert release["snapshot_year"] == 2026
    assert "2026-03" in release["available_snapshot_months"]
