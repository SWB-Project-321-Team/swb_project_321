"""
Tests for NCCS Core discovery, bridge preparation, and filtering helpers.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_NCCS_DIR = _REPO_ROOT / "python" / "ingest" / "nccs_990_core"
if str(_NCCS_DIR) not in sys.path:
    sys.path.insert(0, str(_NCCS_DIR))
import common as nccs_common  # noqa: E402

FIXTURES = _FILE_DIR / "fixtures"
CORE_HTML = (FIXTURES / "catalog_core.html").read_text(encoding="utf-8")
BMF_HTML = (FIXTURES / "catalog_bmf.html").read_text(encoding="utf-8")


def test_parse_core_catalog_fixture_and_latest_common_year() -> None:
    catalog = nccs_common.parse_core_catalog_html(CORE_HTML)
    assert nccs_common.latest_common_core_year(catalog["year_assets_by_group"]) == 2022
    assert catalog["dictionary_urls"]["core_dictionary_pc_pz"].endswith("CORE-HRMN_dd.csv")
    assert catalog["dictionary_urls"]["core_dictionary_pf"].endswith("DD-PF-HRMN-V0.csv")


def test_parse_bmf_catalog_fixture() -> None:
    catalog = nccs_common.parse_bmf_catalog_html(BMF_HTML)
    assert catalog["dictionary_url"].endswith("harmonized_data_dictionary.xlsx")
    assert sorted(catalog["state_urls"]) == ["AZ", "MN", "MT", "SD", "WY"]
    assert catalog["state_urls"]["SD"].endswith("SD_BMF_V1.1.csv")


def test_parse_core_catalog_missing_dictionary_raises() -> None:
    broken_html = CORE_HTML.replace("DD-PF-HRMN-V0.csv", "DD-PF-HRMN-V1.csv")
    with pytest.raises(ValueError):
        nccs_common.parse_core_catalog_html(broken_html)


def test_derive_benchmark_states_from_geoid_reference(tmp_path: Path) -> None:
    geoid_path = tmp_path / "GEOID_reference.csv"
    pd.DataFrame({"GEOID": ["30049", "27053", "46083", "04013"]}).to_csv(geoid_path, index=False)
    assert nccs_common.derive_benchmark_states(geoid_path) == ["AZ", "MN", "MT", "SD"]


def test_prepare_bmf_bridge_dataframe_prefers_block_fips_and_zip_fallback(tmp_path: Path) -> None:
    bmf_path = tmp_path / "SD_BMF_V1.1.csv"
    pd.DataFrame(
        [
            {
                "EIN2": "12-3456789",
                "CENSUS_BLOCK_FIPS": "300490000001234",
                "F990_ORG_ADDR_ZIP": "57701",
                "ORG_YEAR_LAST": "2022",
                "ORG_YEAR_COUNT": "2",
                "GEOCODER_SCORE": "98",
            },
            {
                "EIN2": "123456789",
                "CENSUS_BLOCK_FIPS": "270530000009999",
                "F990_ORG_ADDR_ZIP": "55369",
                "ORG_YEAR_LAST": "2021",
                "ORG_YEAR_COUNT": "1",
                "GEOCODER_SCORE": "50",
            },
            {
                "EIN2": "000000222",
                "CENSUS_BLOCK_FIPS": "",
                "F990_ORG_ADDR_ZIP": "55369-1234",
                "ORG_YEAR_LAST": "2022",
                "ORG_YEAR_COUNT": "1",
                "GEOCODER_SCORE": "88",
            },
            {
                "EIN2": "000000333",
                "CENSUS_BLOCK_FIPS": "",
                "F990_ORG_ADDR_ZIP": "99999",
                "ORG_YEAR_LAST": "2022",
                "ORG_YEAR_COUNT": "1",
                "GEOCODER_SCORE": "88",
            },
        ]
    ).to_csv(bmf_path, index=False)

    bridge_df = nccs_common.prepare_bmf_bridge_dataframe(
        [bmf_path],
        {"30049", "27053"},
        {"30049": "Montana Region", "27053": "Minnesota Region"},
        {"55369": "27053", "57701": "46093"},
    )

    by_ein = {row["join_ein"]: row for row in bridge_df.to_dict(orient="records")}
    assert by_ein["123456789"]["county_fips"] == "30049"
    assert by_ein["123456789"]["benchmark_match_source"] == "census_block_fips"
    assert by_ein["000000222"]["county_fips"] == "27053"
    assert by_ein["000000222"]["benchmark_match_source"] == "zip_to_county"
    assert "000000333" not in by_ein


def test_filter_core_file_to_benchmark_preserves_columns_and_appends_fields(tmp_path: Path) -> None:
    core_path = tmp_path / "CORE-2022-501C3-CHARITIES-PZ-HRMN.csv"
    output_path = tmp_path / "filtered.csv"
    pd.DataFrame(
        [
            {"EIN2": "123456789", "NAME": "Bench Org", "TOTREV": "100"},
            {"EIN2": "000000222", "NAME": "Second Org", "TOTREV": "200"},
            {"EIN2": "000000999", "NAME": "Outside Org", "TOTREV": "300"},
        ]
    ).to_csv(core_path, index=False)

    bridge_df = pd.DataFrame(
        [
            {
                "join_ein": "123456789",
                "county_fips": "30049",
                "region": "Montana Region",
                "benchmark_match_source": "census_block_fips",
                "is_benchmark_county": True,
            },
            {
                "join_ein": "000000222",
                "county_fips": "27053",
                "region": "Minnesota Region",
                "benchmark_match_source": "zip_to_county",
                "is_benchmark_county": True,
            },
        ]
    )

    input_rows, output_rows, matched_counties = nccs_common.filter_core_file_to_benchmark(core_path, output_path, bridge_df, chunk_size=2)
    filtered = pd.read_csv(output_path, dtype=str)
    assert input_rows == 3
    assert output_rows == 2
    assert matched_counties == 2
    assert list(filtered.columns) == [
        "EIN2",
        "NAME",
        "TOTREV",
        "county_fips",
        "region",
        "benchmark_match_source",
        "is_benchmark_county",
    ]
    assert set(filtered["county_fips"]) == {"30049", "27053"}


def test_cache_source_size_updates_release_assets() -> None:
    release = {
        "assets": [
            {
                "source_url": "https://example.org/core.csv",
                "source_last_modified": "Fri, 01 Mar 2024 00:00:00 GMT",
                "source_content_length_bytes": None,
            }
        ]
    }
    updated = nccs_common.cache_source_size(
        release,
        source_url="https://example.org/core.csv",
        source_last_modified="Fri, 01 Mar 2024 00:00:00 GMT",
        source_content_length_bytes=123456,
    )
    assert updated["assets"][0]["source_content_length_bytes"] == 123456
    cache_key = nccs_common.source_size_cache_key(
        "https://example.org/core.csv",
        "Fri, 01 Mar 2024 00:00:00 GMT",
    )
    assert updated["source_size_cache"][cache_key]["source_content_length_bytes"] == 123456


def test_load_source_size_cache_from_manifest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "release_manifest_year=2022.csv"
    nccs_common.write_csv(
        manifest_path,
        [
            {
                "asset_group": "core_csv",
                "asset_type": "501c3_charities_pz",
                "family": "501c3_charities",
                "scope": "pz",
                "year": "2022",
                "year_basis": "latest_common",
                "benchmark_state": "",
                "source_url": "https://example.org/CORE-2022-501C3-CHARITIES-PZ-HRMN.csv",
                "filename": "CORE-2022-501C3-CHARITIES-PZ-HRMN.csv",
                "source_content_length_bytes": 999,
                "source_last_modified": "Fri, 01 Mar 2024 00:00:00 GMT",
                "local_path": "C:/tmp/core.csv",
                "local_bytes": 999,
                "s3_bucket": "swb-321-irs990-teos",
                "s3_key": "bronze/nccs_990/core/raw/year=2022/CORE-2022-501C3-CHARITIES-PZ-HRMN.csv",
                "s3_bytes": "",
                "size_match": "",
            }
        ],
        [
            "asset_group",
            "asset_type",
            "family",
            "scope",
            "year",
            "year_basis",
            "benchmark_state",
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
    cache = nccs_common.load_source_size_cache_from_manifest(manifest_path)
    cache_key = nccs_common.source_size_cache_key(
        "https://example.org/CORE-2022-501C3-CHARITIES-PZ-HRMN.csv",
        "Fri, 01 Mar 2024 00:00:00 GMT",
    )
    assert cache[cache_key]["source_content_length_bytes"] == 999


def test_live_latest_common_release_resolves_to_2022() -> None:
    if os.environ.get("NCCS_CORE_LIVE_TESTS") != "1":
        if pytest is not None:
            pytest.skip("Set NCCS_CORE_LIVE_TESTS=1 to run the live NCCS discovery check.")
        return
    release, _, _ = nccs_common.discover_release("latest_common", geoid_reference_path=nccs_common.GEOID_REFERENCE_CSV)
    assert release["tax_year"] == 2022
