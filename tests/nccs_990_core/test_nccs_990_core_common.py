"""
Tests for NCCS Core discovery, bridge preparation, and filtering helpers.
"""

from __future__ import annotations

import importlib.util
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
_COMMON_PATH = _NCCS_DIR / "common.py"
_COMMON_SPEC = importlib.util.spec_from_file_location("nccs_core_common_for_tests", _COMMON_PATH)
if _COMMON_SPEC is None or _COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load Core common module from {_COMMON_PATH}")
nccs_common = importlib.util.module_from_spec(_COMMON_SPEC)
sys.modules.setdefault("nccs_core_common_for_tests", nccs_common)
_COMMON_SPEC.loader.exec_module(nccs_common)

_ANALYSIS_STEP_PATH = _NCCS_DIR / "07_extract_analysis_variables_local.py"
_ANALYSIS_SPEC = importlib.util.spec_from_file_location("nccs_core_analysis_step", _ANALYSIS_STEP_PATH)
nccs_core_analysis_step = importlib.util.module_from_spec(_ANALYSIS_SPEC)
assert _ANALYSIS_SPEC is not None and _ANALYSIS_SPEC.loader is not None
_ANALYSIS_SPEC.loader.exec_module(nccs_core_analysis_step)

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


def test_build_benchmark_zip_frame_rejects_ambiguous_assignments(tmp_path: Path) -> None:
    geoid_path = tmp_path / "GEOID_reference.csv"
    zip_path = tmp_path / "zip_to_county.csv"
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

    with pytest.raises(ValueError, match="ambiguous"):
        nccs_common.build_benchmark_zip_frame(geoid_path, zip_path)


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


def test_build_analysis_outputs_preserves_pc_pz_overlap_and_core_scope_contract(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    bmf_staging_dir = tmp_path / "bmf_staging"
    year_dir = staging_dir / "year=2022"
    year_dir.mkdir(parents=True)
    (bmf_staging_dir / "year=2022").mkdir(parents=True)

    pd.DataFrame(
        [
            {"EIN2": "123456789", "F9_00_ORG_EIN": "123456789", "F9_08_REV_TOT_TOT": "1000", "F9_09_EXP_TOT_TOT": "800", "F9_10_ASSET_TOT_EOY": "2500", "F9_10_NAFB_TOT_EOY": "1700", "F9_08_REV_PROG_TOT_TOT": "400", "F9_08_REV_CONTR_TOT": "300", "F9_09_EXP_GRANT_US_ORG_TOT": "50", "F9_09_EXP_GRANT_US_INDIV_TOT": "25", "F9_09_EXP_GRANT_FRGN_TOT": "5", "F9_04_HOSPITAL_X": "X", "county_fips": "46093", "region": "BlackHills"},
        ]
    ).to_csv(year_dir / "CORE-2022-501C3-CHARITIES-PC-HRMN__benchmark.csv", index=False)
    pd.DataFrame(
        [
            {"EIN2": "123456789", "F9_00_ORG_EIN": "123456789", "F9_01_REV_TOT_CY": "1200", "F9_01_EXP_TOT_CY": "700", "F9_01_NAFB_TOT_EOY": "2100", "F9_01_EXP_REV_LESS_EXP_CY": "500", "F9_08_REV_PROG_TOT_TOT": "450", "F9_08_REV_CONTR_TOT": "350", "SA_02_PUB_GIFT_GRANT_CONTR_TOT": "200", "F9_09_EXP_GRANT_US_ORG_TOT": "60", "F9_09_EXP_GRANT_US_INDIV_TOT": "10", "F9_09_EXP_GRANT_FRGN_TOT": "5", "F9_04_SCHOOL_X": "X", "county_fips": "46093", "region": "BlackHills"},
        ]
    ).to_csv(year_dir / "CORE-2022-501C3-CHARITIES-PZ-HRMN__benchmark.csv", index=False)
    pd.DataFrame(
        [
            {"EIN2": "222222222", "F9_00_ORG_EIN": "222222222", "F9_08_REV_TOT_TOT": "900", "F9_09_EXP_TOT_TOT": "600", "F9_10_ASSET_TOT_EOY": "1900", "F9_10_NAFB_TOT_EOY": "1100", "county_fips": "30049", "region": "Missoula"},
        ]
    ).to_csv(year_dir / "CORE-2022-501CE-NONPROFIT-PC-HRMN__benchmark.csv", index=False)
    pd.DataFrame(
        [
            {"EIN2": "333333333", "F9_00_ORG_EIN": "333333333", "F9_01_REV_TOT_CY": "950", "F9_01_EXP_TOT_CY": "700", "F9_01_NAFB_TOT_EOY": "1300", "F9_01_EXP_REV_LESS_EXP_CY": "250", "county_fips": "30049", "region": "Missoula"},
        ]
    ).to_csv(year_dir / "CORE-2022-501CE-NONPROFIT-PZ-HRMN__benchmark.csv", index=False)
    pd.DataFrame(
        [
            {"EIN2": "444444444", "F9_00_ORG_EIN": "444444444", "F9_00_ORG_NAME_L1": "PF Direct Org", "F9_00_ORG_ADDR_STATE": "SD", "F9_00_ORG_ADDR_ZIP5": "57701", "PF_01_REV_TOT_BOOKS": "5000", "PF_01_EXP_TOT_EXP_DISBMT_BOOKS": "3000", "PF_02_ASSET_TOT_EOY_BV": "10000", "PF_02_LIAB_TOT_EOY_BV": "3500", "PF_01_EXCESS_REV_OVER_EXP_BOOKS": "2000", "PF_16_REV_PROG_FEES_RLTD": "700", "PF_01_REV_CONTR_REC_BOOKS": "1200", "PF_01_EXP_CONTR_PAID_BOOKS": "450", "county_fips": "46093", "region": "BlackHills"},
        ]
    ).to_csv(year_dir / "CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv", index=False)

    pd.DataFrame(
        [
            {"harm_ein": "123456789", "harm_tax_year": "2022", "harm_org_name": "Overlap Org", "harm_state": "SD", "harm_zip5": "57701", "harm_org_name__source_column": "NAME", "harm_state__source_column": "STATE", "harm_zip5__source_column": "ZIP5", "harm_ntee_code": "E21", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2022", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2022", "harm_subsection_code__source_column": "SUBSECTION"},
            {"harm_ein": "222222222", "harm_tax_year": "2022", "harm_org_name": "PC BMF Org", "harm_state": "MT", "harm_zip5": "59801", "harm_org_name__source_column": "NAME", "harm_state__source_column": "STATE", "harm_zip5__source_column": "ZIP5", "harm_ntee_code": "P20", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2022", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "4", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2022", "harm_subsection_code__source_column": "SUBSECTION"},
            {"harm_ein": "333333333", "harm_tax_year": "2022", "harm_org_name": "PZ BMF Org", "harm_state": "MT", "harm_zip5": "59801", "harm_org_name__source_column": "NAME", "harm_state__source_column": "STATE", "harm_zip5__source_column": "ZIP5", "harm_ntee_code": "B43", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2022", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2022", "harm_subsection_code__source_column": "SUBSECTION"},
            {"harm_ein": "444444444", "harm_tax_year": "2022", "harm_org_name": "PF Direct Org", "harm_state": "SD", "harm_zip5": "57701", "harm_org_name__source_column": "NAME", "harm_state__source_column": "STATE", "harm_zip5__source_column": "ZIP5", "harm_ntee_code": "T20", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2022", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2022", "harm_subsection_code__source_column": "SUBSECTION"},
        ]
    ).to_parquet(bmf_staging_dir / "year=2022" / "nccs_bmf_exact_year_lookup_year=2022.parquet", index=False)

    outputs = nccs_core_analysis_step.build_analysis_outputs(
        staging_dir=staging_dir,
        metadata_dir=metadata_dir,
        bmf_staging_dir=bmf_staging_dir,
        irs_bmf_raw_dir=tmp_path / "missing_irs_bmf",
        tax_year=2022,
    )

    analysis_df = pd.read_parquet(outputs["analysis_variables"])
    geography_df = pd.read_parquet(outputs["analysis_geography_metrics"])
    coverage_df = pd.read_csv(outputs["analysis_variable_coverage"])

    assert len(analysis_df) == 5
    overlap_rows = analysis_df.loc[analysis_df["ein"].eq("123456789")].copy()
    assert len(overlap_rows) == 2
    assert set(overlap_rows["core_scope"]) == {"pc", "pz"}
    assert set(overlap_rows["core_overlap_ein_tax_year_group_size"].astype(int)) == {2}
    assert analysis_df.loc[analysis_df["ein"].eq("444444444"), "form_type"].iloc[0] == "990PF"
    assert analysis_df.loc[analysis_df["ein"].eq("222222222"), "form_type"].isna().all()
    assert analysis_df.loc[analysis_df["ein"].eq("222222222"), "org_name"].iloc[0] == "PC BMF Org"
    assert float(analysis_df.loc[analysis_df["ein"].eq("123456789") & analysis_df["core_scope"].eq("pc"), "analysis_grants_paid_candidate_amount"].iloc[0]) == 80.0
    assert float(analysis_df.loc[analysis_df["ein"].eq("444444444"), "analysis_program_service_revenue_candidate_amount"].iloc[0]) == 700.0
    assert float(analysis_df.loc[analysis_df["ein"].eq("444444444"), "analysis_program_service_revenue_amount"].iloc[0]) == 700.0
    assert float(analysis_df.loc[analysis_df["ein"].eq("123456789") & analysis_df["core_scope"].eq("pz"), "analysis_calculated_total_contributions_amount"].iloc[0]) == 350.0
    assert float(analysis_df.loc[analysis_df["ein"].eq("444444444"), "analysis_net_asset_amount"].iloc[0]) == 6500.0
    assert float(analysis_df.loc[analysis_df["ein"].eq("444444444"), "analysis_calculated_cleaned_net_margin_ratio"].iloc[0]) == 0.4
    assert "all_rows" in set(geography_df["analysis_exclusion_variant"])
    assert "exclude_imputed_hospital_university_political_org" in set(geography_df["analysis_exclusion_variant"])
    assert "analysis_total_revenue_amount" in set(coverage_df["canonical_variable"])
    assert "analysis_program_service_revenue_amount" in set(coverage_df["canonical_variable"])
    assert "analysis_calculated_total_contributions_amount" in set(coverage_df["canonical_variable"])
    assert "analysis_calculated_cleaned_net_margin_ratio" in set(coverage_df["canonical_variable"])


def test_build_analysis_outputs_uses_nearest_year_identity_fallback(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    bmf_staging_dir = tmp_path / "bmf_staging"
    year_dir = staging_dir / "year=2022"
    year_dir.mkdir(parents=True)
    (bmf_staging_dir / "year=2022").mkdir(parents=True)
    (bmf_staging_dir / "year=2023").mkdir(parents=True)

    pd.DataFrame(
        [{"EIN2": "555555555", "F9_00_ORG_EIN": "555555555", "PF_01_REV_TOT_BOOKS": "100", "PF_01_EXP_TOT_EXP_DISBMT_BOOKS": "75", "PF_02_ASSET_TOT_EOY_BV": "200", "PF_02_LIAB_TOT_EOY_BV": "50", "PF_01_EXP_CONTR_PAID_BOOKS": "-1", "county_fips": "46093", "region": "BlackHills"}]
    ).to_csv(year_dir / "CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv", index=False)

    pd.DataFrame(
        [{"harm_ein": "555555555", "harm_tax_year": "2022", "harm_org_name": pd.NA, "harm_state": pd.NA, "harm_zip5": pd.NA, "harm_org_name__source_column": pd.NA, "harm_state__source_column": pd.NA, "harm_zip5__source_column": pd.NA, "harm_ntee_code": "E21", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2022", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2022", "harm_subsection_code__source_column": "SUBSECTION"}]
    ).to_parquet(bmf_staging_dir / "year=2022" / "nccs_bmf_exact_year_lookup_year=2022.parquet", index=False)
    pd.DataFrame(
        [{"harm_ein": "555555555", "harm_tax_year": "2023", "harm_org_name": "Nearest Year Org", "harm_state": "SD", "harm_zip5": "57701", "harm_org_name__source_column": "NAME", "harm_state__source_column": "STATE", "harm_zip5__source_column": "ZIP5", "harm_ntee_code": "E21", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2023", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2023", "harm_subsection_code__source_column": "SUBSECTION"}]
    ).to_parquet(bmf_staging_dir / "year=2023" / "nccs_bmf_exact_year_lookup_year=2023.parquet", index=False)

    outputs = nccs_core_analysis_step.build_analysis_outputs(
        staging_dir=staging_dir,
        metadata_dir=metadata_dir,
        bmf_staging_dir=bmf_staging_dir,
        irs_bmf_raw_dir=tmp_path / "missing_irs_bmf",
        tax_year=2022,
    )
    analysis_df = pd.read_parquet(outputs["analysis_variables"])
    row = analysis_df.iloc[0]
    assert row["org_name"] == "Nearest Year Org"
    assert "nearest_year_identity_fallback" in str(row["harm_org_name_source_column"])
    assert pd.isna(row["analysis_grants_paid_candidate_amount"])


def test_build_analysis_outputs_blanks_cleaned_net_margin_for_tiny_revenue(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    bmf_staging_dir = tmp_path / "bmf_staging"
    year_dir = staging_dir / "year=2022"
    year_dir.mkdir(parents=True)
    (bmf_staging_dir / "year=2022").mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "EIN2": "666666666",
                "F9_00_ORG_EIN": "666666666",
                "F9_00_ORG_NAME_L1": "Tiny Revenue PF",
                "F9_00_ORG_ADDR_STATE": "SD",
                "F9_00_ORG_ADDR_ZIP5": "57701",
                "PF_01_REV_TOT_BOOKS": "5",
                "PF_01_EXP_TOT_EXP_DISBMT_BOOKS": "100",
                "PF_02_ASSET_TOT_EOY_BV": "1000",
                "PF_02_LIAB_TOT_EOY_BV": "200",
                "county_fips": "46093",
                "region": "BlackHills",
            }
        ]
    ).to_csv(year_dir / "CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv", index=False)

    pd.DataFrame(
        [
            {
                "harm_ein": "666666666",
                "harm_tax_year": "2022",
                "harm_org_name": "Tiny Revenue PF",
                "harm_state": "SD",
                "harm_zip5": "57701",
                "harm_org_name__source_column": "NAME",
                "harm_state__source_column": "STATE",
                "harm_zip5__source_column": "ZIP5",
                "harm_ntee_code": "T20",
                "harm_ntee_code__source_family": "nccs_bmf",
                "harm_ntee_code__source_variant": "2022",
                "harm_ntee_code__source_column": "NTEE_CD",
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "2022",
                "harm_subsection_code__source_column": "SUBSECTION",
            }
        ]
    ).to_parquet(bmf_staging_dir / "year=2022" / "nccs_bmf_exact_year_lookup_year=2022.parquet", index=False)

    outputs = nccs_core_analysis_step.build_analysis_outputs(
        staging_dir=staging_dir,
        metadata_dir=metadata_dir,
        bmf_staging_dir=bmf_staging_dir,
        irs_bmf_raw_dir=tmp_path / "missing_irs_bmf",
        tax_year=2022,
    )
    analysis_df = pd.read_parquet(outputs["analysis_variables"])
    row = analysis_df.iloc[0]
    assert float(row["analysis_calculated_net_margin_ratio"]) == -19.0
    assert pd.isna(row["analysis_calculated_cleaned_net_margin_ratio"])
    assert pd.isna(row["analysis_calculated_cleaned_net_margin_ratio_source_column"])


def test_build_analysis_outputs_marks_unknown_ntee_and_zero_expense_reserves(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    bmf_staging_dir = tmp_path / "bmf_staging"
    year_dir = staging_dir / "year=2022"
    year_dir.mkdir(parents=True)
    (bmf_staging_dir / "year=2022").mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "EIN2": "777777777",
                "F9_00_ORG_EIN": "777777777",
                "F9_00_ORG_NAME_L1": "Zero Expense PF",
                "F9_00_ORG_ADDR_STATE": "SD",
                "F9_00_ORG_ADDR_ZIP5": "57701",
                "PF_01_REV_TOT_BOOKS": "100",
                "PF_01_EXP_TOT_EXP_DISBMT_BOOKS": "0",
                "PF_02_ASSET_TOT_EOY_BV": "900",
                "PF_02_LIAB_TOT_EOY_BV": "100",
                "county_fips": "46093",
                "region": "BlackHills",
            },
            {
                "EIN2": "888888888",
                "F9_00_ORG_EIN": "888888888",
                "F9_00_ORG_NAME_L1": "Zero Everything PF",
                "F9_00_ORG_ADDR_STATE": "SD",
                "F9_00_ORG_ADDR_ZIP5": "57701",
                "PF_01_REV_TOT_BOOKS": "50",
                "PF_01_EXP_TOT_EXP_DISBMT_BOOKS": "0",
                "PF_02_ASSET_TOT_EOY_BV": "0",
                "PF_02_LIAB_TOT_EOY_BV": "0",
                "county_fips": "46093",
                "region": "BlackHills",
            },
        ]
    ).to_csv(year_dir / "CORE-2022-501C3-PRIVFOUND-PF-HRMN-V0__benchmark.csv", index=False)

    pd.DataFrame(
        [
            {
                "harm_ein": "777777777",
                "harm_tax_year": "2022",
                "harm_org_name": "Zero Expense PF",
                "harm_state": "SD",
                "harm_zip5": "57701",
                "harm_org_name__source_column": "NAME",
                "harm_state__source_column": "STATE",
                "harm_zip5__source_column": "ZIP5",
                "harm_ntee_code": pd.NA,
                "harm_ntee_code__source_family": pd.NA,
                "harm_ntee_code__source_variant": pd.NA,
                "harm_ntee_code__source_column": pd.NA,
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "2022",
                "harm_subsection_code__source_column": "SUBSECTION",
            },
            {
                "harm_ein": "888888888",
                "harm_tax_year": "2022",
                "harm_org_name": "Zero Everything PF",
                "harm_state": "SD",
                "harm_zip5": "57701",
                "harm_org_name__source_column": "NAME",
                "harm_state__source_column": "STATE",
                "harm_zip5__source_column": "ZIP5",
                "harm_ntee_code": pd.NA,
                "harm_ntee_code__source_family": pd.NA,
                "harm_ntee_code__source_variant": pd.NA,
                "harm_ntee_code__source_column": pd.NA,
                "harm_subsection_code": "3",
                "harm_subsection_code__source_family": "nccs_bmf",
                "harm_subsection_code__source_variant": "2022",
                "harm_subsection_code__source_column": "SUBSECTION",
            },
        ]
    ).to_parquet(bmf_staging_dir / "year=2022" / "nccs_bmf_exact_year_lookup_year=2022.parquet", index=False)

    outputs = nccs_core_analysis_step.build_analysis_outputs(
        staging_dir=staging_dir,
        metadata_dir=metadata_dir,
        bmf_staging_dir=bmf_staging_dir,
        irs_bmf_raw_dir=tmp_path / "missing_irs_bmf",
        tax_year=2022,
    )
    analysis_df = pd.read_parquet(outputs["analysis_variables"]).sort_values("ein").reset_index(drop=True)
    first = analysis_df.iloc[0]
    second = analysis_df.iloc[1]
    assert first["analysis_ntee_code"] == "UNKNOWN"
    assert first["analysis_ntee_code_source_column"] == "UNKNOWN_SENTINEL"
    assert pd.isna(first["analysis_calculated_ntee_broad_code"])
    assert first["analysis_calculated_months_of_reserves"] == float("inf")
    assert second["analysis_calculated_months_of_reserves"] == 0.0


def test_build_analysis_outputs_writes_portable_docs(tmp_path: Path) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    bmf_staging_dir = tmp_path / "bmf_staging"
    year_dir = staging_dir / "year=2022"
    year_dir.mkdir(parents=True)
    (bmf_staging_dir / "year=2022").mkdir(parents=True)

    pd.DataFrame(
        [{"EIN2": "555555555", "F9_00_ORG_EIN": "555555555", "F9_08_REV_TOT_TOT": "100", "F9_09_EXP_TOT_TOT": "75", "F9_10_ASSET_TOT_EOY": "200", "F9_10_NAFB_TOT_EOY": "120", "county_fips": "46093", "region": "BlackHills"}]
    ).to_csv(year_dir / "CORE-2022-501C3-CHARITIES-PC-HRMN__benchmark.csv", index=False)
    pd.DataFrame(
        [{"harm_ein": "555555555", "harm_tax_year": "2022", "harm_org_name": "Doc Org", "harm_state": "SD", "harm_zip5": "57701", "harm_org_name__source_column": "NAME", "harm_state__source_column": "STATE", "harm_zip5__source_column": "ZIP5", "harm_ntee_code": "E21", "harm_ntee_code__source_family": "nccs_bmf", "harm_ntee_code__source_variant": "2022", "harm_ntee_code__source_column": "NTEE_CD", "harm_subsection_code": "3", "harm_subsection_code__source_family": "nccs_bmf", "harm_subsection_code__source_variant": "2022", "harm_subsection_code__source_column": "SUBSECTION"}]
    ).to_parquet(bmf_staging_dir / "year=2022" / "nccs_bmf_exact_year_lookup_year=2022.parquet", index=False)

    outputs = nccs_core_analysis_step.build_analysis_outputs(
        staging_dir=staging_dir,
        metadata_dir=metadata_dir,
        bmf_staging_dir=bmf_staging_dir,
        irs_bmf_raw_dir=tmp_path / "missing_irs_bmf",
        tax_year=2022,
    )

    mapping_text = Path(outputs["analysis_variable_mapping"]).read_text(encoding="utf-8")
    processing_text = Path(outputs["analysis_data_processing_doc"]).read_text(encoding="utf-8")
    assert "nccs_990_core_analysis_variables.parquet" in mapping_text
    assert "Overlapping `PC` and `PZ` rows are intentionally preserved" in mapping_text
    assert "filtered Core benchmark files" in processing_text
