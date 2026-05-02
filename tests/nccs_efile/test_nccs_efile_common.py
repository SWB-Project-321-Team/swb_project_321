"""
Tests for NCCS efile discovery and annual benchmark helpers.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

try:
    import pytest
except ImportError:
    pytest = None

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_EFILE_DIR = _REPO_ROOT / "python" / "ingest" / "nccs_efile"
if str(_EFILE_DIR) not in sys.path:
    sys.path.insert(0, str(_EFILE_DIR))
_COMMON_PATH = _EFILE_DIR / "common.py"
_COMMON_SPEC = importlib.util.spec_from_file_location("nccs_efile_common_for_tests", _COMMON_PATH)
if _COMMON_SPEC is None or _COMMON_SPEC.loader is None:
    raise ImportError(f"Unable to load efile common module from {_COMMON_PATH}")
efile_common = importlib.util.module_from_spec(_COMMON_SPEC)
sys.modules.setdefault("nccs_efile_common_for_tests", efile_common)
_COMMON_SPEC.loader.exec_module(efile_common)
_ANALYSIS_PATH = _EFILE_DIR / "08_extract_analysis_variables_local.py"
_ANALYSIS_SPEC = importlib.util.spec_from_file_location("nccs_efile_analysis_for_tests", _ANALYSIS_PATH)
if _ANALYSIS_SPEC is None or _ANALYSIS_SPEC.loader is None:
    raise ImportError(f"Unable to load efile analysis module from {_ANALYSIS_PATH}")
efile_analysis = importlib.util.module_from_spec(_ANALYSIS_SPEC)
sys.modules.setdefault("nccs_efile_analysis_for_tests", efile_analysis)
_ANALYSIS_SPEC.loader.exec_module(efile_analysis)


def test_discover_release_uses_required_tables_and_marks_latest_year_partial(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_html = "<html><body><h1>Efile dataset</h1></body></html>"
    catalog_html = """
    <html><body>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P00-T00-HEADER-2022.CSV">2022 header</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P01-T00-SUMMARY-2022.CSV">2022 summary</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.CSV">2022 schedule a</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P00-T00-HEADER-2023.CSV">2023 header</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P01-T00-SUMMARY-2023.CSV">2023 summary</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/SA-P01-T00-PUBLIC-CHARITY-STATUS-2023.CSV">2023 schedule a</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P00-T00-HEADER-2024.CSV">2024 header</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P01-T00-SUMMARY-2024.CSV">2024 summary</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/SA-P01-T00-PUBLIC-CHARITY-STATUS-2024.CSV">2024 schedule a</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P00-T00-HEADER-2025.CSV">2025 header</a>
    <a href="https://nccs-efile.s3.us-east-1.amazonaws.com/public/efile_v2_1/F9-P01-T00-SUMMARY-2025.CSV">2025 summary</a>
    </body></html>
    """

    def fake_fetch_text(url: str) -> str:
        return dataset_html if "datasets/efile" in url else catalog_html

    def fake_probe(url: str, timeout: int = 60) -> dict[str, object] | None:
        return {
            "status_code": 200,
            "content_length": 100,
            "last_modified": "Sun, 22 Mar 2026 00:00:00 GMT",
            "content_type": "text/csv",
        }

    monkeypatch.setattr(efile_common, "fetch_text", fake_fetch_text)
    monkeypatch.setattr(efile_common, "_safe_probe", fake_probe)

    release, returned_dataset_html, returned_catalog_html = efile_common.discover_release(start_year=2022)
    assert returned_dataset_html == dataset_html
    assert returned_catalog_html == catalog_html
    assert release["selected_tax_years"] == [2022, 2023, 2024]
    assert release["latest_published_year"] == 2024
    assert release["partial_tax_years"] == [2024]
    assert len(release["selected_assets"]) == 9


def test_build_efile_year_to_benchmark_dedupes_and_filters_to_benchmark_counties(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    output_path = tmp_path / "staging" / "nccs_efile_benchmark_tax_year=2022.parquet"
    geoid_reference = tmp_path / "GEOID_reference.csv"
    zip_to_county = tmp_path / "zip_to_county.csv"

    year_assets = [
        {
            "table_name": "F9-P00-T00-HEADER",
            "tax_year": 2022,
            "filename": "F9-P00-T00-HEADER-2022.CSV",
            "is_partial_year": False,
        },
        {
            "table_name": "F9-P01-T00-SUMMARY",
            "tax_year": 2022,
            "filename": "F9-P01-T00-SUMMARY-2022.CSV",
            "is_partial_year": False,
        },
        {
            "table_name": "SA-P01-T00-PUBLIC-CHARITY-STATUS",
            "tax_year": 2022,
            "filename": "SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.CSV",
            "is_partial_year": False,
        },
    ]

    header_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[0])
    summary_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[1])
    schedule_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[2])
    header_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    schedule_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-older",
                "RETURN_TYPE": "990",
                "RETURN_TIME_STAMP": "2023-01-01",
                "RETURN_AMENDED_X": "0",
                "F9_00_BUILD_TIME_STAMP": "2023-02-01",
                "F9_00_RETURN_TIME_STAMP": "2023-01-01",
                "F9_00_RETURN_TYPE": "990",
                "F9_00_TAX_YEAR": "2022",
                "F9_00_RETURN_AMENDED_X": "0",
                "F9_00_ORG_EIN": "111111111",
                "F9_00_ORG_NAME_L1": "Older Org",
                "F9_00_ORG_ADDR_STATE": "SD",
                "F9_00_ORG_ADDR_ZIP": "57701",
            },
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-newer",
                "RETURN_TYPE": "990",
                "RETURN_TIME_STAMP": "2023-04-01",
                "RETURN_AMENDED_X": "1",
                "F9_00_BUILD_TIME_STAMP": "2023-04-15",
                "F9_00_RETURN_TIME_STAMP": "2023-04-01",
                "F9_00_RETURN_TYPE": "990",
                "F9_00_TAX_YEAR": "2022",
                "F9_00_RETURN_AMENDED_X": "1",
                "F9_00_ORG_EIN": "111111111",
                "F9_00_ORG_NAME_L1": "Newer Org",
                "F9_00_ORG_ADDR_STATE": "SD",
                "F9_00_ORG_ADDR_ZIP": "57701",
            },
            {
                "ORG_EIN": "222222222",
                "TAX_YEAR": "2022",
                "URL": "url-outside",
                "RETURN_TYPE": "990EZ",
                "RETURN_TIME_STAMP": "2023-03-01",
                "RETURN_AMENDED_X": "0",
                "F9_00_BUILD_TIME_STAMP": "2023-03-05",
                "F9_00_RETURN_TIME_STAMP": "2023-03-01",
                "F9_00_RETURN_TYPE": "990EZ",
                "F9_00_TAX_YEAR": "2022",
                "F9_00_RETURN_AMENDED_X": "0",
                "F9_00_ORG_EIN": "222222222",
                "F9_00_ORG_NAME_L1": "Outside Org",
                "F9_00_ORG_ADDR_STATE": "CA",
                "F9_00_ORG_ADDR_ZIP": "99999",
            },
        ]
    ).to_csv(header_path, index=False)

    pd.DataFrame(
        [
            {"ORG_EIN": "111111111", "TAX_YEAR": "2022", "URL": "url-older", "F9_01_REV_TOT_CY": "100", "F9_01_EXP_TOT_CY": "80", "F9_01_NAFB_ASSET_TOT_EOY": "500", "F9_01_EXP_REV_LESS_EXP_CY": "20"},
            {"ORG_EIN": "111111111", "TAX_YEAR": "2022", "URL": "url-newer", "F9_01_REV_TOT_CY": "110", "F9_01_EXP_TOT_CY": "90", "F9_01_NAFB_ASSET_TOT_EOY": "550", "F9_01_EXP_REV_LESS_EXP_CY": "20"},
            {"ORG_EIN": "222222222", "TAX_YEAR": "2022", "URL": "url-outside", "F9_01_REV_TOT_CY": "40", "F9_01_EXP_TOT_CY": "35", "F9_01_NAFB_ASSET_TOT_EOY": "100", "F9_01_EXP_REV_LESS_EXP_CY": "5"},
        ]
    ).to_csv(summary_path, index=False)

    pd.DataFrame(
        [
            {"ORG_EIN": "111111111", "TAX_YEAR": "2022", "URL": "url-older", "SA_01_PCSTAT_HOSPITAL_X": "N", "SA_01_PCSTAT_SCHOOL_X": "Y"},
            {"ORG_EIN": "111111111", "TAX_YEAR": "2022", "URL": "url-newer", "SA_01_PCSTAT_HOSPITAL_X": "Y", "SA_01_PCSTAT_SCHOOL_X": "N"},
            {"ORG_EIN": "222222222", "TAX_YEAR": "2022", "URL": "url-outside", "SA_01_PCSTAT_HOSPITAL_X": "0", "SA_01_PCSTAT_SCHOOL_X": "0"},
        ]
    ).to_csv(schedule_path, index=False)

    pd.DataFrame(
        [
            {"ZIP": "57701", "county_fips": "46093"},
        ]
    ).to_csv(zip_to_county, index=False)
    pd.DataFrame(
        [
            {"County": "Pennington", "State": "SD", "GEOID": "46093", "Cluster_name": "BlackHills"},
        ]
    ).to_csv(geoid_reference, index=False)

    result = efile_common.build_efile_year_to_benchmark(
        tax_year=2022,
        year_assets=year_assets,
        raw_dir=raw_dir,
        output_path=output_path,
        geoid_reference_path=geoid_reference,
        zip_to_county_path=zip_to_county,
    )

    built = pd.read_parquet(output_path)
    assert result["input_row_count"] == 3
    assert result["header_form_row_count"] == 3
    assert result["benchmark_admitted_row_count"] == 2
    assert result["duplicate_group_count"] == 1
    assert result["post_dedupe_row_count"] == 1
    assert result["output_row_count"] == 1
    assert result["matched_county_fips_count"] == 1
    assert built["efile_selected_filing_url"].tolist() == ["url-newer"]
    assert built["F9_00_ORG_NAME_L1"].tolist() == ["Newer Org"]
    assert built["F9_01_EXP_REV_LESS_EXP_CY"].tolist() == ["20"]
    assert built["SA_01_PCSTAT_HOSPITAL_X"].tolist() == ["Y"]
    assert built["county_fips"].tolist() == ["46093"]
    assert built["region"].tolist() == ["BlackHills"]
    assert built["harm_ein"].tolist() == ["111111111"]
    assert built["harm_tax_year"].tolist() == ["2022"]
    assert built["harm_filing_form"].tolist() == ["990"]
    assert built["harm_org_name"].tolist() == ["Newer Org"]
    assert built["harm_state"].tolist() == ["SD"]
    assert built["harm_zip5"].tolist() == ["57701"]
    assert built["harm_income_amount"].tolist() == ["20"]
    assert built["harm_is_hospital"].tolist() == ["true"]
    assert built["harm_is_university"].tolist() == ["false"]


def test_build_efile_year_to_benchmark_filters_before_joining_auxiliary_tables(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    output_path = tmp_path / "staging" / "nccs_efile_benchmark_tax_year=2022.parquet"
    geoid_reference = tmp_path / "GEOID_reference.csv"
    zip_to_county = tmp_path / "zip_to_county.csv"

    year_assets = [
        {
            "table_name": "F9-P00-T00-HEADER",
            "tax_year": 2022,
            "filename": "F9-P00-T00-HEADER-2022.CSV",
            "is_partial_year": False,
        },
        {
            "table_name": "F9-P01-T00-SUMMARY",
            "tax_year": 2022,
            "filename": "F9-P01-T00-SUMMARY-2022.CSV",
            "is_partial_year": False,
        },
        {
            "table_name": "SA-P01-T00-PUBLIC-CHARITY-STATUS",
            "tax_year": 2022,
            "filename": "SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.CSV",
            "is_partial_year": False,
        },
    ]

    header_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[0])
    summary_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[1])
    schedule_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[2])
    header_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    schedule_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-benchmark",
                "RETURN_TYPE": "990",
                "RETURN_TIME_STAMP": "2023-01-01",
                "RETURN_AMENDED_X": "0",
                "F9_00_BUILD_TIME_STAMP": "2023-02-01",
                "F9_00_RETURN_TIME_STAMP": "2023-01-01",
                "F9_00_RETURN_TYPE": "990",
                "F9_00_TAX_YEAR": "2022",
                "F9_00_RETURN_AMENDED_X": "0",
                "F9_00_ORG_EIN": "111111111",
                "F9_00_ORG_NAME_L1": "Benchmark Org",
                "F9_00_ORG_ADDR_STATE": "SD",
                "F9_00_ORG_ADDR_ZIP": "57701",
            },
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-outside-newer",
                "RETURN_TYPE": "990",
                "RETURN_TIME_STAMP": "2023-05-01",
                "RETURN_AMENDED_X": "1",
                "F9_00_BUILD_TIME_STAMP": "2023-05-05",
                "F9_00_RETURN_TIME_STAMP": "2023-05-01",
                "F9_00_RETURN_TYPE": "990",
                "F9_00_TAX_YEAR": "2022",
                "F9_00_RETURN_AMENDED_X": "1",
                "F9_00_ORG_EIN": "111111111",
                "F9_00_ORG_NAME_L1": "Outside Newer Org",
                "F9_00_ORG_ADDR_STATE": "CA",
                "F9_00_ORG_ADDR_ZIP": "99999",
            },
        ]
    ).to_csv(header_path, index=False)

    pd.DataFrame(
        [
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-benchmark",
                "F9_01_REV_TOT_CY": "100",
                "F9_01_EXP_TOT_CY": "70",
                "F9_01_NAFB_ASSET_TOT_EOY": "200",
                "F9_01_EXP_REV_LESS_EXP_CY": "30",
            },
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-outside-newer",
                "F9_01_REV_TOT_CY": "999",
                "F9_01_EXP_TOT_CY": "888",
                "F9_01_NAFB_ASSET_TOT_EOY": "777",
                "F9_01_EXP_REV_LESS_EXP_CY": "111",
            },
        ]
    ).to_csv(summary_path, index=False)

    pd.DataFrame(
        [
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-benchmark",
                "SA_01_PCSTAT_HOSPITAL_X": "N",
                "SA_01_PCSTAT_SCHOOL_X": "Y",
            },
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-outside-newer",
                "SA_01_PCSTAT_HOSPITAL_X": "Y",
                "SA_01_PCSTAT_SCHOOL_X": "N",
            },
        ]
    ).to_csv(schedule_path, index=False)

    pd.DataFrame([{"ZIP": "57701", "county_fips": "46093"}]).to_csv(zip_to_county, index=False)
    pd.DataFrame([{"County": "Pennington", "State": "SD", "GEOID": "46093", "Cluster_name": "BlackHills"}]).to_csv(
        geoid_reference,
        index=False,
    )

    result = efile_common.build_efile_year_to_benchmark(
        tax_year=2022,
        year_assets=year_assets,
        raw_dir=raw_dir,
        output_path=output_path,
        geoid_reference_path=geoid_reference,
        zip_to_county_path=zip_to_county,
    )

    built = pd.read_parquet(output_path)
    assert result["input_row_count"] == 2
    assert result["header_form_row_count"] == 2
    assert result["benchmark_admitted_row_count"] == 1
    assert result["joined_row_count"] == 1
    assert result["duplicate_group_count"] == 0
    assert result["post_dedupe_row_count"] == 1
    assert result["output_row_count"] == 1
    assert built["efile_selected_filing_url"].tolist() == ["url-benchmark"]
    assert built["F9_01_REV_TOT_CY"].tolist() == ["100"]
    assert built["SA_01_PCSTAT_SCHOOL_X"].tolist() == ["Y"]


def test_normalize_boolish_treats_x_as_true() -> None:
    series = pd.Series(["X", "x", "0", "", "unknown"])
    normalized = efile_common._normalize_boolish(series)
    assert normalized.tolist() == ["true", "true", "false", "", ""]


def test_build_efile_year_to_benchmark_rejects_ambiguous_benchmark_zip_assignments(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    output_path = tmp_path / "staging" / "nccs_efile_benchmark_tax_year=2022.parquet"
    geoid_reference = tmp_path / "GEOID_reference.csv"
    zip_to_county = tmp_path / "zip_to_county.csv"

    year_assets = [
        {"table_name": "F9-P00-T00-HEADER", "tax_year": 2022, "filename": "F9-P00-T00-HEADER-2022.CSV", "is_partial_year": False},
        {"table_name": "F9-P01-T00-SUMMARY", "tax_year": 2022, "filename": "F9-P01-T00-SUMMARY-2022.CSV", "is_partial_year": False},
        {"table_name": "SA-P01-T00-PUBLIC-CHARITY-STATUS", "tax_year": 2022, "filename": "SA-P01-T00-PUBLIC-CHARITY-STATUS-2022.CSV", "is_partial_year": False},
    ]

    header_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[0])
    summary_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[1])
    schedule_path = efile_common.local_asset_path(raw_dir, efile_common.META_DIR, year_assets[2])
    header_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    schedule_path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "ORG_EIN": "111111111",
                "TAX_YEAR": "2022",
                "URL": "url-1",
                "RETURN_TYPE": "990",
                "RETURN_TIME_STAMP": "2023-01-01",
                "RETURN_AMENDED_X": "0",
                "F9_00_BUILD_TIME_STAMP": "2023-02-01",
                "F9_00_RETURN_TIME_STAMP": "2023-01-01",
                "F9_00_RETURN_TYPE": "990",
                "F9_00_TAX_YEAR": "2022",
                "F9_00_RETURN_AMENDED_X": "0",
                "F9_00_ORG_EIN": "111111111",
                "F9_00_ORG_NAME_L1": "Org",
                "F9_00_ORG_ADDR_STATE": "SD",
                "F9_00_ORG_ADDR_ZIP": "57701",
            },
        ]
    ).to_csv(header_path, index=False)
    pd.DataFrame(
        [
            {"ORG_EIN": "111111111", "TAX_YEAR": "2022", "URL": "url-1", "F9_01_REV_TOT_CY": "100", "F9_01_EXP_TOT_CY": "80", "F9_01_NAFB_ASSET_TOT_EOY": "500", "F9_01_EXP_REV_LESS_EXP_CY": "20"},
        ]
    ).to_csv(summary_path, index=False)
    pd.DataFrame(
        [
            {"ORG_EIN": "111111111", "TAX_YEAR": "2022", "URL": "url-1", "SA_01_PCSTAT_HOSPITAL_X": "N", "SA_01_PCSTAT_SCHOOL_X": "N"},
        ]
    ).to_csv(schedule_path, index=False)
    pd.DataFrame(
        [
            {"ZIP": "57701", "county_fips": "46093"},
            {"ZIP": "57701", "county_fips": "46095"},
        ]
    ).to_csv(zip_to_county, index=False)
    pd.DataFrame(
        [
            {"County": "Pennington", "State": "SD", "GEOID": "46093", "Cluster_name": "BlackHills"},
            {"County": "Meade", "State": "SD", "GEOID": "46095", "Cluster_name": "BlackHills"},
        ]
    ).to_csv(geoid_reference, index=False)

    if pytest is None:
        return
    with pytest.raises(ValueError, match="ambiguous"):
        efile_common.build_efile_year_to_benchmark(
            tax_year=2022,
            year_assets=year_assets,
            raw_dir=raw_dir,
            output_path=output_path,
            geoid_reference_path=geoid_reference,
            zip_to_county_path=zip_to_county,
        )


def test_build_efile_analysis_outputs_builds_expected_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    staging_dir = tmp_path / "staging"
    metadata_dir = tmp_path / "metadata"
    docs_dir = tmp_path / "docs"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    for year, rows in (
        (
            2022,
            [
                {
                    "harm_ein": "111111111",
                    "harm_tax_year": "2022",
                    "harm_filing_form": "990",
                    "harm_org_name": "Alpha University Foundation",
                    "harm_state": "SD",
                    "harm_zip5": "57701",
                    "harm_county_fips": "46093",
                    "harm_region": "BlackHills",
                    "harm_revenue_amount": "100",
                    "harm_expenses_amount": "50",
                    "harm_assets_amount": "240",
                    "harm_income_amount": "50",
                    "harm_is_hospital": "",
                    "harm_is_university": "true",
                }
            ],
        ),
        (
            2023,
            [
                {
                    "harm_ein": "222222222",
                    "harm_tax_year": "2023",
                    "harm_filing_form": "990EZ",
                    "harm_org_name": "Beta Health System",
                    "harm_state": "SD",
                    "harm_zip5": "57702",
                    "harm_county_fips": "46093",
                    "harm_region": "BlackHills",
                    "harm_revenue_amount": "200",
                    "harm_expenses_amount": "100",
                    "harm_assets_amount": "120",
                    "harm_income_amount": "100",
                    "harm_is_hospital": "true",
                    "harm_is_university": "",
                }
            ],
        ),
        (
            2024,
            [
                {
                    "harm_ein": "333333333",
                    "harm_tax_year": "2024",
                    "harm_filing_form": "990",
                    "harm_org_name": "Gamma Civic Center",
                    "harm_state": "SD",
                    "harm_zip5": "57703",
                    "harm_county_fips": "46093",
                    "harm_region": "BlackHills",
                    "harm_revenue_amount": "300",
                    "harm_expenses_amount": "150",
                    "harm_assets_amount": "600",
                    "harm_income_amount": "150",
                    "harm_is_hospital": "",
                    "harm_is_university": "",
                }
            ],
        ),
    ):
        year_dir = staging_dir / f"tax_year={year}"
        year_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows).to_parquet(year_dir / f"nccs_efile_benchmark_tax_year={year}.parquet", index=False)

    monkeypatch.setattr(
        efile_analysis,
        "resolve_release_and_write_metadata",
        lambda metadata_dir, start_year=2022: {
            "selected_assets": [
                {"tax_year": 2022, "table_name": "F9-P00-T00-HEADER"},
                {"tax_year": 2023, "table_name": "F9-P00-T00-HEADER"},
                {"tax_year": 2024, "table_name": "F9-P00-T00-HEADER"},
            ]
        },
    )
    monkeypatch.setattr(
        efile_analysis,
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
                    "source_tax_year": "2023",
                    "analysis_ntee_code": "E22",
                    "analysis_ntee_code_source_family": "nccs_bmf",
                    "analysis_ntee_code_source_variant": "2023",
                    "analysis_ntee_code_source_column": "NTEE_CD",
                    "analysis_subsection_code": "3",
                    "analysis_subsection_code_source_family": "nccs_bmf",
                    "analysis_subsection_code_source_variant": "2023",
                    "analysis_subsection_code_source_column": "SUBSECTION",
                },
                {
                    "ein": "333333333",
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
        efile_analysis,
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
    monkeypatch.setattr(efile_analysis, "analysis_variable_mapping_path", lambda: docs_dir / "efile_mapping.md")
    monkeypatch.setattr(efile_analysis, "analysis_data_processing_doc_path", lambda: docs_dir / "efile_doc.md")

    summary = efile_analysis.build_analysis_outputs(metadata_dir=metadata_dir, staging_dir=staging_dir)
    output_df = pd.read_parquet(staging_dir / "nccs_efile_analysis_variables.parquet")
    region_df = pd.read_parquet(staging_dir / "nccs_efile_analysis_geography_metrics.parquet")
    coverage_df = pd.read_csv(metadata_dir / "nccs_efile_analysis_variable_coverage.csv", dtype=str)

    assert summary["analysis_row_count"] == 3
    assert set(output_df["tax_year"]) == {"2022", "2023", "2024"}
    alpha = output_df.loc[output_df["ein"] == "111111111"].iloc[0]
    assert float(alpha["analysis_total_assets_amount"]) == 240.0
    assert float(alpha["analysis_net_asset_amount"]) == 240.0
    assert alpha["analysis_net_asset_amount_quality_flag"] == "asset_based_proxy"
    assert float(alpha["analysis_calculated_net_margin_ratio"]) == 0.5
    assert float(alpha["analysis_calculated_months_of_reserves"]) == pytest.approx(57.6)
    gamma = output_df.loc[output_df["ein"] == "333333333"].iloc[0]
    assert bool(gamma["analysis_is_political_org"]) is True
    assert set(region_df["analysis_exclusion_variant"]) == {"all_rows", "exclude_imputed_hospital_university_political_org"}
    assert "analysis_total_assets_amount_sum" in region_df.columns
    assert "analysis_net_asset_amount_sum" in region_df.columns
    assert "analysis_calculated_normalized_total_assets_per_nonprofit" in region_df.columns
    assert "analysis_calculated_normalized_total_assets_per_unique_nonprofit" in region_df.columns
    assert "analysis_calculated_normalized_net_asset_per_nonprofit" in region_df.columns
    assert "analysis_calculated_normalized_net_asset_per_unique_nonprofit" in region_df.columns
    program_service_coverage = coverage_df.loc[
        coverage_df["canonical_variable"] == "analysis_program_service_revenue_amount"
    ].iloc[0]
    assert program_service_coverage["availability_status"] == "unavailable"
    assert program_service_coverage["analysis_requirement"] == "Program service revenue"
    assert program_service_coverage["variable_role"] == "unavailable"
    assets_coverage = coverage_df.loc[
        coverage_df["canonical_variable"] == "analysis_total_assets_amount"
    ].iloc[0]
    assert assets_coverage["availability_status"] == "available"
    assert assets_coverage["variable_role"] == "direct"
    assert (metadata_dir / "nccs_efile_analysis_variable_coverage.csv").exists()
    assert (docs_dir / "efile_mapping.md").exists()
