from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_IRS_BMF_DIR = _REPO_ROOT / "python" / "ingest" / "irs_bmf"
if str(_IRS_BMF_DIR) not in sys.path:
    sys.path.insert(0, str(_IRS_BMF_DIR))

_COMMON_PATH = _IRS_BMF_DIR / "common.py"
_FILTER_PATH = _IRS_BMF_DIR / "04_filter_bmf_to_benchmark_local.py"
_ANALYSIS_PATH = _IRS_BMF_DIR / "07_extract_analysis_variables_local.py"


def _load_module(module_name: str, script_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {script_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(module_name, module)
    spec.loader.exec_module(module)
    return module


irs_bmf_common = _load_module("irs_bmf_common_for_tests", _COMMON_PATH)
irs_bmf_filter = _load_module("irs_bmf_filter_for_tests", _FILTER_PATH)
irs_bmf_analysis = _load_module("irs_bmf_analysis_for_tests", _ANALYSIS_PATH)


def test_build_benchmark_zip_map_rejects_ambiguous_assignments() -> None:
    ref = pd.DataFrame({"GEOID": ["46093", "46095"]})
    zip_pairs = pd.DataFrame({"_zip": ["57701", "57701"], "_geoid": ["46093", "46095"]})
    try:
        irs_bmf_common.build_benchmark_zip_map(ref, zip_pairs)
        raised = False
    except RuntimeError:
        raised = True
    assert raised is True


def test_build_filtered_outputs_filters_before_combining_and_resolves_cross_state_duplicates(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    metadata_dir = tmp_path / "metadata"
    staging_dir = tmp_path / "staging"
    raw_dir.mkdir(parents=True)
    metadata_dir.mkdir(parents=True)

    pd.DataFrame(
        [
            {
                "EIN": "123456789",
                "NAME": "Preferred Org",
                "STATE": "SD",
                "ZIP": "57701",
                "SUBSECTION": "03",
                "TAX_PERIOD": "202412",
                "ASSET_AMT": "100",
                "INCOME_AMT": "40",
                "REVENUE_AMT": "80",
                "NTEE_CD": "B43",
            },
            {
                "EIN": "555555555",
                "NAME": "Drop No Year",
                "STATE": "SD",
                "ZIP": "57701",
                "SUBSECTION": "03",
                "TAX_PERIOD": "",
                "ASSET_AMT": "5",
                "INCOME_AMT": "1",
                "REVENUE_AMT": "2",
                "NTEE_CD": "X20",
            },
        ]
    ).to_csv(raw_dir / "eo_sd.csv", index=False)
    pd.DataFrame(
        [
            {
                "EIN": "123456789",
                "NAME": "Less Rich Duplicate",
                "STATE": "",
                "ZIP": "57701",
                "SUBSECTION": "03",
                "TAX_PERIOD": "202412",
                "ASSET_AMT": "",
                "INCOME_AMT": "",
                "REVENUE_AMT": "",
                "NTEE_CD": "",
            },
            {
                "EIN": "999999999",
                "NAME": "Outside Org",
                "STATE": "MT",
                "ZIP": "99999",
                "SUBSECTION": "03",
                "TAX_PERIOD": "202412",
                "ASSET_AMT": "10",
                "INCOME_AMT": "5",
                "REVENUE_AMT": "7",
                "NTEE_CD": "X20",
            },
        ]
    ).to_csv(raw_dir / "eo_mt.csv", index=False)
    pd.DataFrame({"GEOID": ["46093"], "State": ["SD"], "Cluster_name": ["BlackHills"]}).to_csv(tmp_path / "GEOID_reference.csv", index=False)
    pd.DataFrame({"ZIP": ["57701"], "FIPS": ["46093"]}).to_csv(tmp_path / "zip_to_county_fips.csv", index=False)

    summary = irs_bmf_filter.build_filtered_outputs(
        raw_dir=raw_dir,
        metadata_dir=metadata_dir,
        staging_dir=staging_dir,
        geoid_reference_path=tmp_path / "GEOID_reference.csv",
        zip_to_county_path=tmp_path / "zip_to_county_fips.csv",
    )

    combined = pd.read_parquet(staging_dir / "irs_bmf_combined_filtered.parquet")
    year_2024 = pd.read_parquet(staging_dir / "year=2024" / "irs_bmf_benchmark_year=2024.parquet")
    manifest = pd.read_csv(metadata_dir / "irs_bmf_filter_manifest.csv")

    assert summary["combined_row_count_before_resolution"] == 2
    assert summary["combined_row_count_after_resolution"] == 1
    assert summary["duplicate_group_count_before_resolution"] == 1
    assert combined["ein"].tolist() == ["123456789"]
    assert combined["org_name"].tolist() == ["Preferred Org"]
    assert combined["region"].tolist() == ["BlackHills"]
    assert combined["tax_year"].tolist() == ["2024"]
    assert year_2024["ein"].tolist() == ["123456789"]
    assert int(manifest.loc[manifest["artifact_type"].eq("combined_filtered"), "duplicate_rows_dropped_count"].iloc[0]) == 1


def test_build_analysis_outputs_generates_expected_flags_and_docs(tmp_path: Path) -> None:
    metadata_dir = tmp_path / "metadata"
    staging_dir = tmp_path / "staging"
    bmf_staging_dir = tmp_path / "nccs_bmf"
    metadata_dir.mkdir(parents=True)
    for year in (2022, 2023, 2024):
        lookup_dir = bmf_staging_dir / f"year={year}"
        lookup_dir.mkdir(parents=True)
        lookup_row = {
            "harm_ein": f"00000000{year - 2021}",
            "harm_tax_year": str(year),
            "harm_ntee_code": pd.NA,
            "harm_ntee_code__source_family": pd.NA,
            "harm_ntee_code__source_variant": pd.NA,
            "harm_ntee_code__source_column": pd.NA,
            "harm_subsection_code": pd.NA,
            "harm_subsection_code__source_family": pd.NA,
            "harm_subsection_code__source_variant": pd.NA,
            "harm_subsection_code__source_column": pd.NA,
        }
        if year == 2023:
            lookup_row.update(
                {
                    "harm_ntee_code": "P20",
                    "harm_ntee_code__source_family": "nccs_bmf",
                    "harm_ntee_code__source_variant": "2023_exact",
                    "harm_ntee_code__source_column": "NTEE_CD",
                    "harm_subsection_code": "4",
                    "harm_subsection_code__source_family": "nccs_bmf",
                    "harm_subsection_code__source_variant": "2023_exact",
                    "harm_subsection_code__source_column": "SUBSECTION",
                }
            )
        pd.DataFrame([lookup_row]).to_parquet(lookup_dir / f"nccs_bmf_exact_year_lookup_year={year}.parquet", index=False)

    for year in (2022, 2023, 2024):
        year_dir = staging_dir / f"year={year}"
        year_dir.mkdir(parents=True)
        pd.DataFrame(
            [
                {
                    "ein": f"00000000{year - 2021}",
                    "org_name": "Test University" if year == 2022 else ("State Political Action Committee" if year == 2023 else "Unknown Org"),
                    "state": "SD",
                    "zip5": "57701",
                    "county_fips": "46093",
                    "region": "BlackHills",
                    "tax_period": f"{year}12",
                    "tax_year": str(year),
                    "raw_ntee_code": "B43" if year == 2022 else (pd.NA if year == 2023 else "E22"),
                    "raw_subsection_code": "03" if year != 2023 else "4",
                    "raw_total_revenue_amount": float(year),
                    "raw_total_assets_amount": float(year + 1),
                    "raw_total_income_amount": float(year + 2),
                    "raw_ntee_code_source_column": "NTEE_CD" if year != 2023 else pd.NA,
                    "raw_subsection_code_source_column": "SUBSECTION",
                    "raw_total_revenue_amount_source_column": "REVENUE_AMT",
                    "raw_total_assets_amount_source_column": "ASSET_AMT",
                    "raw_total_income_amount_source_column": "INCOME_AMT",
                    "source_state_code": "SD",
                    "source_state_file": "eo_sd.csv",
                    "source_row_number": 0,
                    "benchmark_match_source": "zip_to_county_benchmark_zip_map",
                    "tax_year_derivation_basis": "TAX_PERIOD[:4]",
                    "state_matches_benchmark_zip": True,
                }
            ]
        ).to_parquet(year_dir / f"irs_bmf_benchmark_year={year}.parquet", index=False)

    summary = irs_bmf_analysis.build_analysis_outputs(
        metadata_dir=metadata_dir,
        staging_dir=staging_dir,
        bmf_staging_dir=bmf_staging_dir,
        coverage_path=metadata_dir / "irs_bmf_analysis_variable_coverage.csv",
        mapping_path=tmp_path / "irs_bmf_analysis_variable_mapping.md",
        processing_doc_path=tmp_path / "irs_bmf_pipeline.md",
    )

    analysis_df = pd.read_parquet(staging_dir / "irs_bmf_analysis_variables.parquet")
    coverage_df = pd.read_csv(metadata_dir / "irs_bmf_analysis_variable_coverage.csv")
    field_df = pd.read_parquet(staging_dir / "irs_bmf_analysis_field_metrics.parquet")

    assert summary["analysis_row_count"] == 3
    assert bool(analysis_df.loc[analysis_df["tax_year"].eq("2022"), "analysis_is_university"].iloc[0]) is True
    assert bool(analysis_df.loc[analysis_df["tax_year"].eq("2023"), "analysis_is_political_org"].iloc[0]) is True
    assert bool(analysis_df.loc[analysis_df["tax_year"].eq("2024"), "analysis_is_hospital"].iloc[0]) is True
    assert pd.isna(analysis_df.loc[analysis_df["tax_year"].eq("2023"), "analysis_ntee_code"].iloc[0])
    assert analysis_df.loc[analysis_df["tax_year"].eq("2023"), "analysis_ntee_code_fallback_enriched"].iloc[0] == "P20"
    assert analysis_df.loc[analysis_df["tax_year"].eq("2023"), "analysis_ntee_code_fallback_enriched_source_family"].iloc[0] == "nccs_bmf"
    assert bool(analysis_df.loc[analysis_df["tax_year"].eq("2023"), "analysis_missing_classification_flag"].iloc[0]) is False
    assert (coverage_df["canonical_variable"] == "analysis_missing_classification_flag").any()
    assert (coverage_df["canonical_variable"] == "analysis_ntee_code_fallback_enriched").any()
    assert "analysis_calculated_ntee_broad_code" in field_df.columns
    assert (tmp_path / "irs_bmf_analysis_variable_mapping.md").exists()
    assert (tmp_path / "irs_bmf_pipeline.md").exists()
