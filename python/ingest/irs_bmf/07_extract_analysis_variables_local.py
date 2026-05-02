"""
Step 07: Build IRS EO BMF analysis outputs for the 2022-2024 benchmark window.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from common import (
    ANALYSIS_COVERAGE_PREFIX,
    ANALYSIS_DOCUMENTATION_PREFIX,
    ANALYSIS_PREFIX,
    ANALYSIS_VARIABLE_MAPPING_PREFIX,
    ANALYSIS_TAX_YEAR_MAX,
    ANALYSIS_TAX_YEAR_MIN,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    DEFAULT_STATE_CODES,
    IRS_BMF_BASE_URL,
    META_DIR,
    STAGING_DIR,
    analysis_data_processing_doc_path,
    analysis_field_metrics_output_path,
    analysis_geography_metrics_output_path,
    analysis_variable_coverage_path,
    analysis_variable_mapping_path,
    analysis_variables_output_path,
    banner,
    combined_filtered_output_path,
    load_env_from_secrets,
    print_elapsed,
    state_source_url,
    yearly_filtered_output_path,
)
from ingest._shared.analysis_support import (
    DEFAULT_BMF_STAGING_DIR,
    apply_imputed_flag_family,
    apply_classification_fallbacks,
    build_ntee_proxy_flag,
    build_political_proxy_flag,
    load_bmf_classification_lookup,
)
from utils.paths import DATA as DATA_ROOT


_REPO_ROOT = Path(__file__).resolve().parents[3]


def _portable_path(path: Path) -> str:
    """Render generated documentation paths without machine-local prefixes."""
    resolved = path.resolve()
    try:
        return f"SWB_321_DATA_ROOT/{resolved.relative_to(DATA_ROOT.resolve()).as_posix()}"
    except ValueError:
        pass
    try:
        return resolved.relative_to(_REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


UNAVAILABLE_VARIABLES = [
    {"canonical_variable": "analysis_program_service_revenue_amount", "variable_role": "unavailable", "analysis_requirement": "Program service revenue", "notes": "IRS EO BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_calculated_total_contributions_amount", "variable_role": "unavailable", "analysis_requirement": "Total contributions", "notes": "IRS EO BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_other_contributions_amount", "variable_role": "unavailable", "analysis_requirement": "Other contributions", "notes": "IRS EO BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_calculated_grants_total_amount", "variable_role": "unavailable", "analysis_requirement": "Grants (total amount)", "notes": "IRS EO BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_total_expense_amount", "variable_role": "unavailable", "analysis_requirement": "Total expense", "notes": "IRS EO BMF does not provide a direct expense field."},
    {"canonical_variable": "analysis_net_asset_amount", "variable_role": "unavailable", "analysis_requirement": "Net asset", "notes": "IRS EO BMF does not provide a direct net-asset field."},
    {"canonical_variable": "analysis_calculated_surplus_amount", "variable_role": "unavailable", "analysis_requirement": "Surplus", "notes": "IRS EO BMF does not provide a revenue-minus-expense surplus field."},
    {"canonical_variable": "analysis_calculated_net_margin_ratio", "variable_role": "unavailable", "analysis_requirement": "Net margin", "notes": "IRS EO BMF cannot calculate net margin without total expenses."},
    {"canonical_variable": "analysis_calculated_months_of_reserves", "variable_role": "unavailable", "analysis_requirement": "Months of reserves", "notes": "IRS EO BMF cannot calculate months of reserves without expenses and a direct net-asset field."},
]

AVAILABLE_VARIABLE_METADATA = [
    {"canonical_variable": "analysis_total_revenue_amount", "variable_role": "direct", "analysis_requirement": "Total revenue", "source_rule": "Direct IRS EO BMF REVENUE_AMT field from the filtered benchmark rows", "provenance_column": "analysis_total_revenue_amount_source_column", "notes": "Source-direct IRS EO BMF amount retained as nullable numeric."},
    {"canonical_variable": "analysis_total_assets_amount", "variable_role": "direct", "analysis_requirement": "Total assets", "source_rule": "Direct IRS EO BMF ASSET_AMT field from the filtered benchmark rows", "provenance_column": "analysis_total_assets_amount_source_column", "notes": "Source-direct IRS EO BMF amount retained as nullable numeric."},
    {"canonical_variable": "analysis_total_income_amount", "variable_role": "direct", "analysis_requirement": "Total income", "source_rule": "Direct IRS EO BMF INCOME_AMT field from the filtered benchmark rows", "provenance_column": "analysis_total_income_amount_source_column", "notes": "Source-direct IRS EO BMF amount retained as nullable numeric."},
    {"canonical_variable": "analysis_ntee_code", "variable_role": "direct", "analysis_requirement": "NTEE filed classification code", "source_rule": "Direct IRS EO BMF NTEE_CD field from the filtered benchmark rows", "provenance_column": "analysis_ntee_code_source_column", "notes": "Source-direct IRS EO BMF classification retained without overwrite."},
    {"canonical_variable": "analysis_subsection_code", "variable_role": "direct", "analysis_requirement": "Subsection code", "source_rule": "Direct IRS EO BMF SUBSECTION field from the filtered benchmark rows", "provenance_column": "analysis_subsection_code_source_column", "notes": "Source-direct IRS EO BMF subsection retained without overwrite."},
    {"canonical_variable": "analysis_ntee_code_fallback_enriched", "variable_role": "enriched", "analysis_requirement": "NTEE filed classification code", "source_rule": "IRS EO BMF direct NTEE first, then NCCS BMF exact-year, then NCCS BMF nearest-year fallback", "provenance_column": "analysis_ntee_code_fallback_enriched_source_column", "notes": "Analyst-facing resolved NTEE code with explicit IRS-first precedence."},
    {"canonical_variable": "analysis_subsection_code_fallback_enriched", "variable_role": "enriched", "analysis_requirement": "Subsection code", "source_rule": "IRS EO BMF direct subsection first, then NCCS BMF exact-year, then NCCS BMF nearest-year fallback", "provenance_column": "analysis_subsection_code_fallback_enriched_source_column", "notes": "Analyst-facing resolved subsection code with explicit IRS-first precedence."},
    {"canonical_variable": "analysis_missing_classification_flag", "variable_role": "calculated", "analysis_requirement": "", "source_rule": "True when analysis_ntee_code_fallback_enriched remains null after IRS-first plus NCCS fallback resolution", "provenance_column": "analysis_missing_classification_flag_source_column", "notes": "Final unresolved-classification flag for analyst filtering."},
    {"canonical_variable": "analysis_calculated_ntee_broad_code", "variable_role": "calculated", "analysis_requirement": "Broad NTEE field classification code", "source_rule": "First letter of analysis_ntee_code_fallback_enriched", "provenance_column": "analysis_calculated_ntee_broad_code_source_column", "notes": "Broad field representation metric built from the analyst-facing resolved NTEE field."},
    {"canonical_variable": "analysis_is_hospital", "variable_role": "proxy", "analysis_requirement": "Hospital flag", "source_rule": "NTEE proxy from analysis_ntee_code_fallback_enriched", "provenance_column": "analysis_is_hospital_source_column", "notes": "Analyst-facing hospital proxy from the resolved NTEE field."},
    {"canonical_variable": "analysis_is_university", "variable_role": "proxy", "analysis_requirement": "University flag", "source_rule": "NTEE proxy from analysis_ntee_code_fallback_enriched", "provenance_column": "analysis_is_university_source_column", "notes": "Analyst-facing university proxy from the resolved NTEE field."},
    {"canonical_variable": "analysis_is_political_org", "variable_role": "proxy", "analysis_requirement": "Political organization flag", "source_rule": "Subsection proxy from analysis_subsection_code_fallback_enriched", "provenance_column": "analysis_is_political_org_source_column", "notes": "Analyst-facing political proxy from the resolved subsection field."},
    {"canonical_variable": "analysis_imputed_is_hospital", "variable_role": "imputed", "analysis_requirement": "Hospital flag", "source_rule": "analysis_is_hospital, then conservative name fallback, then default false", "provenance_column": "analysis_imputed_is_hospital_source_column", "notes": "Complete hospital flag for analysis exclusions."},
    {"canonical_variable": "analysis_imputed_is_university", "variable_role": "imputed", "analysis_requirement": "University flag", "source_rule": "analysis_is_university, then conservative name fallback, then default false", "provenance_column": "analysis_imputed_is_university_source_column", "notes": "Complete university flag for analysis exclusions."},
    {"canonical_variable": "analysis_imputed_is_political_org", "variable_role": "imputed", "analysis_requirement": "Political organization flag", "source_rule": "analysis_is_political_org, then conservative name fallback, then default false", "provenance_column": "analysis_imputed_is_political_org_source_column", "notes": "Complete political-organization flag for analysis exclusions."},
]


def _ensure_parent_dirs(*paths: Path) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path.parent}", flush=True)


def _build_broad_ntee(ntee_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    broad = ntee_series.astype("string").fillna("").str.upper().str.slice(0, 1)
    broad = broad.mask(broad.eq(""), pd.NA)
    provenance = pd.Series(pd.NA, index=ntee_series.index, dtype="string")
    provenance.loc[broad.notna()] = "analysis_ntee_code"
    return broad, provenance


def _build_geography_metrics_output(analysis_df: pd.DataFrame) -> pd.DataFrame:
    scope_specs = [
        ("all_rows", pd.Series(True, index=analysis_df.index)),
        (
            "exclude_imputed_hospital_university_political_org",
            ~(
                analysis_df["analysis_imputed_is_hospital"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_university"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_political_org"].fillna(False).astype(bool)
            ),
        ),
    ]
    rows: list[dict[str, Any]] = []
    for exclusion_variant, scope_mask in tqdm(scope_specs, desc="irs_bmf geography scopes", unit="scope"):
        scoped_df = analysis_df.loc[scope_mask].copy()
        for geography_level, group_cols in [("region", ["region", "tax_year"]), ("county", ["region", "county_fips", "tax_year"])]:
            grouped = scoped_df.groupby(group_cols, dropna=False, sort=True)
            for keys, group_df in grouped:
                if geography_level == "region":
                    region, tax_year = keys
                    county_fips = pd.NA
                else:
                    region, county_fips, tax_year = keys
                revenue_sum = group_df["analysis_total_revenue_amount"].sum(min_count=1)
                assets_sum = group_df["analysis_total_assets_amount"].sum(min_count=1)
                income_sum = group_df["analysis_total_income_amount"].sum(min_count=1)
                nonprofit_count = int(len(group_df))
                unique_ein_count = int(group_df["ein"].dropna().astype("string").nunique())
                rows.append(
                    {
                        "geography_level": geography_level,
                        "region": region,
                        "county_fips": county_fips,
                        "tax_year": tax_year,
                        "analysis_exclusion_variant": exclusion_variant,
                        "nonprofit_count": nonprofit_count,
                        "analysis_total_nonprofit_count": nonprofit_count,
                        "unique_nonprofit_ein_count": unique_ein_count,
                        "analysis_total_revenue_amount_sum": revenue_sum if pd.notna(revenue_sum) else pd.NA,
                        "analysis_total_assets_amount_sum": assets_sum if pd.notna(assets_sum) else pd.NA,
                        "analysis_total_income_amount_sum": income_sum if pd.notna(income_sum) else pd.NA,
                        "analysis_calculated_normalized_total_revenue_per_nonprofit": (float(revenue_sum) / nonprofit_count) if nonprofit_count and pd.notna(revenue_sum) else pd.NA,
                        "analysis_calculated_normalized_total_assets_per_nonprofit": (float(assets_sum) / nonprofit_count) if nonprofit_count and pd.notna(assets_sum) else pd.NA,
                        "analysis_calculated_normalized_total_revenue_per_unique_nonprofit": (float(revenue_sum) / unique_ein_count) if unique_ein_count and pd.notna(revenue_sum) else pd.NA,
                        "analysis_calculated_normalized_total_assets_per_unique_nonprofit": (float(assets_sum) / unique_ein_count) if unique_ein_count and pd.notna(assets_sum) else pd.NA,
                        "analysis_imputed_is_hospital_true_count": int(group_df["analysis_imputed_is_hospital"].fillna(False).astype(bool).sum()),
                        "analysis_imputed_is_university_true_count": int(group_df["analysis_imputed_is_university"].fillna(False).astype(bool).sum()),
                        "analysis_imputed_is_political_org_true_count": int(group_df["analysis_imputed_is_political_org"].fillna(False).astype(bool).sum()),
                    }
                )
    return pd.DataFrame(rows).sort_values(
        ["geography_level", "region", "county_fips", "tax_year", "analysis_exclusion_variant"],
        kind="mergesort",
    ).reset_index(drop=True)


def _build_field_metrics_output(analysis_df: pd.DataFrame) -> pd.DataFrame:
    scope_specs = [
        ("all_rows", pd.Series(True, index=analysis_df.index)),
        (
            "exclude_imputed_hospital_university_political_org",
            ~(
                analysis_df["analysis_imputed_is_hospital"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_university"].fillna(False).astype(bool)
                | analysis_df["analysis_imputed_is_political_org"].fillna(False).astype(bool)
            ),
        ),
    ]
    rows: list[dict[str, Any]] = []
    for exclusion_variant, scope_mask in tqdm(scope_specs, desc="irs_bmf field scopes", unit="scope"):
        scoped_df = analysis_df.loc[scope_mask].copy()
        region_year_totals = (
            scoped_df.groupby(["region", "tax_year"], dropna=False, sort=True)
            .agg(
                region_org_count=("ein", "size"),
                region_revenue_sum=("analysis_total_revenue_amount", lambda s: s.sum(min_count=1)),
                region_assets_sum=("analysis_total_assets_amount", lambda s: s.sum(min_count=1)),
            )
            .reset_index()
        )
        grouped = scoped_df.groupby(["region", "tax_year", "analysis_calculated_ntee_broad_code"], dropna=False, sort=True)
        for keys, group_df in grouped:
            region, tax_year, broad_ntee = keys
            revenue_sum = group_df["analysis_total_revenue_amount"].sum(min_count=1)
            assets_sum = group_df["analysis_total_assets_amount"].sum(min_count=1)
            org_count = int(len(group_df))
            totals_row = region_year_totals.loc[
                region_year_totals["region"].astype("string").eq(str(region))
                & region_year_totals["tax_year"].astype("string").eq(str(tax_year))
            ].iloc[0]
            region_org_count = int(totals_row["region_org_count"])
            region_revenue_sum = totals_row["region_revenue_sum"]
            region_assets_sum = totals_row["region_assets_sum"]
            rows.append(
                {
                    "region": region,
                    "tax_year": tax_year,
                    "analysis_exclusion_variant": exclusion_variant,
                    "analysis_calculated_ntee_broad_code": broad_ntee,
                    "nonprofit_count": org_count,
                    "analysis_total_nonprofit_count": org_count,
                    "analysis_total_revenue_amount_sum": revenue_sum if pd.notna(revenue_sum) else pd.NA,
                    "analysis_total_assets_amount_sum": assets_sum if pd.notna(assets_sum) else pd.NA,
                    "analysis_calculated_share_of_region_nonprofit_count": (org_count / region_org_count) if region_org_count else pd.NA,
                    "analysis_calculated_share_of_region_total_nonprofit_count": (org_count / region_org_count) if region_org_count else pd.NA,
                    "analysis_calculated_share_of_region_revenue": (float(revenue_sum) / float(region_revenue_sum)) if pd.notna(revenue_sum) and pd.notna(region_revenue_sum) and float(region_revenue_sum) != 0 else pd.NA,
                    "analysis_calculated_share_of_region_total_revenue": (float(revenue_sum) / float(region_revenue_sum)) if pd.notna(revenue_sum) and pd.notna(region_revenue_sum) and float(region_revenue_sum) != 0 else pd.NA,
                    "analysis_calculated_share_of_region_assets": (float(assets_sum) / float(region_assets_sum)) if pd.notna(assets_sum) and pd.notna(region_assets_sum) and float(region_assets_sum) != 0 else pd.NA,
                    "analysis_calculated_share_of_region_total_assets": (float(assets_sum) / float(region_assets_sum)) if pd.notna(assets_sum) and pd.notna(region_assets_sum) and float(region_assets_sum) != 0 else pd.NA,
                }
            )
    return pd.DataFrame(rows).sort_values(
        ["region", "tax_year", "analysis_exclusion_variant", "analysis_calculated_ntee_broad_code"],
        kind="mergesort",
    ).reset_index(drop=True)


def _write_variable_coverage_csv(analysis_df: pd.DataFrame, output_path: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for year in sorted(analysis_df["tax_year"].dropna().astype("string").unique().tolist()):
        year_df = analysis_df.loc[analysis_df["tax_year"].astype("string").eq(year)].copy()
        row_count = int(len(year_df))
        for metadata in AVAILABLE_VARIABLE_METADATA + UNAVAILABLE_VARIABLES:
            canonical_variable = metadata["canonical_variable"]
            populated_count = int(year_df[canonical_variable].notna().sum()) if canonical_variable in year_df.columns else 0
            rows.append(
                {
                    "canonical_variable": canonical_variable,
                    "availability_status": "available" if canonical_variable in year_df.columns else "unavailable",
                    "variable_role": metadata["variable_role"],
                    "analysis_requirement": metadata.get("analysis_requirement", ""),
                    "tax_year": year,
                    "row_count": row_count,
                    "populated_count": populated_count,
                    "fill_rate": (populated_count / row_count) if row_count else pd.NA,
                    "notes": metadata.get("notes", ""),
                }
            )
    coverage_df = pd.DataFrame(rows).sort_values(["canonical_variable", "tax_year"], kind="mergesort").reset_index(drop=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    coverage_df.to_csv(output_path, index=False)
    print(f"[analysis] Wrote coverage CSV: {output_path}", flush=True)
    return coverage_df


def _write_variable_mapping_md(output_path: Path) -> None:
    lines = [
        "# IRS EO BMF Analysis Variable Mapping",
        "",
        "This file documents the official IRS EO BMF analysis-ready variables.",
        "All paths are repo-relative and the package is intentionally source-direct to IRS EO BMF.",
        "",
        "## Coverage Evidence",
        "",
        f"- Generated coverage CSV: `{_portable_path(analysis_variable_coverage_path())}`",
        f"- Published S3 variable mapping object: `{ANALYSIS_VARIABLE_MAPPING_PREFIX}/{analysis_variable_mapping_path().name}`",
        f"- Published S3 coverage object: `{ANALYSIS_COVERAGE_PREFIX}/{analysis_variable_coverage_path().name}`",
        "- Coverage CSVs are generated by the analysis step and published as quality evidence.",
        "",
        "|canonical_variable|role|analysis_requirement|source_rule|provenance_column|notes|",
        "|---|---|---|---|---|---|",
    ]
    for metadata in AVAILABLE_VARIABLE_METADATA:
        lines.append(
            "|{canonical_variable}|{variable_role}|{analysis_requirement}|{source_rule}|{provenance_column}|{notes}|".format(
                **{k: str(v).replace("|", "\\|") for k, v in metadata.items()}
            )
        )
    for metadata in UNAVAILABLE_VARIABLES:
        lines.append(
            "|{canonical_variable}|{variable_role}|{analysis_requirement}|Unavailable|n/a|{notes}|".format(
                **{k: str(v).replace("|", "\\|") for k, v in metadata.items()}
            )
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[analysis] Wrote mapping markdown: {output_path}", flush=True)


def _write_data_processing_doc(output_path: Path) -> None:
    doc = f"""# IRS EO BMF Pipeline

## Status

This document is the authoritative technical reference for the current IRS EO BMF pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/irs_bmf/`, including the current benchmark-analysis scope of `2022-2024`, the filter-first benchmark build in step 04, the row-level analysis extraction in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the IRS EO BMF data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw state CSVs become benchmark-filtered yearly outputs
- how the final IRS EO BMF analysis package is derived from those filtered outputs
- what is source-direct, fallback-enriched, proxy, imputed, or intentionally left missing

## Pipeline At A Glance

The IRS EO BMF pipeline is a registry, classification, and support-data pipeline. It is not a filing pipeline like GT, and it is not a clean yearly release pipeline like NCCS BMF.

At a high level, the pipeline has these phases:

1. Download raw state-level IRS EO BMF CSVs.
2. Upload the raw state files and metadata to Bronze S3.
3. Strictly verify source/local/S3 byte parity for raw state inputs.
4. Filter each state file to benchmark geography, derive tax year from `TAX_PERIOD`, combine only retained rows, and resolve cross-state duplicate EIN-year groups.
5. Upload the combined and yearly filtered benchmark outputs to Silver S3.
6. Verify local-vs-S3 byte parity for the filtered benchmark outputs.
7. Build the final IRS EO BMF analysis outputs from the filtered benchmark parquets only.
8. Upload the final analysis artifacts and metadata to the dedicated IRS EO BMF analysis prefix.

The main artifact classes are:

- Raw data: one IRS EO BMF CSV per state.
- Metadata/manifests: raw manifest, filter manifest, verification reports, coverage CSV, mapping Markdown, and this processing doc.
- Silver filtered artifacts: one combined filtered parquet plus one benchmark parquet per retained analysis year.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, field metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows a filtered-only combine contract (combine stages use filtered inputs only):

- raw state files are filtered to benchmark geography before any combine stage
- the final analysis step reads only the filtered benchmark outputs
- direct IRS classification is preserved while analyst-facing fallback-enriched classification remains explicit and auditable

## Contract

- The final analysis-ready layer is limited to tax years `{ANALYSIS_TAX_YEAR_MIN}-{ANALYSIS_TAX_YEAR_MAX}` inferred from `TAX_PERIOD`.
- Missing financial amounts remain null; they are not coerced to zero.
- `analysis_ntee_code` and `analysis_subsection_code` remain source-direct IRS EO BMF fields.
- `analysis_ntee_code_fallback_enriched` and `analysis_subsection_code_fallback_enriched` are analyst-facing enriched fields that keep IRS first, then use NCCS BMF exact-year and nearest-year fallback only when the direct IRS value is blank.

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_fetch_bmf_release.py` | Download raw state CSVs and write a raw manifest | IRS EO BMF state URLs | Raw state CSVs and manifest | HTTP |
| 02 | `02_upload_bmf_release_to_s3.py` | Upload raw state CSVs and manifest to Bronze | Local raw state CSVs and manifest | Bronze raw IRS EO BMF objects | S3 |
| 03 | `03_verify_bmf_source_local_s3.py` | Verify source/local/S3 raw bytes | Source sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 04 | `04_filter_bmf_to_benchmark_local.py` | Filter state CSVs to benchmark scope, derive tax year, combine retained rows, resolve cross-state EIN-year duplicates, and write benchmark parquets | Raw state CSVs plus geography crosswalks | Combined filtered parquet, yearly filtered parquets, filter manifest | local files |
| 05 | `05_upload_filtered_bmf_to_s3.py` | Upload combined and yearly filtered benchmark outputs to Silver | Filtered benchmark outputs and metadata | Silver filtered IRS EO BMF objects | S3 |
| 06 | `06_verify_filtered_bmf_local_s3.py` | Verify local-vs-S3 bytes for filtered outputs | Local filtered outputs and Silver objects | Filtered verification CSV | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build the analysis rowset, geography metrics, field metrics, coverage CSV, mapping doc, and this processing doc | Filtered yearly benchmark parquets plus NCCS BMF lookups | Official IRS EO BMF analysis outputs | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload the analysis artifacts and verify bytes | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

## Exact Source Locations

- IRS EO BMF base URL: `{IRS_BMF_BASE_URL}`
- Configured benchmark-state files:
{chr(10).join(f"  - `{state}`: `{state_source_url(state)}`" for state in DEFAULT_STATE_CODES)}

## Raw Source Dictionaries

- Official IRS EO BMF dictionary: `documentation/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`
- Local client package copy: `docs/final_preprocessing_docs/technical_docs/source_dictionaries/irs_eo_bmf/eo-info.pdf`
- Applies to the raw IRS EO BMF state CSVs used by this pipeline.

### Step 01: state-file retrieval

The raw source is one IRS EO BMF state CSV per file.

The fetch step downloads the configured state files and writes a raw manifest that records:

- the state code
- the raw filename
- the source URL
- source and local byte metadata

### Step 02 and Step 03: Bronze upload and raw verification

After fetch, the pipeline uploads the raw state files and manifest to Bronze and then verifies source/local/S3 byte parity.

This preserves:

- local raw state files
- Bronze raw copies
- a verification CSV documenting whether source, local, and S3 bytes agree

## Where Data Is Stored

### Local artifacts

- Combined filtered parquet: `{_portable_path(combined_filtered_output_path())}`
- Analysis parquet: `{_portable_path(analysis_variables_output_path())}`
- Geography metrics parquet: `{_portable_path(analysis_geography_metrics_output_path())}`
- Field metrics parquet: `{_portable_path(analysis_field_metrics_output_path())}`
- Coverage CSV: `{_portable_path(analysis_variable_coverage_path())}`
- Mapping Markdown: `{_portable_path(analysis_variable_mapping_path())}`
- Data-processing Markdown: `{_portable_path(analysis_data_processing_doc_path())}`

### S3 layout

- Raw prefix: `bronze/irs990/bmf`
- Raw metadata prefix: `bronze/irs990/bmf/metadata`
- Filtered benchmark prefix: `silver/irs990/bmf`
- Filtered metadata prefix: `silver/irs990/bmf/metadata`
- Analysis prefix: `{ANALYSIS_PREFIX}`
- Analysis documentation prefix: `{ANALYSIS_DOCUMENTATION_PREFIX}`
- Analysis variable mapping prefix: `{ANALYSIS_VARIABLE_MAPPING_PREFIX}`
- Analysis coverage prefix: `{ANALYSIS_COVERAGE_PREFIX}`

## Step-By-Step Transformation

### Step 04: filter first, then combine

Step 04 is the key upstream curation step.

For each raw state file, it:

1. normalizes ZIP and state fields
2. filters rows to benchmark counties and regions
3. derives `tax_year` from `TAX_PERIOD`
4. discards rows outside the analysis-ready year scope when appropriate
5. appends only the retained benchmark rows into the combined filtered dataset

This means the final combined filtered parquet is built only from already-admitted rows.

### Cross-state duplicate EIN-year resolution

After filtering, the combined retained rowset may still contain cross-state duplicate `ein + tax_year` groups.

These are resolved upstream in the filtered benchmark build by preferring:

1. the row whose state best matches the benchmark geography state
2. the row with the richest direct field population
3. a stable lexical tie-break on source file name

The final analysis build expects zero duplicate `ein + tax_year` groups in the filtered yearly benchmark outputs.

### Filtered benchmark outputs

The pipeline writes:

- one combined filtered IRS EO BMF parquet
- one filtered yearly benchmark parquet per retained tax year
- a filter manifest describing counts by state, year derivation, and duplicate resolution

These filtered artifacts are the stable upstream inputs for the final analysis build.

### Step 07: analysis build from filtered outputs only

The analysis step reads only the filtered yearly benchmark parquets for tax years `{ANALYSIS_TAX_YEAR_MIN}-{ANALYSIS_TAX_YEAR_MAX}`.

It does not reopen the raw state CSVs.

The final row grain is one row per resolved `ein + tax_year`.

### Direct financial and classification extraction

The source-direct IRS EO BMF fields are:

- `analysis_total_revenue_amount`
- `analysis_total_assets_amount`
- `analysis_total_income_amount`
- `analysis_ntee_code`
- `analysis_subsection_code`

These fields preserve their direct IRS provenance columns. Missing source values remain null rather than being coerced to zero or overwritten.

### Analyst-facing fallback-enriched classification fields

For analysis convenience, the pipeline also builds:

- `analysis_ntee_code_fallback_enriched`
- `analysis_subsection_code_fallback_enriched`

Their fallback order is:

1. direct IRS EO BMF value
2. exact-year NCCS BMF
3. nearest-year NCCS BMF

This means the package keeps both:

- the source-direct IRS classification fields
- the more complete analyst-facing resolved classification fields

### Proxy and imputed flags

The source-backed or proxy exclusion flags are:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

These are derived from the fallback-enriched classification fields, not from the direct-only IRS fields.

The complete analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed fields use the same conservative name-based helper pattern used in the other source-specific analysis packages.

### Geography metrics

The geography metrics parquet aggregates the row-level analysis dataset by:

- geography level
- region
- optional county FIPS
- tax year
- exclusion variant

It includes counts, unique EIN counts, financial sums, normalized totals, and counts of imputed exclusion flags.

### Field metrics

The field metrics parquet aggregates by:

- region
- tax year
- exclusion variant
- `analysis_calculated_ntee_broad_code`

It reports organization count, financial sums, and share-of-region metrics to support field-composition analysis.

### Coverage, mapping, and upload verification

The coverage CSV records fill rates for direct IRS fields, fallback-enriched classification fields, and the proxy/imputed exclusion flags.

## Coverage Evidence

- Generated coverage CSV: `{_portable_path(analysis_variable_coverage_path())}`
- Published S3 documentation object: `{ANALYSIS_DOCUMENTATION_PREFIX}/{analysis_data_processing_doc_path().name}`
- Published S3 variable mapping object: `{ANALYSIS_VARIABLE_MAPPING_PREFIX}/{analysis_variable_mapping_path().name}`
- Published S3 coverage object: `{ANALYSIS_COVERAGE_PREFIX}/{analysis_variable_coverage_path().name}`
- Coverage CSVs are generated by step 07 and published as authoritative quality evidence.

The mapping Markdown documents which fields are:

- direct
- enriched
- calculated
- proxy
- imputed
- unavailable

The upload step verifies local-vs-S3 bytes for every analysis artifact before accepting the upload as complete.

## Final Analysis Outputs

The official IRS EO BMF analysis package consists of:

- one row-level analysis parquet
- one geography metrics parquet
- one field metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is intended as a registry, geography, and classification support layer rather than a full filing-detail pipeline.

## Testing And Verification

- Unit tests live in `tests/irs_bmf/test_irs_bmf_common.py`.
- The tests cover filter-before-combine behavior, tax-year derivation, duplicate resolution, direct extraction, fallback-enriched classification, proxy/imputed flags, and upload registration.
- Operational verification includes raw source/local/S3 byte checks, filtered benchmark verification, and analysis upload byte checks.

## Current Caveats

- IRS EO BMF is a state-snapshot registry source, not a filing-level dataset.
- `tax_year` is inferred from `TAX_PERIOD`, so its temporal semantics are weaker than a true annual filing pipeline.
- Financial completeness is weaker than NCCS BMF and much weaker than GT or efile.
- `analysis_ntee_code_fallback_enriched` is the analyst-facing resolved NTEE field; `analysis_ntee_code` remains intentionally source-direct and may be sparser.

## Analysis requirement alignment appendix

| analysis_requirement | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Direct IRS EO BMF amount field |
| Total assets | analysis_total_assets_amount | direct | Direct IRS EO BMF amount field |
| Total income | analysis_total_income_amount | direct | Direct IRS EO BMF amount field |
| NTEE filed classification code | analysis_ntee_code | direct | Direct IRS EO BMF NTEE field |
| NTEE filed classification code (analysis-ready resolved) | analysis_ntee_code_fallback_enriched | enriched | IRS first, then NCCS BMF exact-year, then NCCS BMF nearest-year |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of the fallback-enriched NTEE code |
| Hospital flag | analysis_is_hospital | proxy | NTEE proxy from fallback-enriched classification |
| University flag | analysis_is_university | proxy | NTEE proxy from fallback-enriched classification |
| Political organization flag | analysis_is_political_org | proxy | Subsection proxy from fallback-enriched classification |
| Total expense | analysis_total_expense_amount | unavailable | IRS EO BMF does not provide this expense concept in the current pipeline |
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(doc.strip() + "\n", encoding="utf-8")
    print(f"[analysis] Wrote data-processing markdown: {output_path}", flush=True)


def build_analysis_outputs(
    *,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
    bmf_staging_dir: Path = DEFAULT_BMF_STAGING_DIR,
    coverage_path: Path | None = None,
    mapping_path: Path | None = None,
    processing_doc_path: Path | None = None,
) -> dict[str, Any]:
    """Build the canonical IRS EO BMF analysis outputs from filtered yearly parquets."""
    analysis_variables_path = analysis_variables_output_path(staging_dir)
    geography_metrics_path = analysis_geography_metrics_output_path(staging_dir)
    field_metrics_path = analysis_field_metrics_output_path(staging_dir)
    coverage_path = coverage_path or analysis_variable_coverage_path(metadata_dir)
    mapping_path = mapping_path or analysis_variable_mapping_path()
    processing_doc_path = processing_doc_path or analysis_data_processing_doc_path()
    _ensure_parent_dirs(analysis_variables_path, geography_metrics_path, field_metrics_path, coverage_path, mapping_path, processing_doc_path)

    frames: list[pd.DataFrame] = []
    for year in range(ANALYSIS_TAX_YEAR_MIN, ANALYSIS_TAX_YEAR_MAX + 1):
        path = yearly_filtered_output_path(year, staging_dir)
        if not path.exists():
            raise FileNotFoundError(f"Missing filtered IRS EO BMF benchmark parquet: {path}")
        year_df = pd.read_parquet(path).copy()
        print(f"[analysis] Loaded year={year} filtered benchmark rows: {len(year_df):,}", flush=True)
        frames.append(year_df)

    source_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    duplicate_group_count = int((source_df.groupby(["ein", "tax_year"]).size() > 1).sum()) if not source_df.empty else 0
    if duplicate_group_count:
        raise RuntimeError(
            "Filtered IRS EO BMF benchmark outputs still contain duplicate ein + tax_year groups. "
            "The upstream filtered combine step should resolve these before analysis."
        )

    analysis_df = source_df.copy()
    analysis_df["analysis_total_revenue_amount"] = analysis_df["raw_total_revenue_amount"].astype("Float64")
    analysis_df["analysis_total_assets_amount"] = analysis_df["raw_total_assets_amount"].astype("Float64")
    analysis_df["analysis_total_income_amount"] = analysis_df["raw_total_income_amount"].astype("Float64")
    analysis_df["analysis_total_revenue_amount_source_column"] = analysis_df["raw_total_revenue_amount_source_column"].astype("string")
    analysis_df["analysis_total_assets_amount_source_column"] = analysis_df["raw_total_assets_amount_source_column"].astype("string")
    analysis_df["analysis_total_income_amount_source_column"] = analysis_df["raw_total_income_amount_source_column"].astype("string")

    analysis_df["analysis_ntee_code"] = analysis_df["raw_ntee_code"].astype("string")
    analysis_df["analysis_ntee_code_source_family"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df.loc[analysis_df["analysis_ntee_code"].notna(), "analysis_ntee_code_source_family"] = "irs_bmf"
    analysis_df["analysis_ntee_code_source_variant"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df.loc[analysis_df["analysis_ntee_code"].notna(), "analysis_ntee_code_source_variant"] = analysis_df.loc[analysis_df["analysis_ntee_code"].notna(), "source_state_code"].astype("string")
    analysis_df["analysis_ntee_code_source_column"] = analysis_df["raw_ntee_code_source_column"].astype("string")

    analysis_df["analysis_subsection_code"] = analysis_df["raw_subsection_code"].astype("string")
    analysis_df["analysis_subsection_code_source_family"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df.loc[analysis_df["analysis_subsection_code"].notna(), "analysis_subsection_code_source_family"] = "irs_bmf"
    analysis_df["analysis_subsection_code_source_variant"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df.loc[analysis_df["analysis_subsection_code"].notna(), "analysis_subsection_code_source_variant"] = analysis_df.loc[analysis_df["analysis_subsection_code"].notna(), "source_state_code"].astype("string")
    analysis_df["analysis_subsection_code_source_column"] = analysis_df["raw_subsection_code_source_column"].astype("string")

    bmf_lookup_df = load_bmf_classification_lookup(
        required_tax_years=sorted(analysis_df["tax_year"].dropna().astype("string").unique().tolist()),
        bmf_staging_dir=bmf_staging_dir,
    )
    fallback_df = analysis_df[["ein", "tax_year", "state"]].copy()
    fallback_df["analysis_ntee_code"] = pd.Series(pd.NA, index=fallback_df.index, dtype="string")
    fallback_df["analysis_subsection_code"] = pd.Series(pd.NA, index=fallback_df.index, dtype="string")
    fallback_summary = apply_classification_fallbacks(
        fallback_df,
        analysis_df[["ein", "tax_year", "state"]].copy(),
        state_column="state",
        bmf_lookup_df=bmf_lookup_df,
        irs_lookup_df=pd.DataFrame(
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
    print(f"[analysis] NCCS fallback summary for IRS EO BMF package: {fallback_summary}", flush=True)

    analysis_df["analysis_ntee_code_fallback_enriched"] = analysis_df["analysis_ntee_code"].combine_first(fallback_df["analysis_ntee_code"]).astype("string")
    analysis_df["analysis_ntee_code_fallback_enriched_source_family"] = analysis_df["analysis_ntee_code_source_family"].combine_first(fallback_df["analysis_ntee_code_source_family"]).astype("string")
    analysis_df["analysis_ntee_code_fallback_enriched_source_variant"] = analysis_df["analysis_ntee_code_source_variant"].combine_first(fallback_df["analysis_ntee_code_source_variant"]).astype("string")
    analysis_df["analysis_ntee_code_fallback_enriched_source_column"] = analysis_df["analysis_ntee_code_source_column"].combine_first(fallback_df["analysis_ntee_code_source_column"]).astype("string")

    analysis_df["analysis_subsection_code_fallback_enriched"] = analysis_df["analysis_subsection_code"].combine_first(fallback_df["analysis_subsection_code"]).astype("string")
    analysis_df["analysis_subsection_code_fallback_enriched_source_family"] = analysis_df["analysis_subsection_code_source_family"].combine_first(fallback_df["analysis_subsection_code_source_family"]).astype("string")
    analysis_df["analysis_subsection_code_fallback_enriched_source_variant"] = analysis_df["analysis_subsection_code_source_variant"].combine_first(fallback_df["analysis_subsection_code_source_variant"]).astype("string")
    analysis_df["analysis_subsection_code_fallback_enriched_source_column"] = analysis_df["analysis_subsection_code_source_column"].combine_first(fallback_df["analysis_subsection_code_source_column"]).astype("string")

    analysis_df["analysis_missing_classification_flag"] = analysis_df["analysis_ntee_code_fallback_enriched"].isna().astype("boolean")
    analysis_df["analysis_missing_classification_flag_source_column"] = pd.Series("analysis_ntee_code_fallback_enriched", index=analysis_df.index, dtype="string")
    analysis_df["analysis_calculated_ntee_broad_code"], analysis_df["analysis_calculated_ntee_broad_code_source_column"] = _build_broad_ntee(analysis_df["analysis_ntee_code_fallback_enriched"])
    analysis_df["analysis_is_hospital"], analysis_df["analysis_is_hospital_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code_fallback_enriched"], ("E20", "E21", "E22", "E24"), provenance_label="analysis_ntee_code_fallback_enriched")
    analysis_df["analysis_is_university"], analysis_df["analysis_is_university_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code_fallback_enriched"], ("B40", "B41", "B42", "B43", "B50"), provenance_label="analysis_ntee_code_fallback_enriched")
    analysis_df["analysis_is_political_org"], analysis_df["analysis_is_political_org_source_column"] = build_political_proxy_flag(analysis_df["analysis_subsection_code_fallback_enriched"], provenance_label="analysis_subsection_code_fallback_enriched")
    apply_imputed_flag_family(analysis_df, name_column="org_name")

    analysis_df = analysis_df[
        [
            "ein",
            "tax_year",
            "org_name",
            "state",
            "zip5",
            "county_fips",
            "region",
            "tax_period",
            "source_state_code",
            "source_state_file",
            "analysis_total_revenue_amount",
            "analysis_total_assets_amount",
            "analysis_total_income_amount",
            "analysis_total_revenue_amount_source_column",
            "analysis_total_assets_amount_source_column",
            "analysis_total_income_amount_source_column",
            "analysis_ntee_code",
            "analysis_ntee_code_source_family",
            "analysis_ntee_code_source_variant",
            "analysis_ntee_code_source_column",
            "analysis_ntee_code_fallback_enriched",
            "analysis_ntee_code_fallback_enriched_source_family",
            "analysis_ntee_code_fallback_enriched_source_variant",
            "analysis_ntee_code_fallback_enriched_source_column",
            "analysis_subsection_code",
            "analysis_subsection_code_source_family",
            "analysis_subsection_code_source_variant",
            "analysis_subsection_code_source_column",
            "analysis_subsection_code_fallback_enriched",
            "analysis_subsection_code_fallback_enriched_source_family",
            "analysis_subsection_code_fallback_enriched_source_variant",
            "analysis_subsection_code_fallback_enriched_source_column",
            "analysis_missing_classification_flag",
            "analysis_missing_classification_flag_source_column",
            "analysis_calculated_ntee_broad_code",
            "analysis_calculated_ntee_broad_code_source_column",
            "analysis_is_hospital",
            "analysis_is_hospital_source_column",
            "analysis_is_university",
            "analysis_is_university_source_column",
            "analysis_is_political_org",
            "analysis_is_political_org_source_column",
            "analysis_imputed_is_hospital",
            "analysis_imputed_is_hospital_source_column",
            "analysis_imputed_is_university",
            "analysis_imputed_is_university_source_column",
            "analysis_imputed_is_political_org",
            "analysis_imputed_is_political_org_source_column",
        ]
    ].copy()

    analysis_df.sort_values(["tax_year", "region", "county_fips", "ein"], inplace=True, kind="mergesort")
    geography_metrics_df = _build_geography_metrics_output(analysis_df)
    field_metrics_df = _build_field_metrics_output(analysis_df)
    coverage_df = _write_variable_coverage_csv(analysis_df, coverage_path)
    _write_variable_mapping_md(mapping_path)
    _write_data_processing_doc(processing_doc_path)

    analysis_df.to_parquet(analysis_variables_path, index=False)
    geography_metrics_df.to_parquet(geography_metrics_path, index=False)
    field_metrics_df.to_parquet(field_metrics_path, index=False)
    print(f"[analysis] Wrote analysis parquet: {analysis_variables_path}", flush=True)
    print(f"[analysis] Wrote geography metrics parquet: {geography_metrics_path}", flush=True)
    print(f"[analysis] Wrote field metrics parquet: {field_metrics_path}", flush=True)

    return {
        "analysis_row_count": int(len(analysis_df)),
        "geography_metric_row_count": int(len(geography_metrics_df)),
        "field_metric_row_count": int(len(field_metrics_df)),
        "coverage_row_count": int(len(coverage_df)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build IRS EO BMF analysis outputs.")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Documented target S3 bucket")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Documented target S3 region")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local IRS EO BMF metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local IRS EO BMF staging directory")
    parser.add_argument("--bmf-staging-dir", type=Path, default=DEFAULT_BMF_STAGING_DIR, help="Local NCCS BMF staging directory used for classification fallback")
    parser.add_argument("--analysis-prefix", default=ANALYSIS_PREFIX, help="Documented S3 analysis prefix")
    parser.add_argument("--analysis-documentation-prefix", default=ANALYSIS_DOCUMENTATION_PREFIX, help="Documented S3 analysis documentation prefix")
    parser.add_argument("--analysis-variable-mapping-prefix", default=ANALYSIS_VARIABLE_MAPPING_PREFIX, help="Documented S3 analysis variable mapping prefix")
    parser.add_argument("--analysis-coverage-prefix", default=ANALYSIS_COVERAGE_PREFIX, help="Documented S3 analysis coverage prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 07 - EXTRACT IRS EO BMF ANALYSIS VARIABLES LOCAL")
    load_env_from_secrets()
    print(f"[config] Target bucket: {args.bucket}", flush=True)
    print(f"[config] Analysis prefix: {args.analysis_prefix}", flush=True)
    summary = build_analysis_outputs(
        metadata_dir=args.metadata_dir,
        staging_dir=args.staging_dir,
        bmf_staging_dir=args.bmf_staging_dir,
    )
    print(f"[analysis] Summary: {summary}", flush=True)
    print_elapsed(start, "Step 07")


if __name__ == "__main__":
    main()
