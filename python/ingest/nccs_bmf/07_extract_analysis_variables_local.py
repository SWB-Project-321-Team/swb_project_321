"""
Step 07: Build NCCS BMF analysis outputs for the 2022-2024 analysis window.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from common import (
    ANALYSIS_META_PREFIX,
    ANALYSIS_PREFIX,
    ANALYSIS_TAX_YEAR_MAX,
    ANALYSIS_TAX_YEAR_MIN,
    DEFAULT_S3_BUCKET,
    DEFAULT_S3_REGION,
    META_DIR,
    STAGING_DIR,
    analysis_data_processing_doc_path,
    analysis_field_metrics_output_path,
    analysis_geography_metrics_output_path,
    analysis_variable_coverage_path,
    analysis_variable_mapping_path,
    analysis_variables_output_path,
    banner,
    exact_year_lookup_output_path,
    filtered_output_path,
    load_env_from_secrets,
    print_elapsed,
    resolve_release_and_write_metadata,
    selected_assets,
)
from ingest._shared.analysis_support import (
    DEFAULT_BMF_STAGING_DIR,
    DEFAULT_IRS_BMF_RAW_DIR,
    apply_classification_fallbacks,
    load_bmf_classification_lookup,
    load_irs_bmf_classification_lookup,
    apply_imputed_flag_family,
    build_ntee_proxy_flag,
    build_political_proxy_flag,
    numeric_from_text,
    normalize_ein,
    series_has_value,
)


UNAVAILABLE_VARIABLES = [
    {"canonical_variable": "analysis_program_service_revenue_amount", "variable_role": "unavailable", "draft_variable": "Program service revenue", "notes": "BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_calculated_total_contributions_amount", "variable_role": "unavailable", "draft_variable": "Total contributions", "notes": "BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_other_contributions_amount", "variable_role": "unavailable", "draft_variable": "Other contributions", "notes": "BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_calculated_grants_total_amount", "variable_role": "unavailable", "draft_variable": "Grants (total amount)", "notes": "BMF does not carry this 990-style revenue-source field."},
    {"canonical_variable": "analysis_total_expense_amount", "variable_role": "unavailable", "draft_variable": "Total expense", "notes": "BMF does not carry this expense field."},
    {"canonical_variable": "analysis_net_asset_amount", "variable_role": "unavailable", "draft_variable": "Net asset", "notes": "BMF does not provide a direct net-asset field in the current pipeline."},
    {"canonical_variable": "analysis_calculated_surplus_amount", "variable_role": "unavailable", "draft_variable": "Surplus", "notes": "BMF does not carry a direct revenue-minus-expense surplus field in the current pipeline."},
    {"canonical_variable": "analysis_calculated_net_margin_ratio", "variable_role": "unavailable", "draft_variable": "Net margin", "notes": "BMF cannot calculate net margin without total expenses."},
    {"canonical_variable": "analysis_calculated_months_of_reserves", "variable_role": "unavailable", "draft_variable": "Months of reserves", "notes": "BMF cannot calculate months of reserves without total expenses and a direct net-asset field."},
]

AVAILABLE_VARIABLE_METADATA = [
    {"canonical_variable": "analysis_total_revenue_amount", "variable_role": "direct", "draft_variable": "Total revenue", "source_rule": "Year-aware direct BMF revenue field", "provenance_column": "analysis_total_revenue_amount_source_column", "notes": "Regionwide total-revenue metric."},
    {"canonical_variable": "analysis_total_assets_amount", "variable_role": "direct", "draft_variable": "Total assets", "source_rule": "Year-aware direct BMF asset field", "provenance_column": "analysis_total_assets_amount_source_column", "notes": "Regionwide total-assets metric."},
    {"canonical_variable": "analysis_total_income_amount", "variable_role": "direct", "draft_variable": "Total income", "source_rule": "Year-aware direct BMF income field", "provenance_column": "analysis_total_income_amount_source_column", "notes": "Performance support metric for the draft's time-trend notes."},
    {"canonical_variable": "analysis_ntee_code", "variable_role": "enriched", "draft_variable": "NTEE filed classification code", "source_rule": "Exact-year BMF lookup joined on EIN + tax_year, then nearest-year BMF, then IRS EO BMF EIN fallback", "provenance_column": "analysis_ntee_code_source_column", "notes": "Analysis-ready classification support field."},
    {"canonical_variable": "analysis_subsection_code", "variable_role": "enriched", "draft_variable": "Subsection code", "source_rule": "Exact-year BMF lookup joined on EIN + tax_year, then nearest-year BMF, then IRS EO BMF EIN fallback", "provenance_column": "analysis_subsection_code_source_column", "notes": "Political-organization support field."},
    {"canonical_variable": "analysis_calculated_ntee_broad_code", "variable_role": "calculated", "draft_variable": "Broad NTEE field classification code", "source_rule": "First letter of analysis_ntee_code", "provenance_column": "analysis_calculated_ntee_broad_code_source_column", "notes": "Broad field representation metric."},
    {"canonical_variable": "analysis_is_hospital", "variable_role": "proxy", "draft_variable": "Hospital flag", "source_rule": "NTEE proxy from analysis_ntee_code", "provenance_column": "analysis_is_hospital_source_column", "notes": "Source-backed hospital proxy."},
    {"canonical_variable": "analysis_is_university", "variable_role": "proxy", "draft_variable": "University flag", "source_rule": "NTEE proxy from analysis_ntee_code", "provenance_column": "analysis_is_university_source_column", "notes": "Source-backed university proxy."},
    {"canonical_variable": "analysis_is_political_org", "variable_role": "proxy", "draft_variable": "Political organization flag", "source_rule": "Subsection proxy from analysis_subsection_code", "provenance_column": "analysis_is_political_org_source_column", "notes": "Source-backed political proxy."},
    {"canonical_variable": "analysis_imputed_is_hospital", "variable_role": "imputed", "draft_variable": "Hospital flag", "source_rule": "analysis_is_hospital, then conservative name fallback, then default false", "provenance_column": "analysis_imputed_is_hospital_source_column", "notes": "Complete hospital flag for analysis exclusions."},
    {"canonical_variable": "analysis_imputed_is_university", "variable_role": "imputed", "draft_variable": "University flag", "source_rule": "analysis_is_university, then conservative name fallback, then default false", "provenance_column": "analysis_imputed_is_university_source_column", "notes": "Complete university flag for analysis exclusions."},
    {"canonical_variable": "analysis_imputed_is_political_org", "variable_role": "imputed", "draft_variable": "Political organization flag", "source_rule": "analysis_is_political_org, then conservative name fallback, then default false", "provenance_column": "analysis_imputed_is_political_org_source_column", "notes": "Complete political-organization flag for analysis exclusions."},
]


def _ensure_parent_dirs(*paths: Path) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path.parent}", flush=True)


def _year_schema(frame: pd.DataFrame, snapshot_year: int) -> dict[str, str]:
    """Return the year-aware raw BMF column mapping for direct amount extraction."""
    if snapshot_year == 2022:
        return {
            "name_col": "NAME",
            "state_col": "STATE",
            "zip_col": "ZIP5",
            "revenue_col": "CTOTREV",
            "assets_col": "ASSETS",
            "income_col": "INCOME",
        }
    return {
        "name_col": "NAME",
        "state_col": "STATE",
        "zip_col": "ZIP",
        "revenue_col": "REVENUE_AMT",
        "assets_col": "ASSET_AMT",
        "income_col": "INCOME_AMT",
    }


def _build_broad_ntee(ntee_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    broad = ntee_series.astype("string").fillna("").str.upper().str.slice(0, 1)
    broad = broad.mask(broad.eq(""), pd.NA)
    provenance = pd.Series(pd.NA, index=ntee_series.index, dtype="string")
    provenance.loc[broad.notna()] = "analysis_ntee_code"
    return broad, provenance


def _dedupe_bmf_analysis_rows(source_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """
    Deduplicate rare duplicate BMF EIN-year rows for analysis-only use.

    The filtered yearly benchmark artifacts remain unchanged. This analysis-only
    dedupe rule avoids double counting in region totals by preferring the row
    with the highest populated revenue, then highest assets, then highest
    income, then stable source order.
    """
    duplicate_group_count = int((source_df.groupby(["ein", "tax_year"]).size() > 1).sum())
    if duplicate_group_count == 0:
        return source_df.copy(), 0
    sort_df = source_df.copy()
    sort_df["__revenue_sort"] = sort_df["analysis_total_revenue_amount"].fillna(float("-inf"))
    sort_df["__assets_sort"] = sort_df["analysis_total_assets_amount"].fillna(float("-inf"))
    sort_df["__income_sort"] = sort_df["analysis_total_income_amount"].fillna(float("-inf"))
    sort_df["__source_order"] = range(len(sort_df))
    sort_df = sort_df.sort_values(
        by=["ein", "tax_year", "__revenue_sort", "__assets_sort", "__income_sort", "__source_order"],
        ascending=[True, True, False, False, False, True],
        kind="mergesort",
    )
    deduped = sort_df.drop_duplicates(subset=["ein", "tax_year"], keep="first").copy()
    deduped.drop(columns=["__revenue_sort", "__assets_sort", "__income_sort", "__source_order"], inplace=True)
    return deduped.reset_index(drop=True), duplicate_group_count


def _build_geography_metrics_output(analysis_df: pd.DataFrame) -> pd.DataFrame:
    """Build county- and region-level BMF geography metrics."""
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
    for exclusion_variant, scope_mask in tqdm(scope_specs, desc="bmf geography scopes", unit="scope"):
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
                rows.append(
                    {
                        "geography_level": geography_level,
                        "region": region,
                        "county_fips": county_fips,
                        "tax_year": tax_year,
                        "analysis_exclusion_variant": exclusion_variant,
                        "nonprofit_count": nonprofit_count,
                        "analysis_total_nonprofit_count": nonprofit_count,
                        "unique_nonprofit_ein_count": int(group_df["ein"].dropna().astype("string").nunique()),
                        "analysis_total_revenue_amount_sum": revenue_sum if pd.notna(revenue_sum) else pd.NA,
                        "analysis_total_assets_amount_sum": assets_sum if pd.notna(assets_sum) else pd.NA,
                        "analysis_total_income_amount_sum": income_sum if pd.notna(income_sum) else pd.NA,
                        "analysis_calculated_normalized_total_revenue_per_nonprofit": (float(revenue_sum) / nonprofit_count) if nonprofit_count and pd.notna(revenue_sum) else pd.NA,
                        "analysis_calculated_normalized_total_assets_per_nonprofit": (float(assets_sum) / nonprofit_count) if nonprofit_count and pd.notna(assets_sum) else pd.NA,
                        "analysis_calculated_normalized_total_revenue_per_unique_nonprofit": (float(revenue_sum) / int(group_df["ein"].dropna().astype("string").nunique())) if int(group_df["ein"].dropna().astype("string").nunique()) and pd.notna(revenue_sum) else pd.NA,
                        "analysis_calculated_normalized_total_assets_per_unique_nonprofit": (float(assets_sum) / int(group_df["ein"].dropna().astype("string").nunique())) if int(group_df["ein"].dropna().astype("string").nunique()) and pd.notna(assets_sum) else pd.NA,
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
    """Build region-year-broad-NTEE metrics for the representation analyses."""
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
    for exclusion_variant, scope_mask in tqdm(scope_specs, desc="bmf field scopes", unit="scope"):
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


def _write_mapping_markdown(mapping_path: Path) -> None:
    lines = [
        "# NCCS BMF Analysis Variable Mapping",
        "",
        "- Source family: `nccs_bmf`",
        f"- Analysis output: `{analysis_variables_output_path()}`",
        f"- Geography metrics output: `{analysis_geography_metrics_output_path()}`",
        f"- Field metrics output: `{analysis_field_metrics_output_path()}`",
        f"- Coverage report: `{analysis_variable_coverage_path()}`",
        "- Scope: `2022-2024` only",
        "",
        "## Extracted Variables",
        "",
        "|canonical_variable|variable_role|draft_variable|source_rule|provenance_column|notes|",
        "|---|---|---|---|---|---|",
        "",
        "## Unavailable Variables",
        "",
        "|canonical_variable|availability_status|draft_variable|notes|",
        "|---|---|---|---|",
    ]
    for row in AVAILABLE_VARIABLE_METADATA:
        lines.append(f"|{row['canonical_variable']}|{row['variable_role']}|{row['draft_variable']}|{row['source_rule']}|{row['provenance_column']}|{row['notes']}|")
    for row in UNAVAILABLE_VARIABLES:
        lines.append(f"|{row['canonical_variable']}|unavailable|{row['draft_variable']}|{row['notes']}|")
    lines.extend(["", "## Draft Alignment Appendix", "", "|draft_variable|source_specific_output|status|rule_or_reason|", "|---|---|---|---|"])
    for row in AVAILABLE_VARIABLE_METADATA:
        lines.append(f"|{row['draft_variable']}|{row['canonical_variable']}|{row['variable_role']}|{row['source_rule']}|")
    for row in UNAVAILABLE_VARIABLES:
        lines.append(f"|{row['draft_variable']}|{row['canonical_variable']}|unavailable|{row['notes']}|")
    mapping_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_data_processing_doc(doc_path: Path) -> None:
    raw_dir = META_DIR.parent / "raw"
    doc = f"""# NCCS BMF Pipeline

## Status

This document is the authoritative technical reference for the current NCCS BMF pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_bmf/`, including the current analysis window of `2022-2024`, the exact-year lookup artifacts written in step 05, the row-level BMF analysis build in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the NCCS BMF data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how yearly raw BMF files become benchmark-filtered annual parquets
- how the final BMF analysis dataset is built from those annual outputs
- what is direct, calculated, enriched, imputed, or unavailable in the final analysis-ready package

## Pipeline At A Glance

The NCCS BMF pipeline is the repo's primary registry and classification layer for nonprofit counts, geography, subsection logic, and NTEE-based field composition analysis.

At a high level, the pipeline has these phases:

1. Discover which NCCS BMF yearly source files should be used.
2. Download the yearly raw BMF files and write selection metadata.
3. Upload the raw yearly files and release metadata to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw yearly inputs.
5. Filter each yearly raw file to benchmark geography and write paired exact-year lookup overlays.
6. Upload the filtered yearly outputs and exact-year lookup overlays to Silver S3.
7. Build the final BMF analysis-ready rowset, geography metrics, field metrics, coverage CSV, mapping doc, and this processing doc.
8. Upload the final BMF analysis artifacts and metadata to the dedicated analysis S3 prefix.

The main artifact classes are:

- Raw data: yearly NCCS BMF source files exactly as downloaded.
- Metadata/manifests: release-selection metadata, HTML snapshots, download manifests, and verification reports.
- Silver filtered artifacts: benchmark-filtered yearly BMF parquets plus yearly exact-year lookup parquets.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, field metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows the `CODING_RULES.md` filtered-only combine contract:

- downstream combine stages operate only on benchmark-filtered yearly BMF artifacts
- the annual benchmark filter happens upstream in step 05
- the analysis step does not reopen the broad raw yearly files

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_bmf_release.py` | Discover the yearly BMF assets to use for the run | Remote dataset pages and probes | `latest_release.json`, HTML snapshots | HTTP |
| 02 | `02_download_bmf_release.py` | Download the selected yearly BMF raw files | Selected yearly asset set | Raw annual BMF files | HTTP |
| 03 | `03_upload_bmf_release_to_s3.py` | Upload raw BMF files and metadata to Bronze | Local raw files and metadata | Bronze raw BMF objects | S3 |
| 04 | `04_verify_bmf_source_local_s3.py` | Verify raw bytes across source, local disk, and Bronze | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_filter_bmf_to_benchmark_local.py` | Filter each year to benchmark geography and write exact-year lookup overlays | Raw annual BMF files plus geography crosswalks | Filtered annual BMF parquets and exact-year lookup parquets | local files |
| 06 | `06_upload_filtered_bmf_to_s3.py` | Upload filtered annual outputs and exact-year lookups to Silver | Filtered yearly parquets and metadata | Silver filtered BMF objects | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build row-level analysis variables, geography metrics, field metrics, coverage CSV, mapping doc, and this processing doc | Filtered annual BMF parquets and exact-year lookup parquets | Official BMF analysis outputs | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload official analysis outputs and verify local-vs-S3 bytes | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

### Step 01: release discovery

The BMF pipeline begins by identifying which yearly source files represent the current benchmark-analysis window.

The discovery step:

- probes the public NCCS BMF release pages
- captures HTML snapshots for reproducibility
- writes a `latest_release.json` file describing the selected yearly assets

This selection metadata is the source of truth for which annual files later steps should download and process.

### Step 02: raw yearly download

The download step materializes one raw annual BMF file for each selected analysis year.

It preserves:

- the source URL
- the selected filename
- the year represented by that file
- source byte metadata when available

The raw files are intentionally kept unchanged so the filter step can always trace back to the original annual inputs.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw annual files and release metadata to Bronze and then verifies source/local/S3 byte parity.

This means the BMF pipeline keeps:

- local raw files
- Bronze raw copies
- a verification CSV documenting whether the local and S3 byte counts match the original source

## Where Data Is Stored

### Local directories

- Local raw BMF directory: `{raw_dir}`
- Local metadata directory: `{META_DIR}`
- Local staging directory: `{STAGING_DIR}`

### Filtered and analysis artifacts

- Row-level analysis parquet: `{analysis_variables_output_path()}`
- Geography metrics parquet: `{analysis_geography_metrics_output_path()}`
- NTEE field metrics parquet: `{analysis_field_metrics_output_path()}`
- Coverage CSV: `{analysis_variable_coverage_path()}`
- Mapping doc: `{analysis_variable_mapping_path()}`
- Data-processing doc: `{analysis_data_processing_doc_path()}`

### S3 layout

- Silver filtered BMF prefix: `silver/nccs_bmf/`
- Silver filtered metadata prefix: `silver/nccs_bmf/metadata/`
- Official S3 analysis prefix: `silver/nccs_bmf/analysis/`
- Official S3 analysis metadata prefix: `silver/nccs_bmf/analysis/metadata/`

## Step-By-Step Transformation

### Step 05: yearly benchmark filtering and exact-year lookup creation

Step 05 is the key upstream curation step.

For each selected BMF year, it:

1. loads the raw annual BMF file
2. filters rows to the benchmark counties and regions of interest
3. writes one benchmark-filtered yearly parquet
4. writes one exact-year lookup parquet used later for NTEE/subsection enrichment

This is a deliberate design choice. The final analysis step does not enrich classification directly from the broad raw files. Instead, the upstream yearly lookup overlays become the stable exact-year classification inputs consumed downstream.

### Step 06: Silver upload of yearly artifacts

The pipeline then uploads:

- the benchmark-filtered yearly parquets
- the exact-year lookup parquets
- the filtered-artifact metadata

This means the annual benchmark outputs are official curated Silver artifacts in their own right, not just temporary intermediates.

### Step 07: analysis-year union

The analysis step restricts scope to `2022-2024` and loads each benchmark-filtered annual parquet in that range.

It also loads the exact-year lookup parquet for the same year. This preserves a clean separation between:

- direct annual BMF values such as revenue, assets, and income
- exact-year classification overlays
- later fallback enrichment when exact-year classification is blank

### Row-level identity and direct financial extraction

The row-level analysis dataset keeps:

- `ein`
- `tax_year`
- `org_name`
- `state`
- `zip5`
- `county_fips`
- `region`

Direct financial fields are extracted from the year-aware raw BMF columns:

- `analysis_total_revenue_amount`
- `analysis_total_assets_amount`
- `analysis_total_income_amount`

The pipeline also preserves `*_source_column` provenance so the final analysis file records whether a value came from the 2022 legacy amount columns or the 2023+ amount columns.

### Analysis-only duplicate resolution

Rare duplicate `EIN + tax_year` groups are resolved only inside the final analysis rowset.

This is intentionally not an upstream rewrite of the yearly benchmark parquets. The annual benchmark artifacts remain source-faithful filtered outputs. The analysis layer resolves duplicates only so region/year totals do not double count the same organization-year.

The ranking rule prefers:

1. highest nonblank revenue
2. highest nonblank assets
3. highest nonblank income
4. stable source order

### Classification enrichment and fallback order

`analysis_ntee_code` and `analysis_subsection_code` start from the exact-year lookup overlay joined by `ein + tax_year`.

When the exact-year lookup is blank, the final analysis-ready BMF file uses this fallback order:

1. exact-year BMF
2. nearest-year BMF
3. IRS EO BMF fallback

All fills preserve:

- source family
- source variant
- source column

`analysis_calculated_ntee_broad_code` is then derived as the first letter of the final resolved NTEE code.

### Source-backed and imputed exclusion flags

The source-backed flag family is:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

These are derived from the resolved classification fields:

- hospital and university from NTEE
- political organization from subsection

The fully populated analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed columns prefer the source-backed value first, then conservative name-based fallback, then default `False`.

### Geography metrics

The geography metrics parquet aggregates the analysis rowset by:

- geography level
- region
- optional county FIPS
- tax year
- exclusion variant

Metrics include:

- organization counts
- unique EIN counts
- revenue, assets, and income sums
- normalized revenue and assets per nonprofit
- counts of imputed hospital, university, and political-organization flags

### Field metrics

The field metrics parquet aggregates the same rowset by:

- region
- tax year
- exclusion variant
- `analysis_calculated_ntee_broad_code`

Each row reports:

- organization count
- revenue sum
- asset sum
- share of region nonprofit count
- share of region revenue
- share of region assets

This is the table intended to support field-composition and sector-mix analysis from the draft.

### Coverage, mapping, and upload verification

The coverage CSV reports populated counts and fill rates for the available analysis variables by year and overall.

The mapping Markdown documents which variables are:

- direct
- calculated
- enriched
- proxy
- imputed
- unavailable

The upload step performs explicit local-vs-S3 byte checks for every analysis artifact and raises if any uploaded object size does not match the local file.

## Final Analysis Outputs

The official BMF analysis package consists of:

- one row-level analysis parquet
- one geography metrics parquet
- one field metrics parquet
- one coverage CSV
- one variable mapping Markdown
- this data-processing doc

The row-level analysis dataset is the canonical BMF analysis-ready file for `2022-2024`.

## Testing And Verification

- Unit tests live in `tests/nccs_bmf/test_nccs_bmf_common.py`.
- The analysis tests cover year restriction, direct financial extraction, classification fallback behavior, duplicate handling, geography metrics, and field metrics.
- Operational verification includes raw source/local/S3 byte checks, filtered-output uploads, and analysis upload byte checks.

## Current Caveats

- BMF is a registry and classification source, not a full 990 financial-detail source.
- `analysis_total_expense_amount`, `analysis_net_asset_amount`, `analysis_calculated_surplus_amount`, `analysis_calculated_net_margin_ratio`, and `analysis_calculated_months_of_reserves` remain unavailable because the current BMF pipeline does not carry the needed expense and net-asset concepts.
- Revenue-source components such as program service revenue, contributions, and grants remain unavailable rather than being weakly backfilled from another pipeline.
- The analysis-only dedupe happens only in the final rowset; the upstream yearly benchmark artifacts remain unchanged.

## Draft Alignment Appendix

| draft_variable | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Year-aware direct BMF revenue field |
| Total assets | analysis_total_assets_amount | direct | Year-aware direct BMF asset field |
| Total income | analysis_total_income_amount | direct | Year-aware direct BMF income field |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year BMF lookup joined on EIN + tax_year, then nearest-year BMF, then IRS EO BMF EIN fallback |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of final resolved NTEE code |
| Hospital flag | analysis_is_hospital | proxy | Resolved from final NTEE code |
| University flag | analysis_is_university | proxy | Resolved from final NTEE code |
| Political organization flag | analysis_is_political_org | proxy | Resolved from final subsection code |
| Total expense | analysis_total_expense_amount | unavailable | BMF does not carry this expense field |
| Net asset | analysis_net_asset_amount | unavailable | BMF does not provide a direct net-asset field in the current pipeline |
| Program service revenue | analysis_program_service_revenue_amount | unavailable | BMF does not carry this 990-style revenue-source field |
| Total contributions | analysis_calculated_total_contributions_amount | unavailable | BMF does not carry this 990-style revenue-source field |
| Grants (total amount) | analysis_calculated_grants_total_amount | unavailable | BMF does not carry this 990-style revenue-source field |
"""
    doc_path.write_text(doc.strip() + "\n", encoding="utf-8")


def build_analysis_outputs(
    *,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
    bmf_staging_dir: Path = DEFAULT_BMF_STAGING_DIR,
    irs_bmf_raw_dir: Path = DEFAULT_IRS_BMF_RAW_DIR,
) -> dict[str, Any]:
    analysis_variables_path = analysis_variables_output_path(staging_dir)
    geography_metrics_path = analysis_geography_metrics_output_path(staging_dir)
    field_metrics_path = analysis_field_metrics_output_path(staging_dir)
    coverage_path = analysis_variable_coverage_path(metadata_dir)
    mapping_path = analysis_variable_mapping_path()
    processing_doc_path = analysis_data_processing_doc_path()
    _ensure_parent_dirs(analysis_variables_path, geography_metrics_path, field_metrics_path, coverage_path, mapping_path, processing_doc_path)

    release = resolve_release_and_write_metadata(metadata_dir, start_year=ANALYSIS_TAX_YEAR_MIN)
    years = [
        int(asset["snapshot_year"])
        for asset in selected_assets(release)
        if ANALYSIS_TAX_YEAR_MIN <= int(asset["snapshot_year"]) <= ANALYSIS_TAX_YEAR_MAX
    ]
    if not years:
        raise RuntimeError("No BMF years were selected for the 2022-2024 analysis scope.")

    source_frames: list[pd.DataFrame] = []
    lookup_frames: list[pd.DataFrame] = []
    for snapshot_year in tqdm(sorted(years), desc="load BMF benchmark years", unit="year"):
        filtered_path = filtered_output_path(staging_dir, snapshot_year)
        lookup_path = exact_year_lookup_output_path(staging_dir, snapshot_year)
        if not filtered_path.exists():
            raise FileNotFoundError(f"Missing filtered BMF output: {filtered_path}")
        if not lookup_path.exists():
            raise FileNotFoundError(f"Missing exact-year BMF lookup: {lookup_path}")
        year_df = pd.read_parquet(filtered_path)
        schema = _year_schema(year_df, snapshot_year)
        year_df["ein"] = year_df["EIN"].astype("string").map(normalize_ein)
        year_df["tax_year"] = pd.Series(str(snapshot_year), index=year_df.index, dtype="string")
        year_df["org_name"] = year_df[schema["name_col"]].astype("string")
        year_df["state"] = year_df[schema["state_col"]].astype("string")
        year_df["zip5"] = year_df[schema["zip_col"]].astype("string").str.slice(0, 5)
        year_df["county_fips"] = year_df["county_fips"].astype("string")
        year_df["region"] = year_df["region"].astype("string")
        year_df["analysis_total_revenue_amount"] = numeric_from_text(year_df[schema["revenue_col"]])
        year_df["analysis_total_assets_amount"] = numeric_from_text(year_df[schema["assets_col"]])
        year_df["analysis_total_income_amount"] = numeric_from_text(year_df[schema["income_col"]])
        year_df["analysis_total_revenue_amount_source_column"] = pd.Series(schema["revenue_col"], index=year_df.index, dtype="string")
        year_df.loc[year_df["analysis_total_revenue_amount"].isna(), "analysis_total_revenue_amount_source_column"] = pd.NA
        year_df["analysis_total_assets_amount_source_column"] = pd.Series(schema["assets_col"], index=year_df.index, dtype="string")
        year_df.loc[year_df["analysis_total_assets_amount"].isna(), "analysis_total_assets_amount_source_column"] = pd.NA
        year_df["analysis_total_income_amount_source_column"] = pd.Series(schema["income_col"], index=year_df.index, dtype="string")
        year_df.loc[year_df["analysis_total_income_amount"].isna(), "analysis_total_income_amount_source_column"] = pd.NA
        source_frames.append(
            year_df[
                [
                    "ein",
                    "tax_year",
                    "org_name",
                    "state",
                    "zip5",
                    "county_fips",
                    "region",
                    "analysis_total_revenue_amount",
                    "analysis_total_assets_amount",
                    "analysis_total_income_amount",
                    "analysis_total_revenue_amount_source_column",
                    "analysis_total_assets_amount_source_column",
                    "analysis_total_income_amount_source_column",
                ]
            ]
        )
        lookup_df = pd.read_parquet(
            lookup_path,
            columns=[
                "harm_ein",
                "harm_tax_year",
                "harm_ntee_code",
                "harm_ntee_code__source_family",
                "harm_ntee_code__source_variant",
                "harm_ntee_code__source_column",
                "harm_subsection_code",
                "harm_subsection_code__source_family",
                "harm_subsection_code__source_variant",
                "harm_subsection_code__source_column",
            ],
        ).rename(
            columns={
                "harm_ein": "ein",
                "harm_tax_year": "tax_year",
                "harm_ntee_code": "analysis_ntee_code",
                "harm_ntee_code__source_family": "analysis_ntee_code_source_family",
                "harm_ntee_code__source_variant": "analysis_ntee_code_source_variant",
                "harm_ntee_code__source_column": "analysis_ntee_code_source_column",
                "harm_subsection_code": "analysis_subsection_code",
                "harm_subsection_code__source_family": "analysis_subsection_code_source_family",
                "harm_subsection_code__source_variant": "analysis_subsection_code_source_variant",
                "harm_subsection_code__source_column": "analysis_subsection_code_source_column",
            }
        )
        lookup_df["ein"] = lookup_df["ein"].astype("string").map(normalize_ein)
        lookup_df["tax_year"] = lookup_df["tax_year"].astype("string")
        lookup_frames.append(lookup_df)

    source_df = pd.concat(source_frames, ignore_index=True)
    lookup_df = pd.concat(lookup_frames, ignore_index=True).drop_duplicates(subset=["ein", "tax_year"], keep="first")
    print(f"[analysis] Combined BMF source rows before analysis dedupe: {len(source_df):,}", flush=True)
    source_df, duplicate_group_count = _dedupe_bmf_analysis_rows(source_df)
    print(f"[analysis] Combined BMF source rows after analysis dedupe: {len(source_df):,}", flush=True)
    print(f"[analysis] Duplicate EIN-year groups resolved: {duplicate_group_count:,}", flush=True)

    analysis_df = source_df.merge(lookup_df, on=["ein", "tax_year"], how="left", validate="one_to_one")
    bmf_lookup_df = load_bmf_classification_lookup(
        sorted(analysis_df["tax_year"].dropna().astype("string").unique().tolist()),
        bmf_staging_dir=bmf_staging_dir,
    )
    irs_lookup_df = load_irs_bmf_classification_lookup(irs_bmf_raw_dir=irs_bmf_raw_dir)
    classification_summary = apply_classification_fallbacks(
        analysis_df,
        analysis_df.copy(),
        state_column="state",
        bmf_lookup_df=bmf_lookup_df,
        irs_lookup_df=irs_lookup_df,
    )
    print(f"[analysis] Classification fallback summary: {classification_summary}", flush=True)
    analysis_df["analysis_calculated_ntee_broad_code"], analysis_df["analysis_calculated_ntee_broad_code_source_column"] = _build_broad_ntee(analysis_df["analysis_ntee_code"])
    analysis_df["analysis_is_hospital"], analysis_df["analysis_is_hospital_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("E20", "E21", "E22", "E24"))
    analysis_df["analysis_is_university"], analysis_df["analysis_is_university_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("B40", "B41", "B42", "B43", "B50"))
    analysis_df["analysis_is_political_org"], analysis_df["analysis_is_political_org_source_column"] = build_political_proxy_flag(analysis_df["analysis_subsection_code"])
    apply_imputed_flag_family(analysis_df, name_column="org_name")

    geography_metrics_df = _build_geography_metrics_output(analysis_df)
    field_metrics_df = _build_field_metrics_output(analysis_df)
    analysis_df.sort_values(["tax_year", "region", "county_fips", "ein"], inplace=True, kind="mergesort")
    geography_metrics_df.sort_values(["geography_level", "region", "county_fips", "tax_year", "analysis_exclusion_variant"], inplace=True, kind="mergesort")
    field_metrics_df.sort_values(["region", "tax_year", "analysis_exclusion_variant", "analysis_calculated_ntee_broad_code"], inplace=True, kind="mergesort")
    analysis_df.to_parquet(analysis_variables_path, index=False)
    geography_metrics_df.to_parquet(geography_metrics_path, index=False)
    field_metrics_df.to_parquet(field_metrics_path, index=False)

    coverage_rows: list[dict[str, Any]] = []
    final_variables = [
        "analysis_total_revenue_amount",
        "analysis_total_assets_amount",
        "analysis_total_income_amount",
        "analysis_ntee_code",
        "analysis_calculated_ntee_broad_code",
        "analysis_subsection_code",
        "analysis_is_hospital",
        "analysis_is_university",
        "analysis_is_political_org",
        "analysis_imputed_is_hospital",
        "analysis_imputed_is_university",
        "analysis_imputed_is_political_org",
    ]
    for variable_name in tqdm(final_variables, desc="bmf coverage variables", unit="variable"):
        metadata_row = next(row for row in AVAILABLE_VARIABLE_METADATA if row["canonical_variable"] == variable_name)
        for tax_year in ["all"] + sorted(analysis_df["tax_year"].dropna().astype("string").unique().tolist(), key=int):
            group_df = analysis_df if tax_year == "all" else analysis_df.loc[analysis_df["tax_year"].astype("string").eq(str(tax_year))]
            row_count = int(len(group_df))
            populated_count = int(series_has_value(group_df[variable_name]).sum()) if row_count else 0
            coverage_rows.append(
                {
                    "canonical_variable": variable_name,
                    "availability_status": "available",
                    "variable_role": metadata_row["variable_role"],
                    "draft_variable": metadata_row["draft_variable"],
                    "tax_year": tax_year,
                    "row_count": row_count,
                    "populated_count": populated_count,
                    "fill_rate": (populated_count / row_count) if row_count else pd.NA,
                    "notes": metadata_row["notes"],
                }
            )
    for row in UNAVAILABLE_VARIABLES:
        coverage_rows.append(
            {
                "canonical_variable": row["canonical_variable"],
                "availability_status": "unavailable",
                "variable_role": row["variable_role"],
                "draft_variable": row["draft_variable"],
                "tax_year": "all",
                "row_count": int(len(analysis_df)),
                "populated_count": 0,
                "fill_rate": 0.0,
                "notes": row["notes"],
            }
        )
    pd.DataFrame(coverage_rows).to_csv(coverage_path, index=False)
    _write_mapping_markdown(mapping_path)
    _write_data_processing_doc(processing_doc_path)

    print("[analysis] Counts by tax year:", flush=True)
    for row in analysis_df.groupby("tax_year", dropna=False).size().reset_index(name="row_count").itertuples(index=False):
        print(f"[analysis]   tax_year={row.tax_year} row_count={row.row_count:,}", flush=True)
    return {
        "analysis_row_count": int(len(analysis_df)),
        "duplicate_group_count": duplicate_group_count,
        "geography_metric_row_count": int(len(geography_metrics_df)),
        "field_metric_row_count": int(len(field_metrics_df)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build NCCS BMF analysis outputs for 2022-2024.")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging directory")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Unused here but logged to mirror pipeline conventions")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Unused here but logged to mirror pipeline conventions")
    parser.add_argument("--analysis-prefix", default=ANALYSIS_PREFIX, help="Logged official analysis S3 prefix")
    parser.add_argument("--analysis-meta-prefix", default=ANALYSIS_META_PREFIX, help="Logged official analysis metadata prefix")
    parser.add_argument("--bmf-staging-dir", type=Path, default=DEFAULT_BMF_STAGING_DIR, help="Lookup directory for exact-year and nearest-year BMF classification fallbacks")
    parser.add_argument("--irs-bmf-raw-dir", type=Path, default=DEFAULT_IRS_BMF_RAW_DIR, help="Raw IRS EO BMF directory for final classification fallback")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 07 - BUILD NCCS BMF ANALYSIS OUTPUTS")
    load_env_from_secrets()
    print(f"[analysis] Metadata dir: {args.metadata_dir}", flush=True)
    print(f"[analysis] Staging dir: {args.staging_dir}", flush=True)
    print(f"[analysis] Official analysis prefix: {args.analysis_prefix}", flush=True)
    print(f"[analysis] Official analysis metadata prefix: {args.analysis_meta_prefix}", flush=True)
    summary = build_analysis_outputs(
        metadata_dir=args.metadata_dir,
        staging_dir=args.staging_dir,
        bmf_staging_dir=args.bmf_staging_dir,
        irs_bmf_raw_dir=args.irs_bmf_raw_dir,
    )
    print(f"[analysis] Summary: {summary}", flush=True)
    print_elapsed(start, "Step 07")


if __name__ == "__main__":
    main()
