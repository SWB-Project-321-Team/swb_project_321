"""
Step 08: Build NCCS efile analysis outputs for the 2022-2024 analysis window.
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
    analysis_geography_metrics_output_path,
    analysis_variable_coverage_path,
    analysis_variable_mapping_path,
    analysis_variables_output_path,
    banner,
    grouped_assets_by_year,
    load_env_from_secrets,
    print_elapsed,
    resolve_release_and_write_metadata,
)
from ingest._shared.analysis_support import (
    apply_classification_fallbacks,
    blank_to_na,
    build_name_proxy_flag,
    build_ntee_proxy_flag,
    build_political_proxy_flag,
    build_ratio_metric,
    empty_string_series,
    load_bmf_classification_lookup,
    load_irs_bmf_classification_lookup,
    normalize_bool_text,
    numeric_from_text,
    series_has_value,
)


UNAVAILABLE_VARIABLES = [
    {
        "canonical_variable": "analysis_program_service_revenue_amount",
        "availability_status": "unavailable",
        "variable_role": "unavailable",
        "draft_variable": "Program service revenue",
        "source_rule": "Not present in the current efile benchmark artifact.",
        "notes": "Keep unavailable rather than backfilling from GT.",
    },
    {
        "canonical_variable": "analysis_calculated_total_contributions_amount",
        "availability_status": "unavailable",
        "variable_role": "unavailable",
        "draft_variable": "Total contributions",
        "source_rule": "Contribution-component fields are not present in the current efile benchmark artifact.",
        "notes": "Keep unavailable rather than backfilling from GT.",
    },
    {
        "canonical_variable": "analysis_other_contributions_amount",
        "availability_status": "unavailable",
        "variable_role": "unavailable",
        "draft_variable": "Other contributions",
        "source_rule": "Contribution-component fields are not present in the current efile benchmark artifact.",
        "notes": "Keep unavailable rather than backfilling from GT.",
    },
    {
        "canonical_variable": "analysis_calculated_grants_total_amount",
        "availability_status": "unavailable",
        "variable_role": "unavailable",
        "draft_variable": "Grants (total amount)",
        "source_rule": "Grant-component fields are not present in the current efile benchmark artifact.",
        "notes": "Keep unavailable rather than backfilling from GT.",
    },
]

_REPO_ROOT = Path(__file__).resolve().parents[3]

AVAILABLE_VARIABLE_METADATA = [
    {
        "canonical_variable": "analysis_total_revenue_amount",
        "variable_role": "direct",
        "draft_variable": "Total revenue",
        "source_rule": "Direct from harm_revenue_amount",
        "provenance_column": "analysis_total_revenue_amount_source_column",
        "notes": "Efile total revenue direct field.",
    },
    {
        "canonical_variable": "analysis_total_expense_amount",
        "variable_role": "direct",
        "draft_variable": "Total expense",
        "source_rule": "Direct from harm_expenses_amount",
        "provenance_column": "analysis_total_expense_amount_source_column",
        "notes": "Efile total expense direct field.",
    },
    {
        "canonical_variable": "analysis_total_assets_amount",
        "variable_role": "direct",
        "draft_variable": "Total assets",
        "source_rule": "Direct from harm_assets_amount",
        "provenance_column": "analysis_total_assets_amount_source_column",
        "notes": "Direct asset field used for the draft's total-assets questions.",
    },
    {
        "canonical_variable": "analysis_net_asset_amount",
        "variable_role": "proxy",
        "draft_variable": "Net asset",
        "source_rule": "Asset-based proxy carried from harm_assets_amount",
        "provenance_column": "analysis_net_asset_amount_source_column",
        "notes": "Conservative proxy preserved for draft financial-performance work.",
    },
    {
        "canonical_variable": "analysis_calculated_surplus_amount",
        "variable_role": "direct",
        "draft_variable": "Surplus",
        "source_rule": "Direct from harm_income_amount, carried as the draft surplus metric",
        "provenance_column": "analysis_calculated_surplus_amount_source_column",
        "notes": "Current-year revenue-less-expense amount already produced upstream.",
    },
    {
        "canonical_variable": "analysis_calculated_net_margin_ratio",
        "variable_role": "calculated",
        "draft_variable": "Net margin",
        "source_rule": "analysis_calculated_surplus_amount / positive analysis_total_revenue_amount",
        "provenance_column": "analysis_calculated_net_margin_ratio_source_column",
        "notes": "Null when revenue is missing, zero, or negative.",
    },
    {
        "canonical_variable": "analysis_calculated_months_of_reserves",
        "variable_role": "calculated",
        "draft_variable": "Months of reserves",
        "source_rule": "(analysis_net_asset_amount / positive analysis_total_expense_amount) * 12",
        "provenance_column": "analysis_calculated_months_of_reserves_source_column",
        "notes": "Uses the conservative asset-based net-asset proxy.",
    },
    {
        "canonical_variable": "analysis_ntee_code",
        "variable_role": "enriched",
        "draft_variable": "NTEE filed classification code",
        "source_rule": "NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback",
        "provenance_column": "analysis_ntee_code_source_column",
        "notes": "Classification enrichment only, not a row-admission filter.",
    },
    {
        "canonical_variable": "analysis_subsection_code",
        "variable_role": "enriched",
        "draft_variable": "Subsection code",
        "source_rule": "NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback",
        "provenance_column": "analysis_subsection_code_source_column",
        "notes": "Supports political-organization classification.",
    },
    {
        "canonical_variable": "analysis_calculated_ntee_broad_code",
        "variable_role": "calculated",
        "draft_variable": "Broad NTEE field classification code",
        "source_rule": "First letter of analysis_ntee_code",
        "provenance_column": "analysis_calculated_ntee_broad_code_source_column",
        "notes": "Broad field category used in field-composition analyses.",
    },
    {
        "canonical_variable": "analysis_is_hospital",
        "variable_role": "direct",
        "draft_variable": "Hospital flag",
        "source_rule": "Direct from efile hospital flag",
        "provenance_column": "analysis_is_hospital_source_column",
        "notes": "Canonical efile source-backed hospital flag.",
    },
    {
        "canonical_variable": "analysis_is_university",
        "variable_role": "direct",
        "draft_variable": "University flag",
        "source_rule": "Direct from efile school/university flag",
        "provenance_column": "analysis_is_university_source_column",
        "notes": "Canonical efile source-backed university flag.",
    },
    {
        "canonical_variable": "analysis_is_political_org",
        "variable_role": "proxy",
        "draft_variable": "Political organization flag",
        "source_rule": "Source-backed subsection proxy",
        "provenance_column": "analysis_is_political_org_source_column",
        "notes": "Canonical political-organization proxy for exclusion analyses.",
    },
]


def _ensure_parent_dirs(*paths: Path) -> None:
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        print(f"[paths] Ready: {path.parent}", flush=True)


def _repo_relative_display(path: Path) -> str:
    """Render repo-local paths portably for generated docs."""
    try:
        return path.resolve().relative_to(_REPO_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def _build_direct_numeric(
    source_df: pd.DataFrame,
    source_column: str,
    output_column: str,
) -> tuple[pd.Series, pd.Series]:
    if source_column not in source_df.columns:
        return pd.Series(pd.NA, index=source_df.index, dtype="Float64"), empty_string_series(source_df.index)
    values = numeric_from_text(source_df[source_column])
    provenance = pd.Series(source_column, index=source_df.index, dtype="string")
    provenance = provenance.mask(values.isna(), pd.NA)
    print(f"[analysis] {output_column}: populated={int(values.notna().sum()):,}", flush=True)
    return values, provenance


def _build_broad_ntee(ntee_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    exact_ntee = blank_to_na(ntee_series)
    broad = exact_ntee.astype("string").str.upper().str.slice(0, 1)
    broad = broad.mask(broad.fillna("").eq(""), pd.NA)
    provenance = pd.Series(pd.NA, index=ntee_series.index, dtype="string")
    provenance.loc[broad.notna()] = "analysis_ntee_code"
    return broad, provenance


def _build_imputed_from_direct_and_proxy(
    analysis_df: pd.DataFrame,
    *,
    canonical_column: str,
    proxy_column: str,
    name_column: str,
    high_patterns: tuple[str, ...],
    medium_patterns: tuple[str, ...],
    block_patterns: tuple[str, ...],
    output_column: str,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    canonical = analysis_df[canonical_column].astype("boolean")
    proxy = analysis_df[proxy_column].astype("boolean")
    imputed = canonical.copy()
    source_column = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    source_rule = pd.Series(
        "canonical direct source, then source-backed proxy, then high-confidence name match, then default False",
        index=analysis_df.index,
        dtype="string",
    )

    direct_source_column = analysis_df.get(f"{canonical_column}_source_column", empty_string_series(analysis_df.index))
    proxy_source_column = analysis_df.get(f"{proxy_column}_source_column", empty_string_series(analysis_df.index))
    source_column.loc[canonical.notna()] = direct_source_column.loc[canonical.notna()].astype("string")

    proxy_fill_mask = imputed.isna() & proxy.notna()
    imputed.loc[proxy_fill_mask] = proxy.loc[proxy_fill_mask]
    source_column.loc[proxy_fill_mask] = proxy_source_column.loc[proxy_fill_mask].astype("string")

    name_proxy, name_confidence = build_name_proxy_flag(
        analysis_df[name_column].astype("string"),
        imputed,
        high_patterns,
        medium_patterns,
        block_patterns,
    )
    high_name_mask = imputed.isna() & name_confidence.fillna("").eq("high") & name_proxy.fillna(False).astype(bool)
    imputed.loc[high_name_mask] = True
    source_column.loc[high_name_mask] = name_column

    default_false_mask = imputed.isna()
    imputed.loc[default_false_mask] = False
    source_column.loc[default_false_mask] = "imputed_default_false"

    print(
        f"[analysis] {output_column}: populated={int(imputed.notna().sum()):,} "
        f"proxy_fill={int(proxy_fill_mask.sum()):,} high_name={int(high_name_mask.sum()):,} "
        f"default_false={int(default_false_mask.sum()):,}",
        flush=True,
    )
    return imputed.astype("boolean"), source_column, source_rule


def _build_region_metrics_output(analysis_df: pd.DataFrame) -> pd.DataFrame:
    """Build region-level efile metrics for the current analysis scope."""
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
    region_year_pairs = (
        analysis_df[["region", "tax_year"]]
        .drop_duplicates()
        .sort_values(["region", "tax_year"], kind="mergesort")
        .itertuples(index=False, name=None)
    )
    region_year_pairs = list(region_year_pairs)

    output_rows: list[dict[str, Any]] = []
    for exclusion_variant, scope_mask in tqdm(scope_specs, desc="efile region metric scopes", unit="scope"):
        scoped_df = analysis_df.loc[scope_mask].copy()
        grouped = {
            (region, tax_year): group_df
            for (region, tax_year), group_df in scoped_df.groupby(["region", "tax_year"], dropna=False, sort=True)
        }
        for region, tax_year in region_year_pairs:
            group_df = grouped.get((region, tax_year), scoped_df.iloc[0:0].copy())
            filing_count = int(len(group_df))
            unique_ein_count = int(group_df["ein"].dropna().astype("string").nunique())
            revenue_sum = group_df["analysis_total_revenue_amount"].sum(min_count=1)
            expense_sum = group_df["analysis_total_expense_amount"].sum(min_count=1)
            asset_sum = group_df["analysis_total_assets_amount"].sum(min_count=1)
            surplus_sum = group_df["analysis_calculated_surplus_amount"].sum(min_count=1)
            avg_net_margin = group_df["analysis_calculated_net_margin_ratio"].mean() if filing_count else pd.NA
            avg_months_reserves = group_df["analysis_calculated_months_of_reserves"].mean() if filing_count else pd.NA
            output_rows.append(
                {
                    "region": region,
                    "tax_year": tax_year,
                    "analysis_exclusion_variant": exclusion_variant,
                    "nonprofit_filing_count": filing_count,
                    "unique_nonprofit_ein_count": unique_ein_count,
                    "analysis_total_revenue_amount_sum": revenue_sum if pd.notna(revenue_sum) else pd.NA,
                    "analysis_total_expense_amount_sum": expense_sum if pd.notna(expense_sum) else pd.NA,
                    "analysis_total_assets_amount_sum": asset_sum if pd.notna(asset_sum) else pd.NA,
                    "analysis_net_asset_amount_sum": asset_sum if pd.notna(asset_sum) else pd.NA,
                    "analysis_calculated_surplus_amount_sum": surplus_sum if pd.notna(surplus_sum) else pd.NA,
                    "analysis_calculated_normalized_total_revenue_per_nonprofit": (float(revenue_sum) / filing_count) if filing_count and pd.notna(revenue_sum) else pd.NA,
                    "analysis_calculated_normalized_total_assets_per_nonprofit": (float(asset_sum) / filing_count) if filing_count and pd.notna(asset_sum) else pd.NA,
                    "analysis_calculated_normalized_net_asset_per_nonprofit": (float(asset_sum) / filing_count) if filing_count and pd.notna(asset_sum) else pd.NA,
                    "analysis_calculated_normalized_total_revenue_per_unique_nonprofit": (float(revenue_sum) / unique_ein_count) if unique_ein_count and pd.notna(revenue_sum) else pd.NA,
                    "analysis_calculated_normalized_total_assets_per_unique_nonprofit": (float(asset_sum) / unique_ein_count) if unique_ein_count and pd.notna(asset_sum) else pd.NA,
                    "analysis_calculated_normalized_net_asset_per_unique_nonprofit": (float(asset_sum) / unique_ein_count) if unique_ein_count and pd.notna(asset_sum) else pd.NA,
                    "analysis_calculated_average_net_margin_ratio": avg_net_margin if pd.notna(avg_net_margin) else pd.NA,
                    "analysis_calculated_average_months_of_reserves": avg_months_reserves if pd.notna(avg_months_reserves) else pd.NA,
                    "analysis_imputed_is_hospital_true_count": int(group_df["analysis_imputed_is_hospital"].fillna(False).astype(bool).sum()),
                    "analysis_imputed_is_university_true_count": int(group_df["analysis_imputed_is_university"].fillna(False).astype(bool).sum()),
                    "analysis_imputed_is_political_org_true_count": int(group_df["analysis_imputed_is_political_org"].fillna(False).astype(bool).sum()),
                }
            )
    return pd.DataFrame(output_rows).sort_values(["region", "tax_year", "analysis_exclusion_variant"], kind="mergesort").reset_index(drop=True)


def _build_coverage_rows(
    analysis_df: pd.DataFrame,
    *,
    metadata_rows: list[dict[str, str]],
) -> list[dict[str, Any]]:
    """Build a simple long coverage table with overall and year/form slices."""
    coverage_rows: list[dict[str, Any]] = []
    group_pairs = [("all", "all")] + list(
        analysis_df[["tax_year", "form_type"]]
        .drop_duplicates()
        .sort_values(["tax_year", "form_type"], kind="mergesort")
        .itertuples(index=False, name=None)
    )
    for metadata_row in tqdm(metadata_rows, desc="efile coverage variables", unit="variable"):
        variable_name = metadata_row["canonical_variable"]
        for tax_year, form_type in group_pairs:
            if tax_year == "all":
                group_df = analysis_df
            else:
                group_df = analysis_df.loc[
                    analysis_df["tax_year"].astype("string").eq(str(tax_year))
                    & analysis_df["form_type"].astype("string").eq(str(form_type))
                ]
            row_count = int(len(group_df))
            populated_count = int(series_has_value(group_df[variable_name]).sum()) if row_count else 0
            coverage_rows.append(
                {
                    "canonical_variable": variable_name,
                    "availability_status": "available",
                    "variable_role": metadata_row["variable_role"],
                    "draft_variable": metadata_row["draft_variable"],
                    "tax_year": tax_year,
                    "form_type": form_type,
                    "row_count": row_count,
                    "populated_count": populated_count,
                    "fill_rate": (populated_count / row_count) if row_count else pd.NA,
                    "notes": metadata_row["notes"],
                }
            )
    for unavailable in UNAVAILABLE_VARIABLES:
        coverage_rows.append(
            {
                "canonical_variable": unavailable["canonical_variable"],
                "availability_status": unavailable["availability_status"],
                "variable_role": unavailable["variable_role"],
                "draft_variable": unavailable["draft_variable"],
                "tax_year": "all",
                "form_type": "all",
                "row_count": int(len(analysis_df)),
                "populated_count": 0,
                "fill_rate": 0.0,
                "notes": unavailable["notes"],
            }
        )
    return coverage_rows


def _write_mapping_markdown(mapping_path: Path, metric_rows: list[dict[str, str]]) -> None:
    """Write a concise but explicit efile analysis mapping document."""
    lines = [
        "# NCCS Efile Analysis Variable Mapping",
        "",
        "- Source family: `nccs_efile`",
        f"- Analysis output: `{_repo_relative_display(analysis_variables_output_path())}`",
        f"- Region metrics output: `{_repo_relative_display(analysis_geography_metrics_output_path())}`",
        f"- Coverage report: `{_repo_relative_display(analysis_variable_coverage_path())}`",
        "- Scope: `2022-2024` only",
        "- Coding rules: [`CODING_RULES.md`](../../secrets/coding_rules/CODING_RULES.md)",
        "",
        "## Extracted Variables",
        "",
        "|canonical_variable|variable_role|draft_variable|source_rule|provenance_column|notes|",
        "|---|---|---|---|---|---|",
    ]
    for row in metric_rows:
        lines.append(f"|{row['canonical_variable']}|{row['variable_role']}|{row['draft_variable']}|{row['source_rule']}|{row['provenance_column']}|{row['notes']}|")
    lines.extend(["", "## Unavailable Variables", "", "|canonical_variable|availability_status|draft_variable|notes|", "|---|---|---|---|"])
    for row in UNAVAILABLE_VARIABLES:
        lines.append(f"|{row['canonical_variable']}|{row['availability_status']}|{row['draft_variable']}|{row['notes']}|")
    lines.extend(["", "## Draft Alignment Appendix", "", "|draft_variable|source_specific_output|status|rule_or_reason|", "|---|---|---|---|"])
    for row in metric_rows:
        lines.append(f"|{row['draft_variable']}|{row['canonical_variable']}|{row['variable_role']}|{row['source_rule']}|")
    for row in UNAVAILABLE_VARIABLES:
        lines.append(f"|{row['draft_variable']}|{row['canonical_variable']}|unavailable|{row['notes']}|")
    mapping_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_data_processing_doc(doc_path: Path) -> None:
    """Write an authoritative source-specific data-processing doc for the efile pipeline."""
    raw_dir = META_DIR.parent / "raw"
    doc = f"""# NCCS Efile Pipeline

## Status

This document is the authoritative technical reference for the current NCCS efile pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_efile/`, including the current analysis window of `2022-2024`, the current filter-first annual benchmark build in step 05, the row-level analysis extraction in step 08, and the official analysis upload in step 09.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the efile data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw annual CSV tables become benchmark-filtered annual filing parquets
- how the final analysis-ready efile dataset is derived from those annual outputs
- what is direct, calculated, enriched, proxy, imputed, or unavailable in the final analysis package

## Pipeline At A Glance

The NCCS efile pipeline turns published NCCS parsed-return tables into annual benchmark-filtered filing rowsets and then into official analysis outputs.

At a high level, the pipeline has these phases:

1. Discover which annual efile assets should be used.
2. Download the required annual raw tables.
3. Upload the raw files and metadata to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw inputs.
5. Build one benchmark-filtered annual parquet per tax year.
6. Upload the annual benchmark outputs to Silver S3.
7. Optionally compare the annual efile outputs against NCCS Core for audit purposes.
8. Build the final efile analysis outputs for `2022-2024`.
9. Upload the final analysis outputs and metadata to the dedicated efile analysis S3 prefix.

The main artifact classes are:

- Raw data: annual efile CSV tables downloaded from the public NCCS release.
- Metadata/manifests: release-selection metadata, HTML snapshots, download manifests, and verification reports.
- Silver filtered artifacts: one benchmark-filtered annual filing parquet per tax year.
- Final analysis artifacts: the row-level analysis parquet, region metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows the `CODING_RULES.md` filtered-only combine contract:

- the wide table combine happens only after HEADER rows have already been admitted to benchmark geography
- filing selection happens before downstream joins to SUMMARY and Schedule A
- the final analysis step reads only the already-filtered annual benchmark parquets

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_efile_release.py` | Probe the public NCCS efile release and select the annual assets to use | Remote HTML pages and HEAD probes | `latest_release.json`, HTML snapshots | HTTP |
| 02 | `02_download_efile_release.py` | Download the selected annual CSV tables | Release manifest and remote table URLs | Raw annual CSV tables plus download manifest | HTTP |
| 03 | `03_upload_efile_release_to_s3.py` | Upload raw files and discovery metadata to Bronze | Local raw CSVs and metadata | Bronze raw objects and metadata objects | S3 |
| 04 | `04_verify_efile_source_local_s3.py` | Verify source/local/S3 byte parity for raw inputs | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_build_efile_benchmark_local.py` | Build annual benchmark-filtered efile parquets | Raw annual CSV tables, benchmark geography crosswalks | `nccs_efile_benchmark_tax_year=YYYY.parquet` and annual manifests | local files |
| 06 | `06_upload_filtered_efile_to_s3.py` | Upload annual benchmark outputs to Silver | Annual benchmark parquets and metadata | Silver annual benchmark objects | S3 |
| 07 | `07_compare_efile_vs_core_local.py` | Audit-only comparison between efile and NCCS Core | Filtered efile and filtered NCCS Core outputs | Comparison CSVs and notes | local files |
| 08 | `08_extract_analysis_variables_local.py` | Build official row-level analysis outputs for `2022-2024` | Annual benchmark parquets plus BMF/IRS classification fallbacks | Row-level analysis parquet, region metrics parquet, coverage CSV, mapping doc, this processing doc | local files |
| 09 | `09_upload_analysis_outputs.py` | Upload official analysis outputs and verify local-vs-S3 bytes | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

### Step 01: release discovery

The discovery step probes the NCCS efile release pages and determines which annual assets should be used for the benchmark-analysis workflow.

The required table families are:

- `F9-P00-T00-HEADER`
- `F9-P01-T00-SUMMARY`
- `SA-P01-T00-PUBLIC-CHARITY-STATUS`

The discovery step writes `latest_release.json` plus HTML snapshots so later runs can replay the exact selected asset set.

### Step 02: raw annual download

The download step materializes one CSV per required table per selected year.

The local raw layout is partitioned by:

- `tax_year=...`
- `table=...`

This lets the benchmark builder read only the tables it actually needs.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw files and release metadata to Bronze and then verifies source/local/S3 byte parity.

This preserves:

- raw local copies
- Bronze raw copies
- a verification CSV recording whether source, local, and S3 byte counts agree

## Where Data Is Stored

### Local directories

- Local raw CSV directory: `{_repo_relative_display(raw_dir)}`
- Local metadata directory: `{_repo_relative_display(META_DIR)}`
- Local staging directory: `{_repo_relative_display(STAGING_DIR)}`

### Analysis artifacts

- Row-level analysis parquet: `{_repo_relative_display(analysis_variables_output_path())}`
- Region metrics parquet: `{_repo_relative_display(analysis_geography_metrics_output_path())}`
- Coverage CSV: `{_repo_relative_display(analysis_variable_coverage_path())}`
- Mapping doc: `{_repo_relative_display(analysis_variable_mapping_path())}`
- Data-processing doc: `{_repo_relative_display(analysis_data_processing_doc_path())}`

### S3 layout

- Silver annual benchmark prefix: `silver/nccs_efile/`
- Silver annual benchmark metadata prefix: `silver/nccs_efile/metadata/`
- Official S3 analysis prefix: `silver/nccs_efile/analysis/`
- Official S3 analysis metadata prefix: `silver/nccs_efile/analysis/metadata/`

## Step-By-Step Transformation

### Step 05: annual benchmark build

Step 05 is the key upstream curation step for efile.

It uses HEADER rows to determine:

- benchmark geography admission
- the retained filing key for each EIN-year
- which rows should survive into the annual benchmark parquet before wide joins happen

Only after those retained benchmark filings are identified does the step join in:

- SUMMARY
- Schedule A public-charity support fields

This means the annual combine runs only on admitted benchmark filings rather than the broad national raw rowset.

### Annual benchmark outputs

The annual benchmark parquet harmonizes the core fields needed downstream into `harm_*` columns, including:

- identity
- geography
- revenue
- expense
- assets
- income
- filing-form context
- direct hospital and university flags when present

These annual benchmark artifacts are the stable upstream source of truth for the final analysis build.

### Step 08: analysis-year union

The analysis step restricts scope to `2022-2024` and loads the annual benchmark parquets for only those years.

It does not change row admission. It unions the already-filtered annual outputs and then derives the analysis variables from that preserved filing rowset.

The final row grain is one filing row per retained `EIN + tax_year + filing form` from the annual benchmark outputs.

### Direct amount extraction

Direct row-level variables include:

- `analysis_total_revenue_amount` from `harm_revenue_amount`
- `analysis_total_expense_amount` from `harm_expenses_amount`
- `analysis_total_assets_amount` from `harm_assets_amount`
- `analysis_calculated_surplus_amount` from `harm_income_amount`

The current pipeline also keeps `analysis_net_asset_amount` as the repo's conservative asset-based proxy field rather than claiming a guaranteed direct GAAP net-asset amount.

Blank or nonnumeric strings are converted to missing values instead of being coerced to zero.

### Calculated row-level metrics

The main calculated metrics are:

- `analysis_calculated_net_margin_ratio`
- `analysis_calculated_months_of_reserves`
- `analysis_calculated_ntee_broad_code`

The ratio fields are written only when the required denominator is positive. They remain null when the denominator is missing, zero, or negative.

### Classification enrichment

The analysis step enriches `analysis_ntee_code` and `analysis_subsection_code` with a strict fallback order:

1. exact-year NCCS BMF lookup by `ein + tax_year`
2. nearest-year NCCS BMF lookup by `ein`
3. IRS EO BMF lookup by `ein`

All resolved fills preserve:

- source family
- source variant
- source column

### Source-backed and imputed exclusion flags

The source-backed flag family is:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

The fully populated analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed columns prefer canonical source-backed values first, then source-backed proxy logic, then high-confidence organization-name matches, then default `False`.

### Region metrics

The region metrics parquet groups by:

- `region`
- `tax_year`
- `analysis_exclusion_variant`

Two exclusion variants are written:

- `all_rows`
- `exclude_imputed_hospital_university_political_org`

Metrics include:

- filing count
- unique EIN count
- revenue, expense, assets, and surplus sums
- normalized totals per filing
- normalized totals per unique EIN
- average net margin
- average months of reserves
- counts of imputed hospital, university, and political-organization flags

### Coverage, mapping, and upload verification

The coverage CSV records populated counts and fill rates by variable and by `tax_year + form_type` slice.

The mapping Markdown documents which variables are:

- direct
- calculated
- enriched
- proxy
- imputed
- unavailable

The upload step prints explicit local-vs-S3 byte checks for every analysis artifact and raises if any uploaded object size does not match local bytes.

## Final Analysis Outputs

The official efile analysis package consists of:

- one row-level analysis parquet
- one region metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is the canonical efile analysis-ready layer for `2022-2024`.

## Variable Families

- Direct-source variables: `analysis_total_revenue_amount`, `analysis_total_expense_amount`, `analysis_total_assets_amount`, `analysis_calculated_surplus_amount`, `analysis_is_hospital`, `analysis_is_university`
- Calculated variables: `analysis_calculated_net_margin_ratio`, `analysis_calculated_months_of_reserves`, `analysis_calculated_ntee_broad_code`
- Enriched variables: `analysis_ntee_code`, `analysis_subsection_code`, and their provenance columns
- Proxy variables: `analysis_net_asset_amount`, `analysis_is_political_org`
- Imputed variables: `analysis_imputed_is_hospital`, `analysis_imputed_is_university`, `analysis_imputed_is_political_org`
- Explicitly unavailable variables remain unavailable rather than being weakly backfilled from GT

## Testing And Verification

- Unit tests live in `tests/nccs_efile/test_nccs_efile_common.py`.
- The analysis tests cover row preservation, year restriction, numeric extraction, classification fallback behavior, calculated metrics, and region aggregation.
- Operational verification includes raw source/local/S3 byte checks, annual benchmark upload flows, and analysis upload byte checks.

## Current Caveats

- The current efile benchmark layer does not expose GT-style contribution or grant component fields, so those variables remain unavailable here.
- `analysis_net_asset_amount` is still an asset-based proxy and should not be confused with a guaranteed raw net-asset field.
- The direct hospital and university fields are source-sparse because the underlying efile flags are sparse. The imputed versions are complete because they continue through proxy/name/default logic.
- The main runner currently executes sequentially rather than exposing GT-style start-step/end-step selection.

## Draft Alignment Appendix

| draft_variable | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| Total revenue | analysis_total_revenue_amount | direct | Direct from `harm_revenue_amount` |
| Total expense | analysis_total_expense_amount | direct | Direct from `harm_expenses_amount` |
| Total assets | analysis_total_assets_amount | direct | Direct from `harm_assets_amount` |
| Net asset | analysis_net_asset_amount | proxy | Conservative asset-based proxy from `harm_assets_amount` |
| Net margin | analysis_calculated_net_margin_ratio | calculated | Surplus divided by positive total revenue |
| Months of reserves | analysis_calculated_months_of_reserves | calculated | (Net-asset proxy / positive total expense) * 12 |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year, nearest-year, then IRS EO BMF fallback |
| Program service revenue | analysis_program_service_revenue_amount | unavailable | Not present in the current efile benchmark artifact |
| Total contributions | analysis_calculated_total_contributions_amount | unavailable | Contribution-component fields are not present in the current efile benchmark artifact |
| Other contributions | analysis_other_contributions_amount | unavailable | Contribution-component fields are not present in the current efile benchmark artifact |
| Grants (total amount) | analysis_calculated_grants_total_amount | unavailable | Grant-component fields are not present in the current efile benchmark artifact |
"""
    doc_path.write_text(doc.strip() + "\n", encoding="utf-8")


def build_analysis_outputs(
    *,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
) -> dict[str, Any]:
    """Build all NCCS efile analysis outputs and return a summary dict."""
    analysis_variables_path = analysis_variables_output_path(staging_dir)
    analysis_metrics_path = analysis_geography_metrics_output_path(staging_dir)
    coverage_path = analysis_variable_coverage_path(metadata_dir)
    mapping_path = analysis_variable_mapping_path()
    processing_doc_path = analysis_data_processing_doc_path()
    _ensure_parent_dirs(analysis_variables_path, analysis_metrics_path, coverage_path, mapping_path, processing_doc_path)

    release = resolve_release_and_write_metadata(metadata_dir, start_year=ANALYSIS_TAX_YEAR_MIN)
    grouped_assets = grouped_assets_by_year(release)
    analysis_years = [year for year in sorted(grouped_assets) if ANALYSIS_TAX_YEAR_MIN <= int(year) <= ANALYSIS_TAX_YEAR_MAX]
    if not analysis_years:
        raise RuntimeError("No NCCS efile benchmark years were discovered for the 2022-2024 analysis scope.")

    print(f"[analysis] Analysis years: {analysis_years}", flush=True)
    year_frames: list[pd.DataFrame] = []
    for tax_year in tqdm(analysis_years, desc="load efile benchmark years", unit="year"):
        year_path = staging_dir / f"tax_year={tax_year}" / f"nccs_efile_benchmark_tax_year={tax_year}.parquet"
        if not year_path.exists():
            raise FileNotFoundError(f"Missing annual efile benchmark parquet: {year_path}")
        frame = pd.read_parquet(year_path)
        print(f"[analysis] tax_year={tax_year} rows={len(frame):,} path={year_path}", flush=True)
        year_frames.append(frame)

    source_df = pd.concat(year_frames, ignore_index=True)
    print(f"[analysis] Combined efile source rows: {len(source_df):,}", flush=True)

    source_df["ein"] = source_df["harm_ein"].astype("string")
    source_df["tax_year"] = source_df["harm_tax_year"].astype("string")
    source_df["form_type"] = source_df["harm_filing_form"].astype("string")
    source_df["org_name"] = source_df["harm_org_name"].astype("string")
    source_df["state"] = source_df["harm_state"].astype("string")
    source_df["zip5"] = source_df["harm_zip5"].astype("string")
    source_df["county_fips"] = source_df["harm_county_fips"].astype("string")
    source_df["region"] = source_df["harm_region"].astype("string")

    analysis_df = pd.DataFrame(
        {
            "ein": source_df["ein"],
            "tax_year": source_df["tax_year"],
            "form_type": source_df["form_type"],
            "org_name": source_df["org_name"],
            "state": source_df["state"],
            "zip5": source_df["zip5"],
            "county_fips": source_df["county_fips"],
            "region": source_df["region"],
        }
    )

    analysis_df["analysis_total_revenue_amount"], analysis_df["analysis_total_revenue_amount_source_column"] = _build_direct_numeric(source_df, "harm_revenue_amount", "analysis_total_revenue_amount")
    analysis_df["analysis_total_expense_amount"], analysis_df["analysis_total_expense_amount_source_column"] = _build_direct_numeric(source_df, "harm_expenses_amount", "analysis_total_expense_amount")
    analysis_df["analysis_total_assets_amount"], analysis_df["analysis_total_assets_amount_source_column"] = _build_direct_numeric(source_df, "harm_assets_amount", "analysis_total_assets_amount")
    analysis_df["analysis_net_asset_amount"] = analysis_df["analysis_total_assets_amount"].copy()
    analysis_df["analysis_net_asset_amount_source_column"] = analysis_df["analysis_total_assets_amount_source_column"].copy()
    analysis_df["analysis_net_asset_amount_quality_flag"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df.loc[analysis_df["analysis_net_asset_amount"].notna(), "analysis_net_asset_amount_quality_flag"] = "asset_based_proxy"
    analysis_df["analysis_calculated_surplus_amount"], analysis_df["analysis_calculated_surplus_amount_source_column"] = _build_direct_numeric(source_df, "harm_income_amount", "analysis_calculated_surplus_amount")
    analysis_df["analysis_calculated_net_margin_ratio"], analysis_df["analysis_calculated_net_margin_ratio_source_column"] = build_ratio_metric(
        analysis_df["analysis_calculated_surplus_amount"],
        analysis_df["analysis_total_revenue_amount"],
        provenance_label="derived:analysis_calculated_surplus_amount-divided-by-analysis_total_revenue_amount",
        require_positive_denominator=True,
    )
    months_ratio, months_provenance = build_ratio_metric(
        analysis_df["analysis_net_asset_amount"],
        analysis_df["analysis_total_expense_amount"],
        provenance_label="derived:analysis_net_asset_amount-divided-by-analysis_total_expense_amount-times-12",
        require_positive_denominator=True,
    )
    analysis_df["analysis_calculated_months_of_reserves"] = (months_ratio * 12).astype("Float64")
    analysis_df["analysis_calculated_months_of_reserves_source_column"] = months_provenance

    required_tax_years = sorted(source_df["tax_year"].dropna().astype("string").unique().tolist())
    bmf_lookup_df = load_bmf_classification_lookup(required_tax_years)
    irs_lookup_df = load_irs_bmf_classification_lookup()
    classification_summary = apply_classification_fallbacks(
        analysis_df,
        source_df,
        state_column="state",
        bmf_lookup_df=bmf_lookup_df,
        irs_lookup_df=irs_lookup_df,
    )
    analysis_df["analysis_calculated_ntee_broad_code"], analysis_df["analysis_calculated_ntee_broad_code_source_column"] = _build_broad_ntee(analysis_df["analysis_ntee_code"])

    analysis_df["analysis_is_hospital"] = normalize_bool_text(source_df["harm_is_hospital"])
    analysis_df["analysis_is_hospital_source_column"] = pd.Series("SA_01_PCSTAT_HOSPITAL_X", index=analysis_df.index, dtype="string")
    analysis_df.loc[analysis_df["analysis_is_hospital"].isna(), "analysis_is_hospital_source_column"] = pd.NA

    analysis_df["analysis_is_university"] = normalize_bool_text(source_df["harm_is_university"])
    analysis_df["analysis_is_university_source_column"] = pd.Series("SA_01_PCSTAT_SCHOOL_X", index=analysis_df.index, dtype="string")
    analysis_df.loc[analysis_df["analysis_is_university"].isna(), "analysis_is_university_source_column"] = pd.NA

    analysis_df["analysis_hospital_proxy_from_ntee"], analysis_df["analysis_hospital_proxy_from_ntee_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("E20", "E21", "E22", "E24"))
    analysis_df["analysis_university_proxy_from_ntee"], analysis_df["analysis_university_proxy_from_ntee_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("B40", "B41", "B42", "B43", "B50"))
    analysis_df["analysis_is_political_org"], analysis_df["analysis_is_political_org_source_column"] = build_political_proxy_flag(analysis_df["analysis_subsection_code"])

    analysis_df["analysis_imputed_is_hospital"], analysis_df["analysis_imputed_is_hospital_source_column"], analysis_df["analysis_imputed_is_hospital_source_rule"] = _build_imputed_from_direct_and_proxy(
        analysis_df,
        canonical_column="analysis_is_hospital",
        proxy_column="analysis_hospital_proxy_from_ntee",
        name_column="org_name",
        high_patterns=(r"\bhospital\b", r"\bmedical center\b", r"\bhealth system\b"),
        medium_patterns=(r"\bambulance district\b", r"\bcommunity health center\b"),
        block_patterns=(r"\bmedical records\b", r"\bhealthcare\b", r"\bhealth care\b"),
        output_column="analysis_imputed_is_hospital",
    )
    analysis_df["analysis_imputed_is_university"], analysis_df["analysis_imputed_is_university_source_column"], analysis_df["analysis_imputed_is_university_source_rule"] = _build_imputed_from_direct_and_proxy(
        analysis_df,
        canonical_column="analysis_is_university",
        proxy_column="analysis_university_proxy_from_ntee",
        name_column="org_name",
        high_patterns=(r"\buniversity\b", r"\bcollege\b", r"\bcollegiate\b", r"\bDWU\b", r"\bNAU\b", r"\bSDSU\b", r"\bUSD\b", r"\bMSU\b"),
        medium_patterns=(r"\bcampus\b", r"\bstudent\b", r"\balumni\b", r"\bdepartment\b"),
        block_patterns=(r"\bfire department\b", r"\bpolice department\b"),
        output_column="analysis_imputed_is_university",
    )
    political_name_proxy, political_confidence = build_name_proxy_flag(
        analysis_df["org_name"].astype("string"),
        analysis_df["analysis_is_political_org"].astype("boolean"),
        (r"\bvoters?\b", r"\bpolitical action committee\b", r"\bPAC\b", r"\bcampaign\b", r"\belection\b", r"\bballot\b", r"\bparty\b"),
        (r"\baction\b", r"\bamerica\b", r"\bvoices\b", r"\bworking\b"),
        (r"\banimal\b", r"\barts\b", r"\bhistory\b", r"\bmuseum\b"),
    )
    political_imputed = analysis_df["analysis_is_political_org"].astype("boolean").copy()
    political_source = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    political_source.loc[analysis_df["analysis_is_political_org"].notna()] = analysis_df["analysis_is_political_org_source_column"].loc[analysis_df["analysis_is_political_org"].notna()].astype("string")
    political_high_mask = political_imputed.isna() & political_confidence.fillna("").eq("high") & political_name_proxy.fillna(False).astype(bool)
    political_default_false_mask = political_imputed.isna() & ~political_confidence.fillna("").eq("high")
    political_imputed.loc[political_high_mask] = True
    political_imputed.loc[political_default_false_mask] = False
    political_source.loc[political_high_mask] = "org_name"
    political_source.loc[political_default_false_mask] = "imputed_default_false"
    analysis_df["analysis_imputed_is_political_org"] = political_imputed.astype("boolean")
    analysis_df["analysis_imputed_is_political_org_source_column"] = political_source
    analysis_df["analysis_imputed_is_political_org_source_rule"] = pd.Series("source-backed subsection proxy, then high-confidence name match, then default False", index=analysis_df.index, dtype="string")

    region_metrics_df = _build_region_metrics_output(analysis_df)
    analysis_df.drop(columns=["analysis_hospital_proxy_from_ntee", "analysis_hospital_proxy_from_ntee_source_column", "analysis_university_proxy_from_ntee", "analysis_university_proxy_from_ntee_source_column"], inplace=True)

    analysis_df.sort_values(["tax_year", "region", "county_fips", "ein"], inplace=True, kind="mergesort")
    region_metrics_df.sort_values(["region", "tax_year", "analysis_exclusion_variant"], inplace=True, kind="mergesort")
    analysis_df.to_parquet(analysis_variables_path, index=False)
    region_metrics_df.to_parquet(analysis_metrics_path, index=False)
    print(f"[analysis] Wrote analysis variables: {analysis_variables_path}", flush=True)
    print(f"[analysis] Wrote analysis geography metrics: {analysis_metrics_path}", flush=True)
    print(f"[analysis] Row counts in/out: {len(source_df):,} -> {len(analysis_df):,}", flush=True)

    final_variables = [
        "analysis_total_revenue_amount", "analysis_total_expense_amount", "analysis_total_assets_amount", "analysis_net_asset_amount",
        "analysis_calculated_surplus_amount", "analysis_calculated_net_margin_ratio",
        "analysis_calculated_months_of_reserves", "analysis_ntee_code", "analysis_calculated_ntee_broad_code",
        "analysis_subsection_code", "analysis_is_hospital", "analysis_is_university",
        "analysis_is_political_org", "analysis_imputed_is_hospital", "analysis_imputed_is_university",
        "analysis_imputed_is_political_org",
    ]
    coverage_df = pd.DataFrame(_build_coverage_rows(analysis_df, metadata_rows=AVAILABLE_VARIABLE_METADATA))
    coverage_df.to_csv(coverage_path, index=False)
    print(f"[analysis] Wrote coverage report: {coverage_path}", flush=True)

    _write_mapping_markdown(mapping_path, AVAILABLE_VARIABLE_METADATA)
    _write_data_processing_doc(processing_doc_path)

    print("[analysis] Counts by tax_year and form_type:", flush=True)
    counts = analysis_df.groupby(["tax_year", "form_type"], dropna=False).size().reset_index(name="row_count").sort_values(["tax_year", "form_type"], kind="mergesort")
    for row in counts.itertuples(index=False):
        print(f"[analysis]   tax_year={row.tax_year} form_type={row.form_type} row_count={row.row_count:,}", flush=True)
    print("[analysis] Fill summary for key variables:", flush=True)
    for variable_name in final_variables:
        populated_count = int(series_has_value(analysis_df[variable_name]).sum())
        print(f"[analysis]   {variable_name}: populated={populated_count:,} fill_rate={(populated_count / len(analysis_df)):.3%}", flush=True)

    unresolved_count = len(UNAVAILABLE_VARIABLES)
    print(f"[analysis] Explicit unavailable-variable count: {unresolved_count}", flush=True)
    print(f"[analysis] Classification fallback summary: {classification_summary}", flush=True)
    return {
        "source_row_count": int(len(source_df)),
        "analysis_row_count": int(len(analysis_df)),
        "region_metric_row_count": int(len(region_metrics_df)),
        "analysis_years": analysis_years,
        "classification_summary": classification_summary,
        "unavailable_variable_count": unresolved_count,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build NCCS efile analysis outputs for 2022-2024.")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging root directory")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Unused here but logged to mirror pipeline conventions")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Unused here but logged to mirror pipeline conventions")
    parser.add_argument("--analysis-prefix", default=ANALYSIS_PREFIX, help="Logged official analysis S3 prefix")
    parser.add_argument("--analysis-meta-prefix", default=ANALYSIS_META_PREFIX, help="Logged official analysis metadata S3 prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 08 - BUILD NCCS EFILE ANALYSIS OUTPUTS")
    load_env_from_secrets()
    print(f"[analysis] Metadata dir: {args.metadata_dir}", flush=True)
    print(f"[analysis] Staging dir: {args.staging_dir}", flush=True)
    print(f"[analysis] Official analysis prefix: {args.analysis_prefix}", flush=True)
    print(f"[analysis] Official analysis metadata prefix: {args.analysis_meta_prefix}", flush=True)
    summary = build_analysis_outputs(metadata_dir=args.metadata_dir, staging_dir=args.staging_dir)
    print(f"[analysis] Summary: {summary}", flush=True)
    print_elapsed(start, "Step 08")


if __name__ == "__main__":
    main()
