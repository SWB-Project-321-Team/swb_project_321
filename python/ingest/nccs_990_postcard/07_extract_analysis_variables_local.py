"""
Step 07: Build NCCS postcard analysis outputs for the 2022-2024 analysis window.
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
    POSTCARD_TAX_YEAR_START_DEFAULT,
    STAGING_DIR,
    analysis_data_processing_doc_path,
    analysis_geography_metrics_output_path,
    analysis_variable_coverage_path,
    analysis_variable_mapping_path,
    analysis_variables_output_path,
    banner,
    filtered_tax_year_window_parquet_path,
    load_env_from_secrets,
    normalize_ein9,
    print_elapsed,
    resolve_release_and_write_metadata,
)
from ingest._shared.analysis_support import (
    NAME_PROXY_FLAG_SPECS,
    apply_classification_fallbacks,
    apply_imputed_flag_family,
    build_ntee_proxy_flag,
    build_political_proxy_flag,
    discover_bmf_exact_year_lookup_inputs,
    load_bmf_classification_lookup,
    load_irs_bmf_classification_lookup,
    normalize_bool_text,
    series_has_value,
)


UNAVAILABLE_VARIABLES = [
    {"canonical_variable": "analysis_total_revenue_amount", "variable_role": "unavailable", "draft_variable": "Total revenue", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_program_service_revenue_amount", "variable_role": "unavailable", "draft_variable": "Program service revenue", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_calculated_total_contributions_amount", "variable_role": "unavailable", "draft_variable": "Total contributions", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_calculated_grants_total_amount", "variable_role": "unavailable", "draft_variable": "Grants (total amount)", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_total_expense_amount", "variable_role": "unavailable", "draft_variable": "Total expense", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_net_asset_amount", "variable_role": "unavailable", "draft_variable": "Net asset", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_calculated_surplus_amount", "variable_role": "unavailable", "draft_variable": "Surplus", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_calculated_net_margin_ratio", "variable_role": "unavailable", "draft_variable": "Net margin", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
    {"canonical_variable": "analysis_calculated_months_of_reserves", "variable_role": "unavailable", "draft_variable": "Months of reserves", "notes": "Postcard does not carry this financial-performance or revenue-source field."},
]

AVAILABLE_VARIABLE_METADATA = [
    {"canonical_variable": "analysis_ntee_code", "variable_role": "enriched", "draft_variable": "NTEE filed classification code", "source_rule": "NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback", "provenance_column": "analysis_ntee_code_source_column", "notes": "Classification enrichment only."},
    {"canonical_variable": "analysis_subsection_code", "variable_role": "enriched", "draft_variable": "Subsection code", "source_rule": "NCCS BMF exact-year, then nearest-year, then IRS EO BMF EIN fallback", "provenance_column": "analysis_subsection_code_source_column", "notes": "Supports political-organization classification."},
    {"canonical_variable": "analysis_calculated_ntee_broad_code", "variable_role": "calculated", "draft_variable": "Broad NTEE field classification code", "source_rule": "First letter of analysis_ntee_code", "provenance_column": "analysis_calculated_ntee_broad_code_source_column", "notes": "Broad NTEE field category."},
    {"canonical_variable": "analysis_is_hospital", "variable_role": "proxy", "draft_variable": "Hospital flag", "source_rule": "NTEE proxy from resolved NTEE code", "provenance_column": "analysis_is_hospital_source_column", "notes": "Source-backed classification proxy."},
    {"canonical_variable": "analysis_is_university", "variable_role": "proxy", "draft_variable": "University flag", "source_rule": "NTEE proxy from resolved NTEE code", "provenance_column": "analysis_is_university_source_column", "notes": "Source-backed classification proxy."},
    {"canonical_variable": "analysis_is_political_org", "variable_role": "proxy", "draft_variable": "Political organization flag", "source_rule": "Subsection proxy from resolved subsection code", "provenance_column": "analysis_is_political_org_source_column", "notes": "Source-backed classification proxy."},
    {"canonical_variable": "analysis_imputed_is_hospital", "variable_role": "proxy", "draft_variable": "Hospital flag", "source_rule": "Canonical proxy, then high-confidence name match, then default False", "provenance_column": "analysis_imputed_is_hospital_source_column", "notes": "Complete analysis-ready hospital flag."},
    {"canonical_variable": "analysis_imputed_is_university", "variable_role": "proxy", "draft_variable": "University flag", "source_rule": "Canonical proxy, then high-confidence name match, then default False", "provenance_column": "analysis_imputed_is_university_source_column", "notes": "Complete analysis-ready university flag."},
    {"canonical_variable": "analysis_imputed_is_political_org", "variable_role": "proxy", "draft_variable": "Political organization flag", "source_rule": "Canonical proxy, then high-confidence name match, then default False", "provenance_column": "analysis_imputed_is_political_org_source_column", "notes": "Complete analysis-ready political flag."},
    {"canonical_variable": "analysis_gross_receipts_under_25000", "variable_role": "direct", "draft_variable": "Gross receipts under $25K flag", "source_rule": "Direct postcard field", "provenance_column": "analysis_gross_receipts_under_25000_source_column", "notes": "Small-filer support field."},
    {"canonical_variable": "analysis_terminated", "variable_role": "direct", "draft_variable": "Terminated flag", "source_rule": "Direct postcard field", "provenance_column": "analysis_terminated_source_column", "notes": "Entity status support field."},
    {"canonical_variable": "analysis_is_small_filer_support", "variable_role": "direct", "draft_variable": "Small-filer support flag", "source_rule": "Constant postcard_pipeline_scope", "provenance_column": "analysis_is_small_filer_support_source_column", "notes": "Documents that postcard is being used only as a small-filer support layer."},
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
    """Build county- and region-level postcard count metrics."""
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

    output_rows: list[dict[str, Any]] = []
    for exclusion_variant, scope_mask in tqdm(scope_specs, desc="postcard geography scopes", unit="scope"):
        scoped_df = analysis_df.loc[scope_mask].copy()
        for geography_level, group_cols in [("region", ["region", "tax_year"]), ("county", ["region", "county_fips", "tax_year"])]:
            grouped = scoped_df.groupby(group_cols, dropna=False, sort=True)
            for keys, group_df in grouped:
                if geography_level == "region":
                    region, tax_year = keys
                    county_fips = pd.NA
                else:
                    region, county_fips, tax_year = keys
                output_rows.append(
                    {
                        "geography_level": geography_level,
                        "region": region,
                        "county_fips": county_fips,
                        "tax_year": tax_year,
                        "analysis_exclusion_variant": exclusion_variant,
                        "nonprofit_filing_count": int(len(group_df)),
                        "analysis_total_nonprofit_count": int(len(group_df)),
                        "unique_nonprofit_ein_count": int(group_df["ein"].dropna().astype("string").nunique()),
                        "analysis_imputed_is_hospital_true_count": int(group_df["analysis_imputed_is_hospital"].fillna(False).astype(bool).sum()),
                        "analysis_imputed_is_university_true_count": int(group_df["analysis_imputed_is_university"].fillna(False).astype(bool).sum()),
                        "analysis_imputed_is_political_org_true_count": int(group_df["analysis_imputed_is_political_org"].fillna(False).astype(bool).sum()),
                        "analysis_gross_receipts_under_25000_true_count": int(group_df["analysis_gross_receipts_under_25000"].fillna(False).astype(bool).sum()),
                        "analysis_terminated_true_count": int(group_df["analysis_terminated"].fillna(False).astype(bool).sum()),
                        "analysis_ntee_code_populated_count": int(series_has_value(group_df["analysis_ntee_code"]).sum()),
                        "analysis_ntee_code_missing_count": int((~series_has_value(group_df["analysis_ntee_code"])).sum()),
                        "analysis_calculated_distinct_broad_ntee_category_count": int(group_df["analysis_calculated_ntee_broad_code"].dropna().astype("string").nunique()),
                    }
                )
    return pd.DataFrame(output_rows).sort_values(
        ["geography_level", "region", "county_fips", "tax_year", "analysis_exclusion_variant"],
        kind="mergesort",
    ).reset_index(drop=True)


def _write_mapping_markdown(mapping_path: Path) -> None:
    lines = [
        "# NCCS 990 Postcard Analysis Variable Mapping",
        "",
        "- Source family: `nccs_990_postcard`",
        f"- Analysis output: `{analysis_variables_output_path()}`",
        f"- Geography metrics output: `{analysis_geography_metrics_output_path()}`",
        f"- Coverage report: `{analysis_variable_coverage_path()}`",
        "- Scope: `2022-2024` retained postcard tax years",
        "",
        "## Available Variables",
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
    doc = f"""# NCCS 990 Postcard Pipeline

## Status

This document is the authoritative technical reference for the current NCCS 990 Postcard pipeline implemented in this repository.

It reflects the code as it exists now under `python/ingest/nccs_990_postcard/`, including the current retained tax-year window of `2022-2024`, the latest-snapshot benchmark derivative written in step 05, the row-level analysis extraction in step 07, and the official analysis upload in step 08.

This document is intentionally detailed. It is meant to let another engineer or analyst understand:

- where the postcard data comes from
- how it is stored locally and in S3
- the exact script order and responsibilities
- how raw monthly postcard CSVs become a retained benchmark-scoped derivative
- how the final postcard analysis package is derived from that derivative
- which variables are supported directly, enriched, imputed, or intentionally unavailable

## Pipeline At A Glance

The postcard pipeline is a latest-snapshot small-filer support layer, not a full annual financial panel.

At a high level, the pipeline has these phases:

1. Discover the latest postcard snapshot year and monthly file set.
2. Download the raw monthly postcard CSVs.
3. Upload the raw files and metadata to Bronze S3.
4. Strictly verify source/local/S3 byte parity for the raw monthly inputs.
5. Filter monthly files to benchmark geography and dedupe to one retained row per EIN in the latest snapshot derivative.
6. Upload the filtered postcard derivatives to Silver S3.
7. Build the final postcard analysis outputs for retained `2022-2024` rows.
8. Upload the final analysis artifacts and metadata to the postcard analysis S3 prefix.

The main artifact classes are:

- Raw data: monthly postcard CSV files exactly as downloaded.
- Metadata/manifests: release-selection metadata, snapshot HTML, download manifests, and verification reports.
- Silver filtered artifacts: retained postcard derivatives built from benchmark-scoped monthly rows.
- Final analysis artifacts: the row-level analysis parquet, geography metrics parquet, coverage CSV, mapping Markdown, and this processing doc.

This pipeline follows the `CODING_RULES.md` filtered-only combine contract:

- each monthly raw file is filtered chunk-by-chunk before retained rows are combined
- the latest-snapshot EIN dedupe happens only after geography admission
- the final analysis step reads only the retained filtered derivative

## Script Order And Responsibilities

| Step | Script | Primary role | Main inputs | Main outputs | External systems |
| ---- | ------ | ------------ | ----------- | ------------ | ---------------- |
| 01 | `01_discover_postcard_release.py` | Discover the latest postcard snapshot year and available monthly files | Remote HTML pages and monthly probes | `latest_release.json`, snapshot HTML | HTTP |
| 02 | `02_download_postcard_release.py` | Download the selected monthly postcard CSVs | Selected monthly URLs | Raw monthly postcard CSVs | HTTP |
| 03 | `03_upload_postcard_release_to_s3.py` | Upload raw postcard files and metadata to Bronze | Local raw monthly files and metadata | Bronze raw postcard objects | S3 |
| 04 | `04_verify_postcard_source_local_s3.py` | Verify raw postcard bytes across source, local disk, and Bronze | Remote sizes, local files, Bronze objects | Raw verification CSV | HTTP, S3 |
| 05 | `05_filter_postcard_to_benchmark_local.py` | Filter to benchmark geography and dedupe to one retained row per EIN in the latest snapshot derivative | Raw postcard CSVs plus benchmark geography reference files | Filtered postcard derivatives including the retained-tax-year parquet | local files |
| 06 | `06_upload_filtered_postcard_to_s3.py` | Upload filtered postcard derivatives to Silver | Filtered postcard outputs and manifests | Silver filtered postcard objects | S3 |
| 07 | `07_extract_analysis_variables_local.py` | Build row-level postcard analysis outputs and geography metrics for retained `2022-2024` rows | Filtered postcard derivative plus BMF/IRS classification fallbacks | Row-level analysis parquet, geography metrics parquet, coverage CSV, mapping doc, this processing doc | local files |
| 08 | `08_upload_analysis_outputs.py` | Upload official analysis outputs and metadata to the Silver analysis prefix | Analysis artifacts and docs | Silver analysis objects | S3 |

## How Data Is Retrieved

### Step 01: snapshot discovery

The postcard pipeline first probes recent months to determine:

- the current published postcard snapshot year
- which monthly files exist within that snapshot

The discovery step writes `latest_release.json` and supporting snapshot HTML so later runs can reproduce the selected monthly input set.

### Step 02: monthly raw download

The download step materializes the selected monthly postcard CSVs exactly as published.

These files are intentionally preserved as raw monthly inputs so the benchmark filter can always trace a retained postcard record back to its source month.

### Step 03 and Step 04: Bronze upload and raw verification

After download, the pipeline uploads the raw monthly files and metadata to Bronze and then verifies source/local/S3 byte parity.

This preserves:

- local raw monthly copies
- Bronze raw copies
- a verification CSV documenting whether the source, local, and S3 byte counts agree

## Where Data Is Stored

### Local directories

- Local raw monthly CSV directory: `{raw_dir}`
- Local metadata directory: `{META_DIR}`
- Local staging directory: `{STAGING_DIR}`

### Analysis artifacts

- Row-level analysis parquet: `{analysis_variables_output_path()}`
- Geography metrics parquet: `{analysis_geography_metrics_output_path()}`
- Coverage CSV: `{analysis_variable_coverage_path()}`
- Mapping doc: `{analysis_variable_mapping_path()}`
- Data-processing doc: `{analysis_data_processing_doc_path()}`

### S3 layout

- Silver filtered postcard prefix: `silver/nccs_990/postcard/`
- Silver filtered metadata prefix: `silver/nccs_990/postcard/metadata/`
- Official S3 analysis prefix: `silver/nccs_990/postcard/analysis/`
- Official S3 analysis metadata prefix: `silver/nccs_990/postcard/analysis/metadata/`

## Step-By-Step Transformation

### Step 05: benchmark filtering and retained latest-snapshot derivative

Step 05 is the key upstream curation step for postcard.

For each monthly file, it:

1. reads the monthly CSV in chunks
2. filters rows to benchmark geography before any large combined rowset is built
3. concatenates the retained benchmark-scoped rows
4. dedupes to one retained row per EIN in the latest snapshot view
5. writes the retained tax-year-window derivative consumed by the analysis step

This means the downstream analysis build never needs to reopen the broad raw monthly rowset.

### Retained row semantics

The postcard derivative is intentionally:

- benchmark-scoped
- latest-snapshot based
- one-row-per-EIN after retained dedupe

It is not a true annual filing panel. That is a central interpretive constraint for the final postcard analysis outputs.

### Step 07: analysis-year clamp and row-level analysis build

The analysis step reads the retained postcard derivative and then clamps the final analysis rows to tax years `2022-2024`.

Rows outside that tax-year window stay out of the final analysis outputs, but the latest-snapshot character of the retained derivative remains intact.

### Identity and geography extraction

The row-level analysis dataset keeps postcard identity and geography fields such as:

- `ein`
- `tax_year`
- `org_name`
- `county_fips`
- `region`
- `benchmark_match_source`
- `snapshot_month`
- `snapshot_year`

Postcard is intentionally treated as a counts-and-geography layer, not a financial-performance layer.

### Classification enrichment

`analysis_ntee_code` and `analysis_subsection_code` use the same fallback order that the repo applies to other non-BMF sources when appropriate:

1. exact-year NCCS BMF lookup by `ein + tax_year`
2. nearest-year NCCS BMF lookup by `ein`
3. IRS EO BMF lookup by `ein`

All fills preserve classification provenance fields so the final dataset records where each value came from.

### Source-backed and imputed support flags

The source-backed flag family is:

- `analysis_is_hospital`
- `analysis_is_university`
- `analysis_is_political_org`

The complete analysis-ready flag family is:

- `analysis_imputed_is_hospital`
- `analysis_imputed_is_university`
- `analysis_imputed_is_political_org`

The imputed flags use:

1. the canonical source-backed proxy
2. high-confidence name patterns
3. default `False`

Postcard also exposes direct small-filer support fields such as:

- `analysis_gross_receipts_under_25000`
- `analysis_terminated`

when they are present in the retained postcard derivative.

### Geography metrics

The postcard geography metrics parquet writes both:

- region-level rows
- county-level rows

Each record is keyed by:

- geography level
- region
- optional county FIPS
- retained tax year
- exclusion variant

Metrics include:

- filing count
- unique EIN count
- counts of imputed exclusion flags
- gross-receipts-under-25k counts
- terminated counts

### Coverage, mapping, and upload verification

The coverage CSV records fill rates for the available postcard classification and support variables by retained tax year.

Unsupported financial variables remain explicitly marked unavailable rather than being backfilled from GT or efile.

The upload step prints explicit local-vs-S3 byte comparisons for each analysis artifact and raises if any remote byte count differs from local bytes.

## Final Analysis Outputs

The official postcard analysis package consists of:

- one row-level analysis parquet
- one geography metrics parquet
- one coverage CSV
- one mapping Markdown
- this data-processing doc

This package is intended for supplemental small-filer geography, counts, and classification analysis.

## Testing And Verification

- Unit tests live in `tests/nccs_990_postcard/test_nccs_990_postcard_common.py`.
- The analysis tests cover tax-year clamping, classification fallback behavior, support-flag construction, and geography metric generation.
- Operational verification includes raw source/local/S3 byte checks, filtered-output uploads, and analysis upload byte checks.

## Current Caveats

- The postcard benchmark derivative is a latest-snapshot, one-row-per-EIN product after retained dedupe. It is not a true annual panel.
- The postcard analysis outputs are suitable for supplemental small-filer counts, geography, and classification support, not for authoritative multi-year financial trend analysis.
- Because postcard does not carry the GT/efile financial fields, revenue, expense, assets, surplus, net margin, reserves, and contribution/grant variables remain explicitly unavailable.
- The main runner currently executes sequentially rather than exposing GT-style start-step/end-step selection.

## Draft Alignment Appendix

| draft_variable | source_specific_output | status | rule_or_reason |
| --- | --- | --- | --- |
| NTEE filed classification code | analysis_ntee_code | enriched | Exact-year, nearest-year, then IRS EO BMF fallback |
| Broad NTEE field classification code | analysis_calculated_ntee_broad_code | calculated | First letter of resolved NTEE code |
| Hospital flag | analysis_is_hospital | proxy | Resolved from NTEE code |
| University flag | analysis_is_university | proxy | Resolved from NTEE code |
| Political organization flag | analysis_is_political_org | proxy | Resolved from subsection code |
| Total revenue | analysis_total_revenue_amount | unavailable | Postcard does not carry this financial-performance field |
| Total expense | analysis_total_expense_amount | unavailable | Postcard does not carry this financial-performance field |
| Net asset | analysis_net_asset_amount | unavailable | Postcard does not carry this financial-performance field |
| Program service revenue | analysis_program_service_revenue_amount | unavailable | Postcard does not carry this revenue-source field |
| Total contributions | analysis_calculated_total_contributions_amount | unavailable | Postcard does not carry this revenue-source field |
| Grants (total amount) | analysis_calculated_grants_total_amount | unavailable | Postcard does not carry this revenue-source field |
"""
    doc_path.write_text(doc.strip() + "\n", encoding="utf-8")


def build_analysis_outputs(
    *,
    metadata_dir: Path = META_DIR,
    staging_dir: Path = STAGING_DIR,
) -> dict[str, Any]:
    analysis_variables_path = analysis_variables_output_path(staging_dir)
    analysis_metrics_path = analysis_geography_metrics_output_path(staging_dir)
    coverage_path = analysis_variable_coverage_path(metadata_dir)
    mapping_path = analysis_variable_mapping_path()
    processing_doc_path = analysis_data_processing_doc_path()
    _ensure_parent_dirs(analysis_variables_path, analysis_metrics_path, coverage_path, mapping_path, processing_doc_path)

    release = resolve_release_and_write_metadata("latest", metadata_dir, snapshot_months_arg="all")
    snapshot_year = int(release["snapshot_year"])
    source_path = filtered_tax_year_window_parquet_path(staging_dir, snapshot_year, POSTCARD_TAX_YEAR_START_DEFAULT)
    if not source_path.exists():
        raise FileNotFoundError(f"Missing postcard tax-year-window derivative: {source_path}")

    source_df = pd.read_parquet(source_path)
    print(f"[analysis] Snapshot year included: {snapshot_year}", flush=True)
    print(f"[analysis] Source rows before analysis-year clamp: {len(source_df):,}", flush=True)
    source_df["tax_year"] = pd.to_numeric(source_df["tax_year"], errors="coerce").astype("Int64")
    source_df = source_df.loc[source_df["tax_year"].ge(ANALYSIS_TAX_YEAR_MIN) & source_df["tax_year"].le(ANALYSIS_TAX_YEAR_MAX)].copy()
    print(f"[analysis] Source rows after analysis-year clamp to 2022-2024: {len(source_df):,}", flush=True)

    source_df["ein"] = source_df["ein"].astype("string").map(normalize_ein9)
    source_df["tax_year"] = source_df["tax_year"].astype("string")
    source_df["legal_name"] = source_df["legal_name"].astype("string")
    source_df["county_fips"] = source_df["county_fips"].astype("string")
    source_df["region"] = source_df["region"].astype("string")
    source_df["benchmark_match_source"] = source_df["benchmark_match_source"].astype("string")
    source_df["snapshot_month"] = source_df["snapshot_month"].astype("string")
    source_df["snapshot_year"] = source_df["snapshot_year"].astype("string")

    analysis_df = pd.DataFrame(
        {
            "ein": source_df["ein"],
            "tax_year": source_df["tax_year"],
            "org_name": source_df["legal_name"],
            "county_fips": source_df["county_fips"],
            "region": source_df["region"],
            "benchmark_match_source": source_df["benchmark_match_source"],
            "snapshot_month": source_df["snapshot_month"],
            "snapshot_year": source_df["snapshot_year"],
        }
    )

    required_tax_years = sorted(source_df["tax_year"].dropna().astype("string").unique().tolist())
    print(f"[analysis] Required tax years for BMF enrichment: {required_tax_years}", flush=True)
    print(f"[analysis] Available exact-year lookup files: {discover_bmf_exact_year_lookup_inputs()}", flush=True)
    bmf_lookup_df = load_bmf_classification_lookup(required_tax_years)
    irs_lookup_df = load_irs_bmf_classification_lookup()
    classification_summary = apply_classification_fallbacks(
        analysis_df,
        source_df.rename(columns={"organization_state": "state"}),
        state_column="organization_state" if "organization_state" in source_df.columns else None,
        bmf_lookup_df=bmf_lookup_df,
        irs_lookup_df=irs_lookup_df,
    )

    analysis_df["analysis_calculated_ntee_broad_code"], analysis_df["analysis_calculated_ntee_broad_code_source_column"] = _build_broad_ntee(analysis_df["analysis_ntee_code"])
    analysis_df["analysis_is_hospital"], analysis_df["analysis_is_hospital_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("E20", "E21", "E22", "E24"))
    analysis_df["analysis_is_university"], analysis_df["analysis_is_university_source_column"] = build_ntee_proxy_flag(analysis_df["analysis_ntee_code"], ("B40", "B41", "B42", "B43", "B50"))
    analysis_df["analysis_is_political_org"], analysis_df["analysis_is_political_org_source_column"] = build_political_proxy_flag(analysis_df["analysis_subsection_code"])
    apply_imputed_flag_family(analysis_df, name_column="org_name")

    if "gross_receipts_under_25000" in source_df.columns:
        analysis_df["analysis_gross_receipts_under_25000"] = normalize_bool_text(source_df["gross_receipts_under_25000"])
        analysis_df["analysis_gross_receipts_under_25000_source_column"] = pd.Series("gross_receipts_under_25000", index=analysis_df.index, dtype="string")
        analysis_df.loc[analysis_df["analysis_gross_receipts_under_25000"].isna(), "analysis_gross_receipts_under_25000_source_column"] = pd.NA
    else:
        analysis_df["analysis_gross_receipts_under_25000"] = pd.Series(pd.NA, index=analysis_df.index, dtype="boolean")
        analysis_df["analysis_gross_receipts_under_25000_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    if "terminated" in source_df.columns:
        analysis_df["analysis_terminated"] = normalize_bool_text(source_df["terminated"])
        analysis_df["analysis_terminated_source_column"] = pd.Series("terminated", index=analysis_df.index, dtype="string")
        analysis_df.loc[analysis_df["analysis_terminated"].isna(), "analysis_terminated_source_column"] = pd.NA
    else:
        analysis_df["analysis_terminated"] = pd.Series(pd.NA, index=analysis_df.index, dtype="boolean")
        analysis_df["analysis_terminated_source_column"] = pd.Series(pd.NA, index=analysis_df.index, dtype="string")
    analysis_df["analysis_is_small_filer_support"] = pd.Series(True, index=analysis_df.index, dtype="boolean")
    analysis_df["analysis_is_small_filer_support_source_column"] = pd.Series("postcard_pipeline_scope", index=analysis_df.index, dtype="string")

    geography_metrics_df = _build_geography_metrics_output(analysis_df)
    analysis_df.sort_values(["tax_year", "region", "county_fips", "ein"], inplace=True, kind="mergesort")
    geography_metrics_df.sort_values(["geography_level", "region", "county_fips", "tax_year", "analysis_exclusion_variant"], inplace=True, kind="mergesort")
    analysis_df.to_parquet(analysis_variables_path, index=False)
    geography_metrics_df.to_parquet(analysis_metrics_path, index=False)

    final_variables = [
        "analysis_ntee_code",
        "analysis_calculated_ntee_broad_code",
        "analysis_subsection_code",
        "analysis_is_hospital",
        "analysis_is_university",
        "analysis_is_political_org",
        "analysis_imputed_is_hospital",
        "analysis_imputed_is_university",
        "analysis_imputed_is_political_org",
        "analysis_gross_receipts_under_25000",
        "analysis_terminated",
        "analysis_is_small_filer_support",
    ]
    coverage_rows: list[dict[str, Any]] = []
    metadata_by_variable = {row["canonical_variable"]: row for row in AVAILABLE_VARIABLE_METADATA}
    for variable_name in tqdm(final_variables, desc="postcard coverage variables", unit="variable"):
        metadata_row = metadata_by_variable[variable_name]
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

    print("[analysis] Snapshot months included in source:", flush=True)
    for snapshot_month in sorted(source_df["snapshot_month"].dropna().astype("string").unique().tolist()):
        print(f"[analysis]   snapshot_month={snapshot_month}", flush=True)
    print("[analysis] Counts by retained tax year:", flush=True)
    for row in analysis_df.groupby("tax_year", dropna=False).size().reset_index(name="row_count").itertuples(index=False):
        print(f"[analysis]   tax_year={row.tax_year} row_count={row.row_count:,}", flush=True)
    print(f"[analysis] Classification fallback summary: {classification_summary}", flush=True)

    return {
        "snapshot_year": snapshot_year,
        "analysis_row_count": int(len(analysis_df)),
        "geography_metric_row_count": int(len(geography_metrics_df)),
        "classification_summary": classification_summary,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build NCCS postcard analysis outputs for 2022-2024.")
    parser.add_argument("--metadata-dir", type=Path, default=META_DIR, help="Local metadata directory")
    parser.add_argument("--staging-dir", type=Path, default=STAGING_DIR, help="Local staging directory")
    parser.add_argument("--bucket", default=DEFAULT_S3_BUCKET, help="Unused here but logged to mirror pipeline conventions")
    parser.add_argument("--region", default=DEFAULT_S3_REGION, help="Unused here but logged to mirror pipeline conventions")
    parser.add_argument("--analysis-prefix", default=ANALYSIS_PREFIX, help="Logged official analysis S3 prefix")
    parser.add_argument("--analysis-meta-prefix", default=ANALYSIS_META_PREFIX, help="Logged official analysis metadata prefix")
    args = parser.parse_args()

    start = time.perf_counter()
    banner("STEP 07 - BUILD NCCS POSTCARD ANALYSIS OUTPUTS")
    load_env_from_secrets()
    print(f"[analysis] Metadata dir: {args.metadata_dir}", flush=True)
    print(f"[analysis] Staging dir: {args.staging_dir}", flush=True)
    print(f"[analysis] Official analysis prefix: {args.analysis_prefix}", flush=True)
    print(f"[analysis] Official analysis metadata prefix: {args.analysis_meta_prefix}", flush=True)
    summary = build_analysis_outputs(metadata_dir=args.metadata_dir, staging_dir=args.staging_dir)
    print(f"[analysis] Summary: {summary}", flush=True)
    print_elapsed(start, "Step 07")


if __name__ == "__main__":
    main()
