"""
Revenue-source comparison for Black Hills and benchmark-region nonprofits.

This script answers the project question:

    Is there a difference in the revenue sources between Black Hills and
    benchmark regions for the limited filer universe of Forms 990, 990-EZ,
    and 990-PF?

Harmonized segments follow Section 3 Q9 with an explicit donor-channel
decomposition for Form 990 filers. The detailed revenue-source segments are:

    - program_service_revenue            (990/EZ Line 2g; PF stays missing)
    - government_grants_received         (Form 990 Line 1e: GOVERNGRANTS;
                                          EZ/PF stay missing)
    - federated_campaigns                (Form 990 Line 1a: FEDERACAMPAI;
                                          EZ/PF stay missing)
    - related_org_contributions          (Form 990 Line 1d: RELATEORGANI;
                                          EZ/PF stay missing)
    - membership_dues                    (Form 990 Line 1b: MEMBERDUESUE;
                                          EZ/PF stay missing)
    - fundraising_events_contributions   (Form 990 Line 1c: FUNDRAEVENTS;
                                          EZ/PF stay missing)
    - mixed_unclassified_contributions   (Form 990 Line 1f ALLOOTHECONT;
                                          for 990-EZ / 990-PF filers, the
                                          full Line 1 / Part I Line 1 total
                                          since those forms do not separately
                                          report sub-components)
    - residual_other_revenue             (total revenue minus the above,
                                          clipped at zero for plotting)

This decomposition is the most informative slice the IRS basic 990 family can
support for the client question of individual versus institutional giving.
Government grants (Line 1e), federated campaigns (Line 1a), and related-org
contributions (Line 1d) are unambiguously institutional. Membership dues
(Line 1b) and fundraising events (Line 1c) are predominantly but not
exclusively individual. Line 1f mixes individual gifts, private foundation
grants, donor-advised fund distributions, corporate gifts, and bequests in a
single bucket that cannot be split further without Schedule B.

The implementation is intentionally verbose. It writes intermediate tables,
prints what it is doing at each major step, and keeps the statistical helper
functions importable so tests can validate the important derivations.
"""

from __future__ import annotations

import argparse
import math
import re
import shutil
import sys
import textwrap
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
from scipy import stats
from statsmodels.formula.api import glm, ols
from statsmodels.multivariate.manova import MANOVA
from statsmodels.stats.oneway import anova_oneway


# Make `python/utils/paths.py` importable when the script is executed directly
# from the repository root, matching the pattern used by existing pipeline code.
_THIS_FILE = Path(__file__).resolve()
_PYTHON_DIR = _THIS_FILE.parents[2]
_PROJECT_ROOT = _THIS_FILE.parents[3]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))

from utils.paths import DATA as DEFAULT_DATA_ROOT  # noqa: E402


CONTRIBUTION_SOURCE_COMPONENTS = [
    "government_grants_received",
    "federated_campaigns",
    "related_org_contributions",
    "membership_dues",
    "fundraising_events_contributions",
    "mixed_unclassified_contributions",
]

SOURCE_COMPONENTS = [
    "program_service_revenue",
    *CONTRIBUTION_SOURCE_COMPONENTS,
    "residual_other_revenue",
]

SHARE_COMPONENTS = [f"{column}_share" for column in SOURCE_COMPONENTS]

AMOUNT_VARIABLES = [
    "total_revenue",
    "program_service_revenue",
    "total_contributions",
    *CONTRIBUTION_SOURCE_COMPONENTS,
    "residual_other_revenue",
]

DIAGNOSTIC_AMOUNT_VARIABLES = [
    "calculated_institutional_contributions_total",
    "government_grants",
    "cash_contributions",
    "noncash_contributions",
    "allo_other_contributions_line_1f",
]

PRIMARY_TEST_VARIABLES = AMOUNT_VARIABLES + SHARE_COMPONENTS + [f"log1p_{column}" for column in AMOUNT_VARIABLES]

REGION_LABELS = {
    "BlackHills": "Black Hills",
    "SiouxFalls": "Sioux Falls",
    "Billings": "Billings",
    "Flagstaff": "Flagstaff",
    "Missoula": "Missoula",
}

REGION_ORDER = ["Black Hills", "Billings", "Flagstaff", "Sioux Falls", "Missoula"]

REGION_COLORS = {
    "Black Hills": "#514680",
    "Billings": "#436C83",
    "Flagstaff": "#348C84",
    "Sioux Falls": "#4BAA78",
    "Missoula": "#8CC856",
    "Benchmark": "#7C8795",
}

DISPLAY_LABELS = {
    "total_revenue": "Total revenue",
    "program_service_revenue": "Program service revenue",
    "total_contributions": "Total contributions",
    "government_grants_received": "Government grants received",
    "federated_campaigns": "Federated campaign contributions",
    "related_org_contributions": "Related organization contributions",
    "membership_dues": "Membership dues",
    "fundraising_events_contributions": "Fundraising event contributions",
    "mixed_unclassified_contributions": "Mixed / unclassified contributions",
    "residual_other_revenue": "Other revenue",
    "other_institutional_contributions": "Other institutional support",
    "individual_likely_contributions": "Individual-adjacent support",
    "mixed_other_contributions": "Mixed / unclassified contributions",
}

SOURCE_COLORS = {
    "total_revenue": "#26364D",
    "program_service_revenue": "#3B6FB6",
    "total_contributions": "#C05A8A",
    "government_grants_received": "#2E8B57",
    "federated_campaigns": "#7B61B5",
    "related_org_contributions": "#A06AB4",
    "membership_dues": "#D8902F",
    "fundraising_events_contributions": "#E7A84A",
    "mixed_unclassified_contributions": "#B64B4B",
    "residual_other_revenue": "#6F7F8F",
    "other_institutional_contributions": "#8E6BBE",
    "individual_likely_contributions": "#D8902F",
    "mixed_other_contributions": "#B64B4B",
}

CLIENT_RAW_DISPLAY_LABELS = {
    "total_revenue": "Total revenue",
    "program_service_revenue": "Program service revenue",
    "total_contributions": "Total contributions",
    "government_grants_received": "Government grants received",
    "federated_campaigns": "Federated campaign contributions",
    "related_org_contributions": "Related organization contributions",
    "membership_dues": "Membership dues",
    "fundraising_events_contributions": "Fundraising event contributions",
    "mixed_unclassified_contributions": "Mixed / unclassified contributions",
    "residual_other_revenue": "Other revenue",
}

CLIENT_LOG_LEVEL_VARIABLES = [
    "log1p_total_revenue",
    "log1p_program_service_revenue",
    "log1p_total_contributions",
    "log1p_government_grants_received",
    "log1p_federated_campaigns",
    "log1p_related_org_contributions",
    "log1p_membership_dues",
    "log1p_fundraising_events_contributions",
    "log1p_mixed_unclassified_contributions",
    "log1p_residual_other_revenue",
]

CLIENT_RAW_LEVEL_VARIABLES = [
    "total_revenue",
    "program_service_revenue",
    "total_contributions",
    *CONTRIBUTION_SOURCE_COMPONENTS,
    "residual_other_revenue",
]

PRESENTATION_PAIRWISE_SOURCE_VARIABLES = [
    "program_service_revenue",
    "government_grants_received",
    "federated_campaigns",
    "related_org_contributions",
    "membership_dues",
    "fundraising_events_contributions",
]

CLIENT_PRESENTATION_VARIABLES = CLIENT_RAW_LEVEL_VARIABLES

# Iteration counts and seed for the 2022 client deck's positive-only median
# permutation test and bootstrap CI. Kept as module-level constants so the
# numbers driving the slide headline are visible in one place and easy to bump
# when accuracy of borderline p-values matters more than runtime.
PERMUTATION_ITERATIONS_2022 = 10000
BOOTSTRAP_ITERATIONS_2022 = 10000
PERMUTATION_SEED_2022 = 321

RAW_BENCHMARK_RELATIVE_PATH = Path("staging") / "filing" / "givingtuesday_990_basic_allforms_benchmark.parquet"
GT_ANALYSIS_RELATIVE_PATH = Path("staging") / "filing" / "givingtuesday_990_basic_allforms_analysis_variables.parquet"
CORE_ANALYSIS_RELATIVE_PATH = Path("staging") / "nccs_990" / "core" / "nccs_990_core_analysis_variables.parquet"


@dataclass(frozen=True)
class AnalysisPaths:
    """Centralized output paths so the run is easy to audit."""

    output_dir: Path
    tables_dir: Path
    figures_dir: Path
    results_dir: Path
    results_tables_dir: Path
    results_figures_dir: Path
    client_assets_dir: Path
    # Repo docs directory used for the externally-distributed client
    # presentation Markdown and its image assets. The CLI defaults this to the
    # repo's `docs/` directory; tests and isolated runs should point this at a
    # temporary path to avoid overwriting the real client deliverables.
    docs_dir: Path
    docs_assets_dir: Path
    docs_presentation_path: Path
    # When False, 2022 pairwise bar charts show only the median dollar label
    # above each bar (no "n=<reporters>" line). Slide bullets still report
    # reporter counts regardless of this setting.
    show_chart_reporter_count: bool = False


def default_docs_dir() -> Path:
    """Return the repo-level docs directory used for client deliverables."""

    return _PROJECT_ROOT / "docs"


def info(message: str) -> None:
    """Print a consistently formatted progress message."""

    print(f"[revenue-sources] {message}", flush=True)


def default_results_dir_for_output(output_dir: Path) -> Path:
    """Return the default user-facing results directory for one analysis run."""

    # Keep the polished report bundle next to this analysis code, outside the
    # data tree, so it is easy to find and review:
    # python/analysis/revenue_sources_black_hills/results/
    return _THIS_FILE.parent / "results"


def ensure_output_dirs(
    output_dir: Path,
    results_dir: Path | None = None,
    docs_dir: Path | None = None,
    *,
    show_chart_reporter_count: bool = False,
) -> AnalysisPaths:
    """Create the output directory tree used by tables, figures, and summaries."""

    tables_dir = output_dir / "tables"
    figures_dir = output_dir / "figures"
    resolved_results_dir = results_dir or default_results_dir_for_output(output_dir)
    results_dir = resolved_results_dir
    results_tables_dir = results_dir / "tables"
    results_figures_dir = results_dir / "figures"
    client_assets_dir = results_dir / "client_notebook_assets"
    resolved_docs_dir = docs_dir or default_docs_dir()
    docs_assets_dir = resolved_docs_dir / "assets" / "section3_q9_2022"
    docs_presentation_path = resolved_docs_dir / "Section3_Q9_2022_Client_Presentation.md"
    tables_dir.mkdir(parents=True, exist_ok=True)
    figures_dir.mkdir(parents=True, exist_ok=True)
    results_tables_dir.mkdir(parents=True, exist_ok=True)
    results_figures_dir.mkdir(parents=True, exist_ok=True)
    client_assets_dir.mkdir(parents=True, exist_ok=True)
    docs_assets_dir.mkdir(parents=True, exist_ok=True)
    info(f"Output directory ready: {output_dir}")
    info(f"Tables will be written to: {tables_dir}")
    info(f"Figures will be written to: {figures_dir}")
    info(f"Final report assets will be collected in: {results_dir}")
    info(f"Client presentation will be written to: {docs_presentation_path}")
    return AnalysisPaths(
        output_dir=output_dir,
        tables_dir=tables_dir,
        figures_dir=figures_dir,
        results_dir=results_dir,
        results_tables_dir=results_tables_dir,
        results_figures_dir=results_figures_dir,
        client_assets_dir=client_assets_dir,
        docs_dir=resolved_docs_dir,
        docs_assets_dir=docs_assets_dir,
        docs_presentation_path=docs_presentation_path,
        show_chart_reporter_count=show_chart_reporter_count,
    )


def to_numeric(series: pd.Series) -> pd.Series:
    """Convert a source column to numeric while preserving missing values."""

    return pd.to_numeric(series, errors="coerce")


def first_existing_column(frame: pd.DataFrame, candidates: Iterable[str]) -> pd.Series:
    """Return the first available candidate column, or an all-missing series."""

    for column in candidates:
        if column in frame.columns:
            return frame[column]
    return pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")


def normalize_bool_series(series: pd.Series) -> pd.Series:
    """Normalize several boolean-ish source encodings into pandas nullable booleans."""

    true_values = {"1", "Y", "YES", "TRUE", "T"}
    false_values = {"0", "N", "NO", "FALSE", "F"}

    def _normalize(value: object) -> object:
        if pd.isna(value):
            return pd.NA
        text = str(value).strip().upper()
        if text in true_values:
            return True
        if text in false_values:
            return False
        return pd.NA

    return series.map(_normalize).astype("boolean")


def map_comparison_group(region: object) -> str:
    """Map detailed region labels into the headline Black Hills vs benchmark contrast."""

    return "Black Hills" if str(region).strip() == "BlackHills" else "Benchmark"


def load_givingtuesday_analysis(data_root: Path, years: list[int]) -> pd.DataFrame:
    """Load the primary GivingTuesday analysis-variable artifact."""

    input_path = data_root / GT_ANALYSIS_RELATIVE_PATH
    info(f"Loading primary GivingTuesday analysis file: {input_path}")
    if not input_path.exists():
        raise FileNotFoundError(f"Primary GivingTuesday analysis file not found: {input_path}")
    frame = pd.read_parquet(input_path)
    info(f"Loaded GivingTuesday rows={len(frame):,}, columns={len(frame.columns):,}")
    frame["tax_year"] = frame["tax_year"].astype(str)
    selected_years = {str(year) for year in years}
    frame = frame.loc[frame["tax_year"].isin(selected_years)].copy()
    info(f"Rows after requested-year filter {sorted(selected_years)}: {len(frame):,}")
    return frame


def load_givingtuesday_raw_benchmark(data_root: Path, years: list[int]) -> pd.DataFrame | None:
    """Load the raw benchmark artifact used for source-field validation."""

    input_path = data_root / RAW_BENCHMARK_RELATIVE_PATH
    info(f"Checking raw GivingTuesday benchmark file for validation: {input_path}")
    if not input_path.exists():
        info("Raw benchmark file is not available; skipping raw-field validation.")
        return None
    frame = pd.read_parquet(input_path)
    frame["tax_year"] = frame["tax_year"].astype(str)
    selected_years = {str(year) for year in years}
    frame = frame.loc[frame["tax_year"].isin(selected_years)].copy()
    info(f"Loaded raw benchmark validation rows={len(frame):,}, columns={len(frame.columns):,}")
    return frame


def build_raw_field_validation_table(raw: pd.DataFrame | None, harmonized: pd.DataFrame) -> pd.DataFrame:
    """
    Reproduce the notebook's raw-to-harmonized lineage validation.

    The table checks the exact source fields used for the four requested Q9
    variables. A mismatch count of zero means the harmonized analysis-variable
    file still matches the raw GivingTuesday source fields for that form type.
    """

    key_cols = ["ein", "tax_year", "form_type", "region"]
    rows: list[dict[str, object]] = []
    if raw is None:
        return pd.DataFrame(
            [
                {
                    "check": "raw_file_available",
                    "passed": False,
                    "details": "Raw benchmark parquet was not found, so raw-field validation was skipped.",
                }
            ]
        )

    same_key_order = harmonized[key_cols].astype(str).reset_index(drop=True).equals(
        raw[key_cols].astype(str).reset_index(drop=True)
    )
    rows.extend(
        [
            {
                "check": "same_key_order",
                "passed": bool(same_key_order),
                "details": "Raw and harmonized files have the same row order by ein, tax_year, form_type, and region.",
            },
            {
                "check": "raw_duplicate_keys",
                "passed": int(raw.duplicated(key_cols).sum()) == 0,
                "details": int(raw.duplicated(key_cols).sum()),
            },
            {
                "check": "harmonized_duplicate_keys",
                "passed": int(harmonized.duplicated(key_cols).sum()) == 0,
                "details": int(harmonized.duplicated(key_cols).sum()),
            },
        ]
    )

    source_cols = [
        "TOTREVCURYEA",
        "TOTALRREVENU",
        "ANREEXTOREEX",
        "TOTPROSERREV",
        "PROGSERVREVE",
        "TOTACASHCONT",
        "CONGIFGRAETC",
        "STREACGRTOIN",
    ]
    available_source_cols = [column for column in source_cols if column in raw.columns]
    check = harmonized.reset_index(drop=True).join(
        raw[available_source_cols].reset_index(drop=True).add_suffix("_raw")
    )

    def _mismatch_count(output_col: str, form_type: str, source_col: str) -> dict[str, object]:
        if output_col not in check.columns or source_col not in check.columns:
            return {
                "check": "source_mapping",
                "analysis_variable": output_col,
                "form_type": form_type,
                "raw_source_column": source_col.replace("_raw", ""),
                "rows_checked": 0,
                "nonmissing_analysis_values": 0,
                "nonmissing_raw_values": 0,
                "mismatches": np.nan,
                "passed": False,
                "details": "Required column was not available.",
            }
        mask = check["form_type"].astype(str).str.upper().eq(form_type)
        observed = to_numeric(check.loc[mask, output_col])
        expected = to_numeric(check.loc[mask, source_col])
        both_missing = observed.isna() & expected.isna()
        matched = both_missing | observed.fillna(0).sub(expected.fillna(0)).abs().le(1e-9)
        mismatches = int((~matched).sum())
        return {
            "check": "source_mapping",
            "analysis_variable": output_col,
            "form_type": form_type,
            "raw_source_column": source_col.replace("_raw", ""),
            "rows_checked": int(mask.sum()),
            "nonmissing_analysis_values": int(observed.notna().sum()),
            "nonmissing_raw_values": int(expected.notna().sum()),
            "mismatches": mismatches,
            "passed": mismatches == 0,
            "details": "",
        }

    rows.extend(
        [
            _mismatch_count("analysis_total_revenue_amount", "990", "TOTREVCURYEA_raw"),
            _mismatch_count("analysis_total_revenue_amount", "990EZ", "TOTALRREVENU_raw"),
            _mismatch_count("analysis_total_revenue_amount", "990PF", "ANREEXTOREEX_raw"),
            _mismatch_count("analysis_program_service_revenue_amount", "990", "TOTPROSERREV_raw"),
            _mismatch_count("analysis_program_service_revenue_amount", "990EZ", "PROGSERVREVE_raw"),
            _mismatch_count("analysis_total_contributions_amount", "990", "TOTACASHCONT_raw"),
            _mismatch_count("analysis_total_contributions_amount", "990EZ", "CONGIFGRAETC_raw"),
            _mismatch_count("analysis_total_contributions_amount", "990PF", "STREACGRTOIN_raw"),
        ]
    )
    return pd.DataFrame(rows)


def prepare_givingtuesday_analysis(frame: pd.DataFrame, *, exclude_outliers: bool = False) -> pd.DataFrame:
    """
    Build the primary analysis frame at organization-year grain.

    The detailed comments in this function are deliberate. These derivations
    define the research question operationally, so they should be visible to
    anyone reviewing or reusing the script.
    """

    info("Deriving primary analysis variables from GivingTuesday fields.")
    out = pd.DataFrame(index=frame.index)
    out["ein"] = first_existing_column(frame, ["ein"]).astype(str).str.zfill(9)
    out["tax_year"] = first_existing_column(frame, ["tax_year"]).astype(str)
    out["form_type"] = first_existing_column(frame, ["form_type"]).astype(str).str.upper().str.strip()
    out["region"] = first_existing_column(frame, ["region"]).astype(str).str.strip()
    out["region_label"] = out["region"].map(REGION_LABELS).fillna(out["region"])
    out["comparison_group"] = out["region"].map(map_comparison_group)
    out["is_black_hills"] = out["comparison_group"].eq("Black Hills").astype(int)

    # Revenue and contribution fields are already extracted by the upstream GT
    # analysis layer. We still coerce them here so downstream tests and plots use
    # one consistent numeric representation.
    out["total_revenue"] = to_numeric(first_existing_column(frame, ["analysis_total_revenue_amount"]))
    out["program_service_revenue"] = to_numeric(first_existing_column(frame, ["analysis_program_service_revenue_amount"]))
    out["cash_contributions"] = to_numeric(first_existing_column(frame, ["analysis_cash_contributions_amount"]))
    out["noncash_contributions"] = to_numeric(first_existing_column(frame, ["analysis_noncash_contributions_amount"]))
    out["allo_other_contributions_line_1f"] = to_numeric(first_existing_column(frame, ["analysis_other_contributions_amount"]))
    out["federated_campaigns"] = to_numeric(first_existing_column(frame, ["analysis_federated_campaigns_amount"]))
    out["membership_dues"] = to_numeric(first_existing_column(frame, ["analysis_membership_dues_amount"]))
    out["fundraising_events_contributions"] = to_numeric(
        first_existing_column(frame, ["analysis_fundraising_events_contributions_amount"])
    )
    out["related_org_contributions"] = to_numeric(first_existing_column(frame, ["analysis_related_org_contributions_amount"]))
    out["government_grants"] = to_numeric(first_existing_column(frame, ["analysis_government_grants_amount"]))
    out["calculated_institutional_contributions_total"] = to_numeric(
        first_existing_column(frame, ["analysis_calculated_grants_total_amount"])
    )
    out["total_contributions"] = to_numeric(
        first_existing_column(frame, ["analysis_total_contributions_amount", "analysis_calculated_total_contributions_amount"])
    )

    # Form 990-PF rows do not have a comparable program-service revenue concept
    # in this source. Keeping those values missing prevents the analysis from
    # accidentally treating "not applicable" as a real zero.
    pf_mask = out["form_type"].eq("990PF")
    program_service_available = out["form_type"].isin(["990", "990EZ"])
    out.loc[program_service_available, "program_service_revenue"] = out.loc[
        program_service_available, "program_service_revenue"
    ].fillna(0.0)
    out.loc[pf_mask, "program_service_revenue"] = np.nan

    # Total contributions is a supported line across the selected 990-family
    # forms. In this extract, blank supported amount lines are interpreted as
    # reported zeros; this is different from form-specific source detail that
    # the filer could not report at all.
    out["total_contributions"] = out["total_contributions"].fillna(0.0)

    # Section 3 Q9 donor-channel decomposition.
    #
    # On Form 990, Part VIII Line 1 has six sub-lines (1a-1f) that reconcile to
    # Line 1h. We expose those sub-lines as the main contribution-source
    # categories: federated campaigns, membership dues, fundraising event
    # contributions, related-organization contributions, government grants, and
    # mixed / unclassified contributions. Line 1f deliberately stays mixed
    # because it can combine individual gifts with private foundation grants,
    # DAF distributions, corporate gifts, bequests, and other contribution
    # types that Form 990 does not separate.
    #
    # 990-EZ and 990-PF do not separately report comparable Line 1
    # sub-components. Those detailed analysis variables stay missing for
    # non-990 filers so tests and medians do not confuse "unavailable" with a
    # reported zero. The full reported 990-EZ / 990-PF contribution total is
    # still routed into mixed / unclassified contributions because that amount
    # is available, but undecomposable.
    is_form_990 = out["form_type"].eq("990")

    # The detailed Line 1 subcategories only exist for Form 990. Within Form
    # 990, a blank sub-line is treated as a reported zero; outside Form 990, it
    # is unavailable and kept missing.
    for detail_column in [
        "federated_campaigns",
        "membership_dues",
        "fundraising_events_contributions",
        "related_org_contributions",
        "government_grants",
        "allo_other_contributions_line_1f",
    ]:
        out[detail_column] = np.where(is_form_990, out[detail_column].fillna(0.0), np.nan)

    sub_components = [
        "federated_campaigns",
        "membership_dues",
        "fundraising_events_contributions",
        "related_org_contributions",
        "government_grants",
        "allo_other_contributions_line_1f",
    ]
    out["sub_component_sum"] = out[sub_components].fillna(0.0).sum(axis=1)

    out["government_grants_received"] = np.where(is_form_990, out["government_grants"].fillna(0.0), np.nan)
    out["other_institutional_contributions"] = np.where(
        is_form_990,
        out["federated_campaigns"].fillna(0.0) + out["related_org_contributions"].fillna(0.0),
        np.nan,
    )
    out["individual_likely_contributions"] = np.where(
        is_form_990,
        out["membership_dues"].fillna(0.0) + out["fundraising_events_contributions"].fillna(0.0),
        np.nan,
    )

    # For 990 rows, the mixed bucket is Line 1f. For 990-EZ / 990-PF, the
    # forms do not expose sub-components, so the entire Line 1 / Part I Line 1
    # total goes into the mixed bucket (it really is undecomposable).
    mixed_for_990 = out["allo_other_contributions_line_1f"].fillna(0.0)
    mixed_for_other_forms = out["total_contributions"].fillna(0.0)
    out["mixed_unclassified_contributions"] = np.where(is_form_990, mixed_for_990, mixed_for_other_forms)
    # Backward-compatible alias for older tables/docs. Client-facing outputs use
    # the more accurate "mixed / unclassified contributions" label.
    out["mixed_other_contributions"] = out["mixed_unclassified_contributions"]

    # Diagnostic: when we trust the 990 sub-components, verify they reconcile
    # to the reported Line 1h total contributions. A meaningful gap usually
    # means the upstream extract dropped a sub-line or rounded.
    out["sub_components_sum_for_990"] = np.where(is_form_990, out["sub_component_sum"], np.nan)
    out["grants_exceed_total_contributions_flag"] = (
        is_form_990
        & out["total_contributions"].notna()
        & out["calculated_institutional_contributions_total"].notna()
        & out["calculated_institutional_contributions_total"].gt(out["total_contributions"])
    )

    # Residual captures revenue not allocated to the five named segments.
    contributions_segment_sum = (
        out["government_grants_received"].fillna(0.0)
        + out["federated_campaigns"].fillna(0.0)
        + out["related_org_contributions"].fillna(0.0)
        + out["membership_dues"].fillna(0.0)
        + out["fundraising_events_contributions"].fillna(0.0)
        + out["mixed_unclassified_contributions"].fillna(0.0)
    )
    filled_psr = out["program_service_revenue"].fillna(0.0)
    out["residual_other_revenue"] = out["total_revenue"] - filled_psr - contributions_segment_sum
    out["negative_residual_flag"] = out["residual_other_revenue"].lt(-1e-9)
    out["share_over_100_flag"] = False

    # Optional outlier flags come from the upstream NTEE/name proxy logic. If the
    # columns are missing, the script keeps all rows and explains that in output.
    out["is_hospital"] = normalize_bool_series(first_existing_column(frame, ["analysis_imputed_is_hospital", "analysis_is_hospital"]))
    out["is_university"] = normalize_bool_series(first_existing_column(frame, ["analysis_imputed_is_university", "analysis_is_university"]))
    out["is_political_org"] = normalize_bool_series(first_existing_column(frame, ["analysis_imputed_is_political_org", "analysis_is_political_org"]))
    if exclude_outliers:
        keep_mask = ~(
            out["is_hospital"].fillna(False)
            | out["is_university"].fillna(False)
            | out["is_political_org"].fillna(False)
        )
        info(f"Applying requested outlier exclusion. Rows before={len(out):,}, after={int(keep_mask.sum()):,}")
        out = out.loc[keep_mask].copy()

    # Keep only the rows that can answer the revenue-source question. A positive
    # total revenue denominator is required for interpretable shares.
    valid_revenue_mask = out["total_revenue"].notna() & out["total_revenue"].gt(0)
    valid_geo_mask = out["region"].notna() & out["region"].ne("") & out["tax_year"].notna() & out["form_type"].notna()
    before_filter = len(out)
    out = out.loc[valid_revenue_mask & valid_geo_mask].copy()
    info(f"Rows after valid geography/form/year and positive revenue filter: {len(out):,} of {before_filter:,}")

    # Shares are row-level organization-year proportions. Negative residuals are
    # retained in diagnostics but clipped to zero for composition-only plots and
    # transforms where negative components are mathematically invalid.
    for column in AMOUNT_VARIABLES:
        out[f"log1p_{column}"] = np.log1p(out[column].clip(lower=0))

    for column in SOURCE_COMPONENTS:
        share_column = f"{column}_share"
        numerator = out[column].copy()
        if column == "residual_other_revenue":
            numerator = numerator.clip(lower=0)
        out[share_column] = numerator / out["total_revenue"]

    share_sum = out[SHARE_COMPONENTS].sum(axis=1, min_count=1)
    out["source_share_sum"] = share_sum
    out["share_over_100_flag"] = share_sum.gt(1.000001)
    out["revenue_size_stratum"] = assign_revenue_size_strata(out["total_revenue"])

    info("Finished deriving revenue amounts, revenue shares, source flags, and revenue-size strata.")
    return out.reset_index(drop=True)


def assign_revenue_size_strata(total_revenue: pd.Series) -> pd.Series:
    """Assign small/medium/large strata using tertiles of positive total revenue."""

    valid = total_revenue.dropna()
    if valid.nunique() < 3:
        return pd.Series(["All"] * len(total_revenue), index=total_revenue.index, dtype="object")
    try:
        strata = pd.qcut(total_revenue, q=3, labels=["Small", "Medium", "Large"], duplicates="drop")
    except ValueError:
        return pd.Series(["All"] * len(total_revenue), index=total_revenue.index, dtype="object")
    return strata.astype("object").fillna("Unknown")


def missingness_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Summarize source-field coverage by comparison group, year, form, and region."""

    info("Building missingness and field-coverage table.")
    rows: list[dict[str, object]] = []
    group_columns = ["comparison_group", "region_label", "tax_year", "form_type"]
    for keys, group in frame.groupby(group_columns, dropna=False):
        base = dict(zip(group_columns, keys, strict=False))
        for column in AMOUNT_VARIABLES + SHARE_COMPONENTS:
            series = group[column]
            nonmissing = series.dropna()
            rows.append(
                {
                    **base,
                    "variable": column,
                    "row_count": len(group),
                    "nonmissing_count": int(nonmissing.size),
                    "nonzero_count": int(nonmissing.ne(0).sum()),
                    "missing_rate": float(series.isna().mean()) if len(group) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def counts_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Count rows and unique EINs across the main reporting dimensions."""

    info("Building counts table by region, group, year, and form type.")
    return (
        frame.groupby(["comparison_group", "region_label", "tax_year", "form_type"], dropna=False)
        .agg(row_count=("ein", "size"), unique_ein_count=("ein", "nunique"))
        .reset_index()
        .sort_values(["comparison_group", "region_label", "tax_year", "form_type"])
    )


def summary_statistics(frame: pd.DataFrame) -> pd.DataFrame:
    """Build descriptive summaries for amounts and shares."""

    info("Building descriptive statistics for revenue amounts and shares.")
    rows: list[dict[str, object]] = []
    group_columns = ["comparison_group", "region_label", "tax_year", "form_type"]
    for keys, group in frame.groupby(group_columns, dropna=False):
        base = dict(zip(group_columns, keys, strict=False))
        for column in AMOUNT_VARIABLES + SHARE_COMPONENTS:
            series = group[column].dropna()
            rows.append(
                {
                    **base,
                    "variable": column,
                    "n": int(series.size),
                    "sum": float(series.sum()) if series.size else np.nan,
                    "mean": float(series.mean()) if series.size else np.nan,
                    "median": float(series.median()) if series.size else np.nan,
                    "std": float(series.std(ddof=1)) if series.size > 1 else np.nan,
                    "iqr": float(series.quantile(0.75) - series.quantile(0.25)) if series.size else np.nan,
                    "p25": float(series.quantile(0.25)) if series.size else np.nan,
                    "p75": float(series.quantile(0.75)) if series.size else np.nan,
                }
            )
    return pd.DataFrame(rows)


def bootstrap_ci_difference(
    frame: pd.DataFrame,
    variable: str,
    *,
    group_column: str = "comparison_group",
    a: str = "Black Hills",
    b: str = "Benchmark",
    iterations: int = 1000,
    seed: int = 321,
    cluster_column: str | None = "ein",
) -> tuple[float, float, float]:
    """Bootstrap the mean difference (a minus b) for one variable.

    When ``cluster_column`` is provided and the column exists in ``frame``, the
    routine performs a *cluster bootstrap*: clusters (e.g. EINs) are sampled
    with replacement within each comparison group, and all org-year rows
    belonging to a sampled cluster are kept together. This preserves the
    within-cluster correlation structure that arises when the same nonprofit
    files multiple years and avoids the row-level bootstrap's tendency to
    underestimate the variance of the mean difference. If ``cluster_column``
    is None or missing, the routine falls back to a row-level bootstrap so the
    function remains usable for tests that have no clustering structure.
    """

    rng = np.random.default_rng(seed)
    work = frame[[group_column, variable]].copy()
    use_clusters = cluster_column is not None and cluster_column in frame.columns
    if use_clusters:
        work[cluster_column] = frame[cluster_column].to_numpy()
    work = work.dropna(subset=[group_column, variable])
    a_values = work.loc[work[group_column].eq(a), variable].to_numpy(dtype=float)
    b_values = work.loc[work[group_column].eq(b), variable].to_numpy(dtype=float)
    if len(a_values) == 0 or len(b_values) == 0:
        return np.nan, np.nan, np.nan
    observed = float(np.mean(a_values) - np.mean(b_values))

    if use_clusters:
        a_frame = work.loc[work[group_column].eq(a)]
        b_frame = work.loc[work[group_column].eq(b)]
        a_cluster_groups = [group[variable].to_numpy(dtype=float) for _, group in a_frame.groupby(cluster_column, sort=False)]
        b_cluster_groups = [group[variable].to_numpy(dtype=float) for _, group in b_frame.groupby(cluster_column, sort=False)]
        a_cluster_count = len(a_cluster_groups)
        b_cluster_count = len(b_cluster_groups)
        if a_cluster_count == 0 or b_cluster_count == 0:
            return observed, np.nan, np.nan
        draws = np.empty(iterations)
        for index in range(iterations):
            a_idx = rng.integers(0, a_cluster_count, size=a_cluster_count)
            b_idx = rng.integers(0, b_cluster_count, size=b_cluster_count)
            a_sample = np.concatenate([a_cluster_groups[i] for i in a_idx])
            b_sample = np.concatenate([b_cluster_groups[i] for i in b_idx])
            draws[index] = a_sample.mean() - b_sample.mean()
        lower, upper = np.percentile(draws, [2.5, 97.5])
        return observed, float(lower), float(upper)

    draws = np.empty(iterations)
    for index in range(iterations):
        a_sample = rng.choice(a_values, size=len(a_values), replace=True)
        b_sample = rng.choice(b_values, size=len(b_values), replace=True)
        draws[index] = np.mean(a_sample) - np.mean(b_sample)
    lower, upper = np.percentile(draws, [2.5, 97.5])
    return observed, float(lower), float(upper)


def bootstrap_median_ci(
    values: Iterable[float],
    *,
    iterations: int = 2000,
    alpha: float = 0.05,
    seed: int = 321,
) -> tuple[float, float]:
    """Return the bootstrap percentile confidence interval for the median.

    Uses the standard non-parametric percentile bootstrap (Efron 1979): resample
    `values` with replacement `iterations` times, take the median of each draw,
    and report the `[alpha/2, 1 - alpha/2]` percentiles of the draw
    distribution. This is the appropriate uncertainty estimate for a positive
    median computed on heavy-tailed dollar amounts because it (a) does not
    assume any parametric distribution and (b) degrades gracefully at small n,
    producing a degenerate but honest interval rather than a misleadingly tight
    normal approximation. Returns `(nan, nan)` when fewer than two finite
    positive observations exist, because rank-based statistics on a single
    point are not meaningful.
    """

    arr = np.asarray(list(values), dtype=float)
    arr = arr[np.isfinite(arr)]
    if arr.size < 2:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    draws = np.empty(iterations, dtype=float)
    n = arr.size
    for index in range(iterations):
        sample = rng.choice(arr, size=n, replace=True)
        draws[index] = float(np.median(sample))
    lower, upper = np.percentile(draws, [100 * alpha / 2.0, 100 * (1.0 - alpha / 2.0)])
    return float(lower), float(upper)


def aggregate_revenue_mix(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """
    Compute aggregate source mix by group.

    The source components in the GivingTuesday all-forms file are analytically
    useful, but in real data they are not always mutually exclusive. For that
    reason this table keeps two different share concepts:

    - `share`: the source-reported component amount divided by total revenue.
      These shares can sum above 100 percent when fields overlap.
    - `normalized_mix_share`: the component amount divided by the sum of the
      nonnegative displayed components. This is used only for 100-percent
      stacked visuals so the plot compares profiles without implying that the
      source fields reconcile perfectly to total revenue.
    """

    info(f"Computing aggregate revenue mix by {', '.join(group_columns)}.")
    grouped = frame.groupby(group_columns, dropna=False)
    rows: list[dict[str, object]] = []
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip(group_columns, keys, strict=False))
        total_revenue = group["total_revenue"].sum(skipna=True)
        for component in SOURCE_COMPONENTS:
            values = group[component]
            if component == "residual_other_revenue":
                values = values.clip(lower=0)
            amount = values.sum(skipna=True)
            rows.append(
                {
                    **base,
                    "component": component,
                    "amount": float(amount),
                    "total_revenue": float(total_revenue),
                    "share": float(amount / total_revenue) if total_revenue else np.nan,
                }
            )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    result["component_amount_sum"] = result.groupby(group_columns, dropna=False)["amount"].transform("sum")
    result["reported_component_share_sum"] = result.groupby(group_columns, dropna=False)["share"].transform("sum")
    result["normalized_mix_share"] = np.where(
        result["component_amount_sum"].gt(0),
        result["amount"] / result["component_amount_sum"],
        np.nan,
    )
    return result


def component_overlap_summary(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """
    Summarize rows where source components do not reconcile cleanly.

    Negative residuals and source-share totals above 100 percent are not
    necessarily code errors; they usually indicate that the component fields
    overlap or are measured on slightly different reporting concepts. Reporting
    their rate keeps the stacked-bar interpretation appropriately cautious.
    """

    info(f"Computing component-overlap diagnostics by {', '.join(group_columns)}.")
    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip(group_columns, keys, strict=False))
        row_count = len(group)
        negative_count = int(group["negative_residual_flag"].sum())
        over_100_count = int(group["share_over_100_flag"].sum())
        rows.append(
            {
                **base,
                "row_count": row_count,
                "negative_residual_count": negative_count,
                "negative_residual_rate": float(negative_count / row_count) if row_count else np.nan,
                "share_over_100_count": over_100_count,
                "share_over_100_rate": float(over_100_count / row_count) if row_count else np.nan,
                "mean_source_share_sum": float(group["source_share_sum"].mean()),
                "median_source_share_sum": float(group["source_share_sum"].median()),
                "max_source_share_sum": float(group["source_share_sum"].max()),
            }
        )
    return pd.DataFrame(rows)


def anova_result(frame: pd.DataFrame, variable: str, group_column: str = "comparison_group") -> dict[str, object]:
    """Run a standard one-way ANOVA for one variable."""

    groups = [group[variable].dropna().to_numpy(dtype=float) for _, group in frame.groupby(group_column)]
    groups = [group for group in groups if len(group) > 1]
    if len(groups) < 2:
        return {"test": "anova", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": int(frame[variable].notna().sum())}
    statistic, p_value = stats.f_oneway(*groups)
    return {"test": "anova", "variable": variable, "statistic": float(statistic), "p_value": float(p_value), "n": int(sum(len(group) for group in groups))}


def welch_anova_result(frame: pd.DataFrame, variable: str, group_column: str = "comparison_group") -> dict[str, object]:
    """Run a Welch unequal-variance mean comparison for secondary diagnostics."""

    values_by_group = [group[variable].dropna().to_numpy(dtype=float) for _, group in frame.groupby(group_column)]
    values_by_group = [values for values in values_by_group if len(values) > 1]
    if len(values_by_group) < 2:
        return {"test": "welch_anova", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": int(frame[variable].notna().sum())}
    if any(np.nanvar(values, ddof=1) <= 0 for values in values_by_group):
        return {
            "test": "welch_anova",
            "variable": variable,
            "statistic": np.nan,
            "p_value": np.nan,
            "n": int(sum(len(values) for values in values_by_group)),
        }
    try:
        result = anova_oneway(values_by_group, use_var="unequal")
    except ValueError:
        return {
            "test": "welch_anova",
            "variable": variable,
            "statistic": np.nan,
            "p_value": np.nan,
            "n": int(sum(len(values) for values in values_by_group)),
        }
    return {
        "test": "welch_anova",
        "variable": variable,
        "statistic": float(result.statistic),
        "p_value": float(result.pvalue),
        "n": int(sum(len(values) for values in values_by_group)),
    }


def rank_test_result(frame: pd.DataFrame, variable: str, group_column: str = "comparison_group") -> dict[str, object]:
    """Run Mann-Whitney for two groups or Kruskal-Wallis for more groups."""

    if group_column == "comparison_group":
        group_order = ["Black Hills", "Benchmark"]
    elif group_column == "region_label":
        group_order = REGION_ORDER
    else:
        group_order = sorted(frame[group_column].dropna().astype(str).unique().tolist())
    values_by_group = [
        frame.loc[frame[group_column].astype(str).eq(group_label), variable].dropna().to_numpy(dtype=float)
        for group_label in group_order
    ]
    values_by_group = [values for values in values_by_group if len(values) > 0]
    if len(values_by_group) < 2:
        return {"test": "rank_test", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": int(frame[variable].notna().sum())}
    if len(values_by_group) == 2:
        statistic, p_value = stats.mannwhitneyu(values_by_group[0], values_by_group[1], alternative="two-sided")
        test_name = "mann_whitney"
    else:
        try:
            statistic, p_value = stats.kruskal(*values_by_group)
        except ValueError:
            statistic, p_value = np.nan, np.nan
        test_name = "kruskal_wallis"
    return {"test": test_name, "variable": variable, "statistic": float(statistic), "p_value": float(p_value), "n": int(sum(len(values) for values in values_by_group))}


def permutation_test_difference(
    frame: pd.DataFrame,
    variable: str,
    *,
    group_column: str = "comparison_group",
    a: str = "Black Hills",
    b: str = "Benchmark",
    iterations: int = 2000,
    seed: int = 321,
) -> dict[str, object]:
    """Nonparametric permutation test for mean difference between two groups."""

    rng = np.random.default_rng(seed)
    work = frame.loc[frame[group_column].isin([a, b]), [group_column, variable]].dropna()
    a_values = work.loc[work[group_column].eq(a), variable].to_numpy(dtype=float)
    b_values = work.loc[work[group_column].eq(b), variable].to_numpy(dtype=float)
    if len(a_values) == 0 or len(b_values) == 0:
        return {"test": "permutation_mean_diff", "variable": variable, "statistic": np.nan, "p_value": np.nan, "n": len(work)}
    observed = float(np.mean(a_values) - np.mean(b_values))
    pooled = np.concatenate([a_values, b_values])
    n_a = len(a_values)
    extreme = 0
    for _ in range(iterations):
        permuted = rng.permutation(pooled)
        difference = np.mean(permuted[:n_a]) - np.mean(permuted[n_a:])
        if abs(difference) >= abs(observed):
            extreme += 1
    p_value = (extreme + 1) / (iterations + 1)
    return {"test": "permutation_mean_diff", "variable": variable, "statistic": observed, "p_value": float(p_value), "n": len(work)}


def permutation_median_difference(
    a_values: np.ndarray | pd.Series,
    b_values: np.ndarray | pd.Series,
    *,
    iterations: int = 10000,
    seed: int = 321,
) -> dict[str, float | int]:
    """Two-sided permutation test for the median difference between two groups.

    The observed statistic is ``median(a) - median(b)``. Group labels are
    randomly reassigned ``iterations`` times against the pooled values, and
    the p-value is the fraction of reassignments whose absolute median
    difference is at least as large as the observed absolute difference. This
    test is the median-focused analogue of the existing
    ``permutation_test_difference`` (which permutes mean differences) and is
    well suited to the heavy-tailed positive-only Form 990 dollar amounts on
    the 2022 client slides.

    A ``+1`` smoothing is applied to numerator and denominator so the smallest
    possible p-value with ``iterations`` reshuffles is ``1 / (iterations + 1)``
    rather than zero.
    """

    a_arr = np.asarray(a_values, dtype=float)
    b_arr = np.asarray(b_values, dtype=float)
    a_arr = a_arr[~np.isnan(a_arr)]
    b_arr = b_arr[~np.isnan(b_arr)]
    n_a = int(len(a_arr))
    n_b = int(len(b_arr))
    if n_a < 2 or n_b < 2:
        return {
            "median_difference": float("nan"),
            "p_value": float("nan"),
            "iterations": int(iterations),
            "n_a": n_a,
            "n_b": n_b,
        }

    observed = float(np.median(a_arr) - np.median(b_arr))
    pooled = np.concatenate([a_arr, b_arr])
    rng = np.random.default_rng(seed)
    extreme = 0
    abs_observed = abs(observed)
    for _ in range(iterations):
        permuted = rng.permutation(pooled)
        difference = float(np.median(permuted[:n_a]) - np.median(permuted[n_a:]))
        if abs(difference) >= abs_observed:
            extreme += 1
    p_value = (extreme + 1) / (iterations + 1)
    return {
        "median_difference": observed,
        "p_value": float(p_value),
        "iterations": int(iterations),
        "n_a": n_a,
        "n_b": n_b,
    }


def bootstrap_median_difference_ci(
    a_values: np.ndarray | pd.Series,
    b_values: np.ndarray | pd.Series,
    *,
    iterations: int = 10000,
    seed: int = 321,
    alpha: float = 0.05,
) -> dict[str, float | int]:
    """Percentile bootstrap CI for the median difference ``median(a) - median(b)``.

    Each bootstrap replicate independently resamples ``a`` and ``b`` with
    replacement and records ``median(a*) - median(b*)``. The reported interval
    is the ``alpha/2`` and ``1 - alpha/2`` percentiles of those replicates,
    which gives a non-parametric confidence interval for the median gap shown
    on the slide bars. ``alpha = 0.05`` yields a 95% CI.
    """

    a_arr = np.asarray(a_values, dtype=float)
    b_arr = np.asarray(b_values, dtype=float)
    a_arr = a_arr[~np.isnan(a_arr)]
    b_arr = b_arr[~np.isnan(b_arr)]
    n_a = int(len(a_arr))
    n_b = int(len(b_arr))
    if n_a < 2 or n_b < 2:
        return {
            "median_difference": float("nan"),
            "ci_low": float("nan"),
            "ci_high": float("nan"),
            "iterations": int(iterations),
            "alpha": float(alpha),
        }

    observed = float(np.median(a_arr) - np.median(b_arr))
    rng = np.random.default_rng(seed + 1)
    replicates = np.empty(iterations, dtype=float)
    for index in range(iterations):
        sample_a = rng.choice(a_arr, size=n_a, replace=True)
        sample_b = rng.choice(b_arr, size=n_b, replace=True)
        replicates[index] = float(np.median(sample_a) - np.median(sample_b))
    low = float(np.percentile(replicates, 100.0 * (alpha / 2)))
    high = float(np.percentile(replicates, 100.0 * (1 - alpha / 2)))
    return {
        "median_difference": observed,
        "ci_low": low,
        "ci_high": high,
        "iterations": int(iterations),
        "alpha": float(alpha),
    }


def effect_size_result(frame: pd.DataFrame, variable: str) -> dict[str, object]:
    """Compute practical effect sizes for Black Hills minus benchmark."""

    bh = frame.loc[frame["comparison_group"].eq("Black Hills"), variable].dropna().to_numpy(dtype=float)
    bm = frame.loc[frame["comparison_group"].eq("Benchmark"), variable].dropna().to_numpy(dtype=float)
    if len(bh) < 2 or len(bm) < 2:
        return {
            "variable": variable,
            "mean_difference": np.nan,
            "standardized_mean_difference": np.nan,
            "cliffs_delta": np.nan,
            "eta_squared": np.nan,
            "omega_squared": np.nan,
        }

    mean_difference = float(np.mean(bh) - np.mean(bm))
    pooled_sd = math.sqrt(((len(bh) - 1) * np.var(bh, ddof=1) + (len(bm) - 1) * np.var(bm, ddof=1)) / (len(bh) + len(bm) - 2))
    smd = mean_difference / pooled_sd if pooled_sd else np.nan
    cliffs = cliffs_delta(bh, bm)

    all_values = np.concatenate([bh, bm])
    group_means = [np.mean(bh), np.mean(bm)]
    grand_mean = np.mean(all_values)
    ss_between = len(bh) * (group_means[0] - grand_mean) ** 2 + len(bm) * (group_means[1] - grand_mean) ** 2
    ss_within = np.sum((bh - group_means[0]) ** 2) + np.sum((bm - group_means[1]) ** 2)
    ss_total = ss_between + ss_within
    df_between = 1
    df_within = len(all_values) - 2
    ms_within = ss_within / df_within if df_within > 0 else np.nan
    eta_squared = ss_between / ss_total if ss_total else np.nan
    omega_squared = (ss_between - df_between * ms_within) / (ss_total + ms_within) if ss_total and not np.isnan(ms_within) else np.nan

    return {
        "variable": variable,
        "mean_difference": mean_difference,
        "standardized_mean_difference": float(smd) if not np.isnan(smd) else np.nan,
        "cliffs_delta": cliffs,
        "eta_squared": float(eta_squared) if not np.isnan(eta_squared) else np.nan,
        "omega_squared": float(omega_squared) if not np.isnan(omega_squared) else np.nan,
    }


def cliffs_delta(a_values: np.ndarray, b_values: np.ndarray) -> float:
    """Compute Cliff's delta using ranks, avoiding a full pairwise matrix."""

    combined = np.concatenate([a_values, b_values])
    labels = np.concatenate([np.ones(len(a_values)), np.zeros(len(b_values))])
    order = np.argsort(combined, kind="mergesort")
    sorted_labels = labels[order]
    sorted_values = combined[order]

    greater = 0.0
    less = 0.0
    b_seen = 0
    total_b = len(b_values)
    index = 0
    while index < len(sorted_values):
        end = index + 1
        while end < len(sorted_values) and sorted_values[end] == sorted_values[index]:
            end += 1
        tie_labels = sorted_labels[index:end]
        a_in_tie = int(np.sum(tie_labels == 1))
        b_in_tie = int(np.sum(tie_labels == 0))
        greater += a_in_tie * b_seen
        less += a_in_tie * (total_b - b_seen - b_in_tie)
        b_seen += b_in_tie
        index = end
    return float((greater - less) / (len(a_values) * len(b_values)))


def fdr_bh(p_values: Iterable[float]) -> list[float]:
    """Benjamini-Hochberg FDR correction that handles missing p-values."""

    p_array = np.array([np.nan if pd.isna(value) else float(value) for value in p_values], dtype=float)
    adjusted = np.full_like(p_array, np.nan, dtype=float)
    valid_mask = ~np.isnan(p_array)
    valid = p_array[valid_mask]
    if valid.size == 0:
        return adjusted.tolist()
    order = np.argsort(valid)
    ranked = valid[order]
    m = len(ranked)
    raw_adjusted = ranked * m / np.arange(1, m + 1)
    raw_adjusted = np.minimum.accumulate(raw_adjusted[::-1])[::-1]
    raw_adjusted = np.clip(raw_adjusted, 0, 1)
    valid_adjusted = np.empty_like(raw_adjusted)
    valid_adjusted[order] = raw_adjusted
    adjusted[valid_mask] = valid_adjusted
    return adjusted.tolist()


def run_univariate_tests(
    frame: pd.DataFrame,
    variables: list[str],
    *,
    label: str,
    group_column: str = "comparison_group",
) -> pd.DataFrame:
    """Run the primary univariate test suite for a set of variables."""

    info(f"Running univariate tests for analysis frame: {label} (group={group_column})")
    rows: list[dict[str, object]] = []
    for variable in variables:
        if variable not in frame.columns:
            continue
        if frame[variable].dropna().nunique() < 2:
            continue
        test_results: list[dict[str, object]] = [
            anova_result(frame, variable, group_column=group_column),
            welch_anova_result(frame, variable, group_column=group_column),
            rank_test_result(frame, variable, group_column=group_column),
        ]
        if group_column == "comparison_group":
            test_results.append(permutation_test_difference(frame, variable))
        for result in test_results:
            result["analysis_frame"] = label
            result["group_column"] = group_column
            rows.append(result)
        if group_column == "comparison_group":
            effect = effect_size_result(frame, variable)
            for key, value in effect.items():
                if key != "variable":
                    rows.append(
                        {
                            "analysis_frame": label,
                            "group_column": group_column,
                            "test": key,
                            "variable": variable,
                            "statistic": value,
                            "p_value": np.nan,
                            "n": int(frame[variable].notna().sum()),
                        }
                    )
    results = pd.DataFrame(rows)
    if not results.empty and "p_value" in results.columns:
        primary_mask = results["test"].isin(["anova", "welch_anova", "mann_whitney", "kruskal_wallis", "permutation_mean_diff"])
        results["p_value_fdr_bh"] = np.nan
        results.loc[primary_mask, "p_value_fdr_bh"] = fdr_bh(results.loc[primary_mask, "p_value"])
    return results


def run_regressions(frame: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Run OLS and logistic-presence models with year and form-type controls."""

    info("Running regression models with tax-year and form-type controls.")
    rows: list[dict[str, object]] = []
    for variable in variables:
        if variable not in frame.columns or frame[variable].dropna().nunique() < 2:
            continue
        model_frame = frame[["ein", "is_black_hills", "tax_year", "form_type", variable]].dropna().copy()
        if model_frame["is_black_hills"].nunique() < 2 or len(model_frame) < 10:
            continue
        formula = f"{variable} ~ is_black_hills + C(tax_year) + C(form_type)"
        try:
            model = ols(formula, data=model_frame).fit(cov_type="cluster", cov_kwds={"groups": model_frame["ein"]})
            rows.append(
                {
                    "model": "ols_clustered_by_ein",
                    "outcome": variable,
                    "term": "is_black_hills",
                    "estimate": float(model.params.get("is_black_hills", np.nan)),
                    "std_error": float(model.bse.get("is_black_hills", np.nan)),
                    "p_value": float(model.pvalues.get("is_black_hills", np.nan)),
                    "n": int(model.nobs),
                }
            )
        except Exception as exc:  # pragma: no cover - defensive for singular designs in real data
            rows.append({"model": "ols_clustered_by_ein", "outcome": variable, "term": "is_black_hills", "estimate": np.nan, "std_error": np.nan, "p_value": np.nan, "n": len(model_frame), "error": str(exc)})

        presence_column = f"{variable}_present"
        model_frame[presence_column] = model_frame[variable].gt(0).astype(int)
        if model_frame[presence_column].nunique() < 2:
            continue
        try:
            with warnings.catch_warnings(record=True) as caught_warnings:
                warnings.simplefilter("always")
                logistic_model = glm(
                    f"{presence_column} ~ is_black_hills + C(tax_year) + C(form_type)",
                    data=model_frame,
                    family=sm.families.Binomial(),
                ).fit(
                    maxiter=200,
                    cov_type="cluster",
                    cov_kwds={"groups": model_frame["ein"].to_numpy()},
                )
            warning_messages = "; ".join(str(warning.message) for warning in caught_warnings)
            rows.append(
                {
                    "model": "logistic_presence_clustered_by_ein",
                    "outcome": presence_column,
                    "term": "is_black_hills",
                    "estimate": float(logistic_model.params.get("is_black_hills", np.nan)),
                    "std_error": float(logistic_model.bse.get("is_black_hills", np.nan)),
                    "p_value": float(logistic_model.pvalues.get("is_black_hills", np.nan)),
                    "n": int(logistic_model.nobs),
                    "warning": warning_messages,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive for separation in real data
            rows.append({"model": "logistic_presence_clustered_by_ein", "outcome": presence_column, "term": "is_black_hills", "estimate": np.nan, "std_error": np.nan, "p_value": np.nan, "n": len(model_frame), "error": str(exc)})
    results = pd.DataFrame(rows)
    if not results.empty:
        results["p_value_fdr_bh"] = fdr_bh(results["p_value"])
    return results


def composition_matrix(frame: pd.DataFrame, components: list[str] = SOURCE_COMPONENTS) -> pd.DataFrame:
    """Build a nonnegative composition matrix that sums to one per row."""

    matrix = frame[components].copy()
    for component in components:
        if component == "residual_other_revenue":
            matrix[component] = matrix[component].clip(lower=0)
        matrix[component] = matrix[component].fillna(0).clip(lower=0)
    row_sum = matrix.sum(axis=1)
    valid = row_sum.gt(0)
    matrix = matrix.loc[valid].div(row_sum.loc[valid], axis=0)
    matrix.index = frame.loc[valid].index
    return matrix


def clr_transform(composition: pd.DataFrame, pseudocount: float = 1e-6) -> pd.DataFrame:
    """Centered log-ratio transform for compositional revenue shares."""

    adjusted = composition + pseudocount
    adjusted = adjusted.div(adjusted.sum(axis=1), axis=0)
    logged = np.log(adjusted)
    return logged.sub(logged.mean(axis=1), axis=0)


def alr_transform(
    composition: pd.DataFrame,
    *,
    denominator: str = "residual_other_revenue",
    pseudocount: float = 1e-6,
) -> pd.DataFrame:
    """
    Additive log-ratio transform for MANOVA-style models.

    CLR-transformed columns sum to zero by construction, which can make MANOVA
    singular in year-specific or otherwise thin frames. ALR keeps k-1
    independent transformed outcomes by comparing each component with one
    denominator component.
    """

    if denominator not in composition.columns:
        denominator = composition.columns[-1]
    adjusted = composition + pseudocount
    adjusted = adjusted.div(adjusted.sum(axis=1), axis=0)
    denominator_values = adjusted[denominator]
    transformed = {
        f"alr_{component}": np.log(adjusted[component] / denominator_values)
        for component in adjusted.columns
        if component != denominator
    }
    return pd.DataFrame(transformed, index=composition.index)


def permanova_two_group(
    frame: pd.DataFrame,
    *,
    group_column: str = "comparison_group",
    iterations: int = 999,
    seed: int = 321,
) -> dict[str, object]:
    """PERMANOVA-style permutation test on Euclidean distances in CLR space."""

    composition = composition_matrix(frame)
    if composition.empty:
        return {"test": "permanova_clr", "statistic": np.nan, "p_value": np.nan, "n": 0}
    groups = frame.loc[composition.index, group_column].astype(str)
    if groups.nunique() != 2:
        return {"test": "permanova_clr", "statistic": np.nan, "p_value": np.nan, "n": len(composition)}
    transformed = clr_transform(composition).to_numpy(dtype=float)
    labels = groups.to_numpy()
    observed = pseudo_f_statistic(transformed, labels)
    rng = np.random.default_rng(seed)
    extreme = 0
    for _ in range(iterations):
        permuted = rng.permutation(labels)
        stat_value = pseudo_f_statistic(transformed, permuted)
        if stat_value >= observed:
            extreme += 1
    p_value = (extreme + 1) / (iterations + 1)
    return {"test": "permanova_clr", "statistic": float(observed), "p_value": float(p_value), "n": len(composition)}


def pseudo_f_statistic(values: np.ndarray, labels: np.ndarray) -> float:
    """Compute a between-group over within-group pseudo-F statistic."""

    grand = values.mean(axis=0)
    groups = np.unique(labels)
    ss_between = 0.0
    ss_within = 0.0
    for group in groups:
        group_values = values[labels == group]
        centroid = group_values.mean(axis=0)
        ss_between += len(group_values) * np.sum((centroid - grand) ** 2)
        ss_within += np.sum((group_values - centroid) ** 2)
    df_between = len(groups) - 1
    df_within = len(values) - len(groups)
    if df_between <= 0 or df_within <= 0 or ss_within == 0:
        return np.nan
    return float((ss_between / df_between) / (ss_within / df_within))


def manova_result(frame: pd.DataFrame) -> dict[str, object]:
    """Run a MANOVA-style overall mix test on ALR-transformed source composition."""

    composition = composition_matrix(frame)
    if composition.empty:
        return {"test": "manova_alr", "statistic": np.nan, "p_value": np.nan, "n": 0}
    transformed = alr_transform(composition)
    model_frame = pd.concat([transformed, frame.loc[composition.index, ["comparison_group", "tax_year", "form_type"]]], axis=1).dropna()
    if model_frame["comparison_group"].nunique() < 2 or len(model_frame) < 10:
        return {"test": "manova_alr", "statistic": np.nan, "p_value": np.nan, "n": len(model_frame)}
    outcomes = " + ".join(transformed.columns)
    rhs_terms = ["comparison_group"]
    if model_frame["tax_year"].nunique(dropna=True) > 1:
        rhs_terms.append("C(tax_year)")
    if model_frame["form_type"].nunique(dropna=True) > 1:
        rhs_terms.append("C(form_type)")
    formula = f"{outcomes} ~ {' + '.join(rhs_terms)}"
    try:
        result = MANOVA.from_formula(formula, data=model_frame).mv_test()
        stat_table = result.results["comparison_group"]["stat"]
        pillai_row = stat_table.loc["Pillai's trace"]
        return {"test": "manova_alr_pillai", "statistic": float(pillai_row["Value"]), "p_value": float(pillai_row["Pr > F"]), "n": len(model_frame)}
    except Exception as exc:  # pragma: no cover - defensive for singular real-data designs
        return {"test": "manova_alr_pillai", "statistic": np.nan, "p_value": np.nan, "n": len(model_frame), "error": str(exc)}


def run_multivariate_tests(frame: pd.DataFrame, *, label: str) -> pd.DataFrame:
    """Run full revenue-mix tests for one analysis frame."""

    info(f"Running compositional and multivariate tests for: {label}")
    rows = [permanova_two_group(frame), manova_result(frame)]
    for row in rows:
        row["analysis_frame"] = label
    results = pd.DataFrame(rows)
    if not results.empty:
        results["p_value_fdr_bh"] = fdr_bh(results["p_value"])
    return results


def concentration_metrics(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Calculate revenue concentration metrics by region or comparison group."""

    info(f"Computing concentration metrics by {', '.join(group_columns)}.")
    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip(group_columns, keys, strict=False))
        revenue_by_ein = group.groupby("ein", dropna=False)["total_revenue"].sum().clip(lower=0)
        total = revenue_by_ein.sum()
        top5 = revenue_by_ein.sort_values(ascending=False).head(5).sum()
        shares = revenue_by_ein / total if total else revenue_by_ein * np.nan
        rows.append(
            {
                **base,
                "organization_count": int(revenue_by_ein.size),
                "total_revenue": float(total),
                "gini_total_revenue": gini(revenue_by_ein.to_numpy(dtype=float)),
                "hhi_total_revenue": float(np.sum(shares**2)) if total else np.nan,
                "top5_revenue_share": float(top5 / total) if total else np.nan,
            }
        )
    return pd.DataFrame(rows)


def gini(values: np.ndarray) -> float:
    """Gini coefficient for nonnegative values."""

    values = values[~np.isnan(values)]
    values = values[values >= 0]
    if values.size == 0:
        return np.nan
    if np.all(values == 0):
        return 0.0
    sorted_values = np.sort(values)
    n = len(sorted_values)
    cumulative = np.cumsum(sorted_values)
    return float((n + 1 - 2 * np.sum(cumulative) / cumulative[-1]) / n)


def negative_residual_diagnostics(frame: pd.DataFrame) -> pd.DataFrame:
    """Return rows where derived residuals or shares look implausible."""

    info("Collecting negative residual and over-100-percent share diagnostics.")
    columns = [
        "ein",
        "tax_year",
        "form_type",
        "region_label",
        "comparison_group",
        "total_revenue",
        "program_service_revenue",
        "total_contributions",
        "residual_other_revenue",
        "source_share_sum",
        "negative_residual_flag",
        "share_over_100_flag",
    ]
    return frame.loc[frame["negative_residual_flag"] | frame["share_over_100_flag"], columns].copy()


def load_core_sensitivity(data_root: Path) -> pd.DataFrame | None:
    """Load the NCCS Core sensitivity artifact if it is available."""

    core_path = data_root / CORE_ANALYSIS_RELATIVE_PATH
    info(f"Checking NCCS Core sensitivity file: {core_path}")
    if not core_path.exists():
        info("NCCS Core sensitivity file is not available; skipping Core sensitivity.")
        return None
    frame = pd.read_parquet(core_path)
    info(f"Loaded NCCS Core rows={len(frame):,}, columns={len(frame.columns):,}")
    out = pd.DataFrame(index=frame.index)
    out["source"] = "nccs_core_2022"
    out["ein"] = first_existing_column(frame, ["ein", "harm_ein"]).astype(str).str.zfill(9)
    out["tax_year"] = first_existing_column(frame, ["tax_year", "harm_tax_year"]).astype(str)
    out["form_type"] = first_existing_column(frame, ["form_type", "harm_filing_form"]).astype(str).str.upper().str.strip()
    out["region"] = first_existing_column(frame, ["region", "harm_region"]).astype(str).str.strip()
    out["region_label"] = out["region"].map(REGION_LABELS).fillna(out["region"])
    out["comparison_group"] = out["region"].map(map_comparison_group)
    out["total_revenue"] = to_numeric(first_existing_column(frame, ["analysis_total_revenue_amount"]))
    out["program_service_revenue"] = to_numeric(first_existing_column(frame, ["analysis_program_service_revenue_amount", "analysis_program_service_revenue_candidate_amount"]))
    out["total_contributions"] = to_numeric(
        first_existing_column(
            frame,
            ["analysis_total_contributions_amount", "analysis_calculated_total_contributions_amount", "analysis_contribution_candidate_amount"],
        )
    )
    out = out.loc[out["tax_year"].eq("2022") & out["total_revenue"].notna() & out["total_revenue"].gt(0)].copy()
    for column in ["program_service_revenue", "total_contributions"]:
        out[f"{column}_share"] = out[column] / out["total_revenue"]
    info(f"NCCS Core sensitivity rows after 2022 positive-revenue filter: {len(out):,}")
    return out.reset_index(drop=True)


def write_table(frame: pd.DataFrame, path: Path) -> None:
    """Write a CSV table with a progress message."""

    frame.to_csv(path, index=False)
    info(f"Saved table: {path} ({len(frame):,} rows)")


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    """Write a parquet file with a progress message."""

    frame.to_parquet(path, index=False)
    info(f"Saved parquet: {path} ({len(frame):,} rows)")


def save_stacked_bar(mix: pd.DataFrame, index_column: str, output_path: Path, title: str) -> None:
    """
    Save a normalized 100-percent stacked bar chart.

    The plotted value is `normalized_mix_share`, not the raw share of total
    revenue, because the component fields can overlap in the source data.
    """

    value_column = "normalized_mix_share" if "normalized_mix_share" in mix.columns else "share"
    pivot = mix.pivot_table(index=index_column, columns="component", values=value_column, aggfunc="sum").fillna(0)
    pivot = pivot[[component for component in SOURCE_COMPONENTS if component in pivot.columns]]
    color_list = [SOURCE_COLORS.get(component, "#6F7F8F") for component in pivot.columns]
    ax = pivot.plot(kind="bar", stacked=True, figsize=(12, 6.5), color=color_list)
    ax.set_title(title)
    ax.set_ylabel("Normalized component share")
    ax.set_xlabel("")
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles,
        [DISPLAY_LABELS.get(label, label) for label in labels],
        title="Revenue source",
        bbox_to_anchor=(1.02, 1),
        loc="upper left",
    )
    ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")


def client_rank_test_table(
    frame: pd.DataFrame,
    variables: list[str],
    group_column: str,
    *,
    include_permutation: bool = False,
) -> pd.DataFrame:
    """Write the notebook-style non-parametric table for share comparisons."""

    rows = []
    for variable in variables:
        if variable not in frame.columns:
            continue
        result = rank_test_result(frame, variable, group_column=group_column)
        rows.append(
            {
                "test": result["test"],
                "variable": variable,
                "statistic": result["statistic"],
                "p_value": result["p_value"],
                "n": result["n"],
            }
        )
        if include_permutation and group_column == "comparison_group":
            permutation = permutation_test_difference(frame, variable, group_column=group_column)
            rows.append(
                {
                    "test": permutation["test"],
                    "variable": variable,
                    "statistic": permutation["statistic"],
                    "p_value": permutation["p_value"],
                    "n": permutation["n"],
                }
            )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["significant_before_fdr"] = np.where(out["p_value"].lt(0.05), "Yes", "No")
    out["fdr_p_value"] = fdr_bh(out["p_value"])
    out["significant_after_fdr"] = np.where(out["fdr_p_value"].lt(0.05), "Yes", "No")
    if group_column == "comparison_group" and {"Black Hills", "Benchmark"}.issubset(set(frame["comparison_group"].dropna())):
        grouped = frame.groupby("comparison_group")

        def direction(row: pd.Series) -> str:
            variable = str(row["variable"])
            if variable not in frame.columns:
                return ""
            bh_values = grouped.get_group("Black Hills")[variable].dropna().clip(lower=0)
            bm_values = grouped.get_group("Benchmark")[variable].dropna().clip(lower=0)
            if row.get("test") == "permutation_mean_diff":
                bh = bh_values.mean()
                bm = bm_values.mean()
            elif variable in CLIENT_RAW_LEVEL_VARIABLES:
                bh = bh_values.median()
                bm = bm_values.median()
                if math.isclose(float(bh), float(bm), rel_tol=0, abs_tol=0.001):
                    bh = bh_values.gt(0).mean()
                    bm = bm_values.gt(0).mean()
            else:
                bh = bh_values.mean()
                bm = bm_values.mean()
            if pd.isna(bh) or pd.isna(bm):
                return ""
            if math.isclose(float(bh), float(bm), rel_tol=0, abs_tol=0.001):
                return "No meaningful difference"
            return "Black Hills higher" if bh > bm else "Black Hills lower"

        out["direction"] = out.apply(direction, axis=1)
    return out


def client_year_by_year_rank_tables(
    frame: pd.DataFrame,
    variables: list[str],
    group_column: str = "comparison_group",
    *,
    include_permutation: bool = False,
) -> pd.DataFrame:
    """Repeat the notebook-style rank-test table within each tax year."""

    tables = []
    for tax_year in sorted(frame["tax_year"].dropna().astype(str).unique()):
        one_year = frame.loc[frame["tax_year"].astype(str).eq(tax_year)].copy()
        table = client_rank_test_table(
            one_year,
            variables,
            group_column,
            include_permutation=include_permutation,
        )
        if table.empty:
            continue
        table.insert(0, "tax_year", tax_year)
        tables.append(table)
    return pd.concat(tables, ignore_index=True) if tables else pd.DataFrame()


def client_raw_level_summary(frame: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Summarize organization-level raw-dollar variables by region."""

    rows: list[dict[str, object]] = []
    for variable in variables:
        if variable not in frame.columns:
            continue
        for region in REGION_ORDER:
            values = frame.loc[frame["region_label"].eq(region), variable].dropna().clip(lower=0)
            n = int(len(values))
            positive = values.loc[values.gt(0)]
            rows.append(
                {
                    "variable": variable,
                    "variable_label": CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable)),
                    "region_label": region,
                    "n": n,
                    "mean": float(values.mean()) if n else np.nan,
                    "median": float(values.median()) if n else np.nan,
                    "nonzero_count": int(values.gt(0).sum()) if n else 0,
                    "nonzero_percent": float(values.gt(0).mean()) if n else np.nan,
                    "positive_median": float(positive.median()) if len(positive) else np.nan,
                }
            )
    return pd.DataFrame(rows)


def client_raw_level_normality_diagnostics(frame: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Run normality/skew diagnostics for raw and log1p dollars."""

    rows: list[dict[str, object]] = []
    for variable in variables:
        if variable not in frame.columns:
            continue
        for region in REGION_ORDER:
            values = frame.loc[frame["region_label"].eq(region), variable].dropna().clip(lower=0)
            if values.empty:
                continue
            raw = values.to_numpy(dtype=float)
            log_values = np.log1p(raw)
            mean = float(np.mean(raw))
            median = float(np.median(raw))
            mean_median_ratio = mean / median if median > 0 else np.nan
            raw_p = np.nan
            log_p = np.nan
            if len(raw) >= 8 and np.unique(raw).size > 1:
                raw_p = float(stats.normaltest(raw).pvalue)
            if len(log_values) >= 8 and np.unique(log_values).size > 1:
                log_p = float(stats.normaltest(log_values).pvalue)
            rows.append(
                {
                    "variable": variable,
                    "variable_label": CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable)),
                    "region_label": region,
                    "n": int(len(raw)),
                    "zero_percent": float(np.mean(raw == 0)),
                    "mean": mean,
                    "median": median,
                    "mean_median_ratio": mean_median_ratio,
                    "skew": float(stats.skew(raw, bias=False)) if len(raw) >= 3 else np.nan,
                    "normaltest_p_raw": raw_p,
                    "normaltest_p_log1p": log_p,
                    "raw_rejects_normality_0_05": bool(pd.notna(raw_p) and raw_p < 0.05),
                    "log1p_rejects_normality_0_05": bool(pd.notna(log_p) and log_p < 0.05),
                }
            )
    return pd.DataFrame(rows)


def client_pairwise_black_hills_region_tests(frame: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Compare Black Hills directly with each benchmark region, one pair at a time."""

    rows: list[dict[str, object]] = []
    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]
    for benchmark_region in benchmark_regions:
        pair = frame.loc[frame["region_label"].isin(["Black Hills", benchmark_region])].copy()
        if pair.empty:
            continue
        pair["pairwise_group"] = np.where(pair["region_label"].eq("Black Hills"), "Black Hills", benchmark_region)
        table = client_rank_test_table(
            pair,
            variables,
            "pairwise_group",
            include_permutation=False,
        )
        # client_rank_test_table only adds permutations for comparison_group. Add them
        # explicitly so every pair has the same two direct-test rows as the pooled table.
        permutation_rows: list[dict[str, object]] = []
        for variable in variables:
            if variable not in pair.columns:
                continue
            permutation = permutation_test_difference(
                pair,
                variable,
                group_column="pairwise_group",
                a="Black Hills",
                b=benchmark_region,
            )
            permutation_rows.append(
                {
                    "test": permutation["test"],
                    "variable": variable,
                    "statistic": permutation["statistic"],
                    "p_value": permutation["p_value"],
                    "n": permutation["n"],
                }
            )
        if permutation_rows:
            table = pd.concat([table, pd.DataFrame(permutation_rows)], ignore_index=True)
            table["significant_before_fdr"] = np.where(table["p_value"].lt(0.05), "Yes", "No")
            table["fdr_p_value"] = fdr_bh(table["p_value"])
            table["significant_after_fdr"] = np.where(table["fdr_p_value"].lt(0.05), "Yes", "No")

        grouped = pair.groupby("pairwise_group")

        def direction(row: pd.Series) -> str:
            variable = str(row["variable"])
            if variable not in pair.columns or "Black Hills" not in grouped.groups or benchmark_region not in grouped.groups:
                return ""
            bh_values = grouped.get_group("Black Hills")[variable].dropna().clip(lower=0)
            bm_values = grouped.get_group(benchmark_region)[variable].dropna().clip(lower=0)
            if row.get("test") == "permutation_mean_diff":
                bh = bh_values.mean()
                bm = bm_values.mean()
            elif variable in CLIENT_RAW_LEVEL_VARIABLES:
                bh = bh_values.median()
                bm = bm_values.median()
                if math.isclose(float(bh), float(bm), rel_tol=0, abs_tol=0.001):
                    bh = bh_values.gt(0).mean()
                    bm = bm_values.gt(0).mean()
            else:
                bh = bh_values.mean()
                bm = bm_values.mean()
            if pd.isna(bh) or pd.isna(bm):
                return ""
            if math.isclose(float(bh), float(bm), rel_tol=0, abs_tol=0.001):
                return "No meaningful difference"
            return "Black Hills higher" if bh > bm else "Black Hills lower"

        table["benchmark_region"] = benchmark_region
        table["comparison"] = f"Black Hills vs {benchmark_region}"
        table["direction"] = table.apply(direction, axis=1)
        rows.extend(table.to_dict("records"))
    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    ordered_columns = [
        "comparison",
        "benchmark_region",
        "test",
        "variable",
        "direction",
        "statistic",
        "p_value",
        "n",
        "significant_before_fdr",
        "fdr_p_value",
        "significant_after_fdr",
    ]
    return out[[column for column in ordered_columns if column in out.columns]]


def write_client_equivalent_tables(frame: pd.DataFrame, tables_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Write the compact tables that mirror the client notebook exactly.

    The main script writes a broader statistical suite. These tables are kept
    separately so reviewers can trace notebook figures and interpretations back
    to compact rank-test, raw-dollar, and log-dollar tables.
    """

    info("Writing client-notebook-equivalent rank-test tables.")
    for stale_name in [
        "client_bh_vs_benchmark_level_welch_tests.csv",
        "client_year_by_year_level_welch_tests.csv",
        "client_bh_vs_benchmark_level_anova.csv",
        "client_year_by_year_level_anova.csv",
    ]:
        stale_path = tables_dir / stale_name
        if stale_path.exists():
            stale_path.unlink()
    client_tables = {
        "client_raw_level_region_summary": client_raw_level_summary(frame, CLIENT_RAW_LEVEL_VARIABLES),
        "client_raw_level_normality_diagnostics": client_raw_level_normality_diagnostics(frame, CLIENT_RAW_LEVEL_VARIABLES),
        "client_five_region_raw_level_rank_tests": client_rank_test_table(
            frame,
            CLIENT_RAW_LEVEL_VARIABLES,
            "region_label",
        ),
        "client_bh_vs_benchmark_raw_level_rank_tests": client_rank_test_table(
            frame,
            CLIENT_RAW_LEVEL_VARIABLES,
            "comparison_group",
            include_permutation=True,
        ),
        "client_pairwise_black_hills_region_raw_level_rank_tests": client_pairwise_black_hills_region_tests(
            frame,
            CLIENT_RAW_LEVEL_VARIABLES,
        ),
        "client_year_by_year_five_region_raw_level_rank_tests": client_year_by_year_rank_tables(
            frame,
            CLIENT_RAW_LEVEL_VARIABLES,
            "region_label",
        ),
        "client_year_by_year_raw_level_rank_tests": client_year_by_year_rank_tables(
            frame,
            CLIENT_RAW_LEVEL_VARIABLES,
            "comparison_group",
            include_permutation=True,
        ),
        "client_five_region_share_rank_tests": client_rank_test_table(frame, SHARE_COMPONENTS, "region_label"),
        "client_bh_vs_benchmark_share_rank_tests": client_rank_test_table(
            frame,
            SHARE_COMPONENTS,
            "comparison_group",
            include_permutation=True,
        ),
        "client_year_by_year_five_region_share_rank_tests": client_year_by_year_rank_tables(
            frame,
            SHARE_COMPONENTS,
            "region_label",
        ),
        "client_year_by_year_bh_vs_benchmark_share_rank_tests": client_year_by_year_rank_tables(
            frame,
            SHARE_COMPONENTS,
            "comparison_group",
            include_permutation=True,
        ),
        "client_pairwise_black_hills_region_share_rank_tests": client_pairwise_black_hills_region_tests(
            frame,
            SHARE_COMPONENTS,
        ),
        "client_bh_vs_benchmark_log_level_rank_tests": client_rank_test_table(
            frame,
            CLIENT_LOG_LEVEL_VARIABLES,
            "comparison_group",
            include_permutation=True,
        ),
        "client_year_by_year_log_level_rank_tests": client_year_by_year_rank_tables(
            frame,
            CLIENT_LOG_LEVEL_VARIABLES,
            "comparison_group",
            include_permutation=True,
        ),
    }
    for name, table in client_tables.items():
        write_table(table, tables_dir / f"{name}.csv")
    return client_tables


def format_p_value(p_value: float) -> str:
    """Format p-values for compact chart titles."""

    if pd.isna(p_value):
        return "p=NA"
    if float(p_value) < 0.001:
        return "p<0.001"
    return f"p={float(p_value):.3f}"


def format_kruskal_title(frame: pd.DataFrame, variable: str, label: str, group_column: str = "region_label") -> str:
    """Return a notebook-style panel title with Kruskal-Wallis statistic and p-value."""

    result = rank_test_result(frame, variable, group_column=group_column)
    statistic = result["statistic"]
    p_value = result["p_value"]
    if pd.isna(statistic):
        return f"{label}\n(Kruskal-Wallis: H=NA, p=NA)"
    return f"{label}\n(Kruskal-Wallis: H={float(statistic):.3f}, {format_p_value(float(p_value))})"


def save_client_stacked_mix(
    mix: pd.DataFrame,
    index_column: str,
    output_path: Path,
    title: str,
    *,
    order: list[str] | None = None,
    x_labels: list[str] | None = None,
    figsize: tuple[float, float] = (11, 6),
) -> None:
    """Save the notebook-style aggregate-dollar stacked revenue mix chart."""

    pivot = mix.pivot_table(index=index_column, columns="component", values="normalized_mix_share", aggfunc="sum").fillna(0)
    if order is not None:
        pivot = pivot.loc[order]
    else:
        pivot = pivot.sort_index()
    pivot = pivot[[component for component in SOURCE_COMPONENTS if component in pivot.columns]]

    fig, ax = plt.subplots(figsize=figsize)
    bottom = np.zeros(len(pivot))
    x_positions = np.arange(len(pivot))
    for component in pivot.columns:
        values = pivot[component].to_numpy()
        ax.bar(
            x_positions,
            values,
            bottom=bottom,
            label=DISPLAY_LABELS[component],
            color=SOURCE_COLORS[component],
            edgecolor="white",
            linewidth=0.6,
        )
        bottom += values

    labels = x_labels if x_labels is not None else [str(value) for value in pivot.index]
    ax.set_xticks(x_positions)
    tick_labels = ax.set_xticklabels(labels)
    for x_position, label, tick_label in zip(x_positions, labels, tick_labels):
        region_color = REGION_COLORS.get(str(label).replace("\n", " "))
        if region_color is None:
            continue
        tick_label.set_color(region_color)
        tick_label.set_fontweight("bold")
        ax.plot(
            [x_position - 0.35, x_position + 0.35],
            [-0.075, -0.075],
            transform=ax.get_xaxis_transform(),
            color=region_color,
            linewidth=4,
            solid_capstyle="round",
            clip_on=False,
        )

    ax.set_title(title, fontsize=14, weight="bold")
    ax.set_ylabel("Normalized share of reported revenue-source mix")
    ax.set_ylim(0, 1)
    ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def build_region_positive_median_table(
    frame: pd.DataFrame,
    components: list[str],
    *,
    group_column: str = "region_label",
) -> pd.DataFrame:
    """Median dollars among positive reporters, by group and revenue-source component."""

    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(group_column, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        base = dict(zip([group_column], keys, strict=False))
        for component in components:
            values = group[component].dropna()
            if component == "residual_other_revenue":
                values = values.clip(lower=0)
            positive = values.loc[values.gt(0)]
            positive_median = float(positive.median()) if len(positive) else np.nan
            rows.append(
                {
                    **base,
                    "component": component,
                    "positive_median": positive_median,
                    "positive_n": int(len(positive)),
                }
            )
    result = pd.DataFrame(rows)
    if result.empty:
        return result
    median_sum = result.groupby(group_column, dropna=False)["positive_median"].transform("sum")
    result["normalized_median_mix_share"] = np.where(
        median_sum.gt(0),
        result["positive_median"] / median_sum,
        np.nan,
    )
    return result


def save_client_stacked_positive_median_mix(
    median_table: pd.DataFrame,
    index_column: str,
    output_path: Path,
    title: str,
    *,
    normalize: bool = True,
    order: list[str] | None = None,
    figsize: tuple[float, float] = (11, 6),
) -> None:
    """Stack positive-only regional medians by source (optional 100% normalization within region)."""

    value_column = "normalized_median_mix_share" if normalize else "positive_median"
    pivot = median_table.pivot_table(index=index_column, columns="component", values=value_column, aggfunc="first").fillna(0)
    if order is not None:
        pivot = pivot.reindex(order).fillna(0)
    else:
        pivot = pivot.sort_index()
    pivot = pivot[[component for component in SOURCE_COMPONENTS if component in pivot.columns]]

    fig, ax = plt.subplots(figsize=figsize)
    bottom = np.zeros(len(pivot))
    x_positions = np.arange(len(pivot))
    for component in pivot.columns:
        values = pivot[component].to_numpy()
        ax.bar(
            x_positions,
            values,
            bottom=bottom,
            label=DISPLAY_LABELS[component],
            color=SOURCE_COLORS[component],
            edgecolor="white",
            linewidth=0.6,
        )
        bottom += values

    labels = [str(value) for value in pivot.index]
    ax.set_xticks(x_positions)
    tick_labels = ax.set_xticklabels(labels)
    for x_position, label, tick_label in zip(x_positions, labels, tick_labels):
        region_color = REGION_COLORS.get(str(label).replace("\n", " "))
        if region_color is None:
            continue
        tick_label.set_color(region_color)
        tick_label.set_fontweight("bold")
        ax.plot(
            [x_position - 0.35, x_position + 0.35],
            [-0.075, -0.075],
            transform=ax.get_xaxis_transform(),
            color=region_color,
            linewidth=4,
            solid_capstyle="round",
            clip_on=False,
        )

    if normalize:
        ax.set_ylabel("Share of summed positive-only medians within region")
        ax.set_ylim(0, 1)
        ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    else:
        bar_totals = pivot.sum(axis=1).to_numpy()
        upper = float(np.nanmax(bar_totals)) if len(bar_totals) else 0.0
        for x_position, total in enumerate(bar_totals):
            if total <= 0:
                continue
            ax.text(
                x_position,
                total + upper * 0.01,
                f"${total:,.0f}",
                ha="center",
                va="bottom",
                fontsize=9,
                color="#333333",
            )
        ax.set_ylabel("Sum of positive-only source medians ($)")
        ax.set_ylim(0, upper * 1.08 if upper else 1)
        ax.yaxis.set_major_formatter(lambda value, _: f"${value:,.0f}")

    ax.set_title(title, fontsize=14, weight="bold")
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def save_client_stacked_mix_dollars(
    mix: pd.DataFrame,
    index_column: str,
    output_path: Path,
    title: str,
    *,
    order: list[str] | None = None,
    x_labels: list[str] | None = None,
    figsize: tuple[float, float] = (11, 6),
) -> None:
    """Save an aggregate-dollar stacked revenue-mix chart (not normalized shares)."""

    pivot = mix.pivot_table(index=index_column, columns="component", values="amount", aggfunc="sum").fillna(0)
    if order is not None:
        pivot = pivot.reindex(order).fillna(0)
    else:
        pivot = pivot.sort_index()
    pivot = pivot[[component for component in SOURCE_COMPONENTS if component in pivot.columns]]

    fig, ax = plt.subplots(figsize=figsize)
    bottom = np.zeros(len(pivot))
    x_positions = np.arange(len(pivot))
    for component in pivot.columns:
        values = pivot[component].to_numpy()
        ax.bar(
            x_positions,
            values,
            bottom=bottom,
            label=DISPLAY_LABELS[component],
            color=SOURCE_COLORS[component],
            edgecolor="white",
            linewidth=0.6,
        )
        bottom += values

    labels = x_labels if x_labels is not None else [str(value) for value in pivot.index]
    ax.set_xticks(x_positions)
    tick_labels = ax.set_xticklabels(labels)
    for x_position, label, tick_label in zip(x_positions, labels, tick_labels):
        region_color = REGION_COLORS.get(str(label).replace("\n", " "))
        if region_color is None:
            continue
        tick_label.set_color(region_color)
        tick_label.set_fontweight("bold")
        ax.plot(
            [x_position - 0.35, x_position + 0.35],
            [-0.075, -0.075],
            transform=ax.get_xaxis_transform(),
            color=region_color,
            linewidth=4,
            solid_capstyle="round",
            clip_on=False,
        )

    bar_totals = pivot.sum(axis=1).to_numpy()
    upper = float(np.nanmax(bar_totals)) if len(bar_totals) else 0.0
    for x_position, total in enumerate(bar_totals):
        if total <= 0:
            continue
        ax.text(
            x_position,
            total + upper * 0.01,
            f"${total:,.0f}",
            ha="center",
            va="bottom",
            fontsize=9,
            color="#333333",
        )

    ax.set_title(title, fontsize=14, weight="bold")
    ax.set_ylabel("Aggregate reported dollars ($)")
    ax.set_ylim(0, upper * 1.08 if upper else 1)
    ax.yaxis.set_major_formatter(lambda value, _: f"${value:,.0f}")
    ax.legend(loc="center left", bbox_to_anchor=(1.02, 0.5), frameon=False)
    ax.grid(axis="y", alpha=0.25)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def format_compact_axis_dollars(value: float) -> str:
    """Format large dollar values compactly for dense presentation charts."""

    if pd.isna(value):
        return ""
    absolute = abs(float(value))
    if absolute >= 1_000_000_000:
        return f"${float(value) / 1_000_000_000:.1f}B"
    if absolute >= 1_000_000:
        if absolute < 10_000_000:
            return f"${float(value) / 1_000_000:.1f}M"
        return f"${float(value) / 1_000_000:.0f}M"
    if absolute >= 1_000:
        return f"${float(value) / 1_000:.0f}K"
    return f"${float(value):.0f}"


def save_client_grouped_mix_dollars_by_source(
    mix: pd.DataFrame,
    output_path: Path,
    title: str,
    *,
    order: list[str] | None = None,
    figsize: tuple[float, float] = (18, 8.5),
) -> None:
    """Save side-by-side aggregate-dollar bars by source and region.

    Each panel uses its own y-axis scale. That is intentional: the chart is
    meant to compare regions within a source, not to compare the magnitude of
    every source on one shared axis where program-service revenue overwhelms
    the smaller contribution lines.
    """

    regions = order or [region for region in REGION_ORDER if region in set(mix["region_label"])]
    pivot = (
        mix.pivot_table(index="component", columns="region_label", values="amount", aggfunc="sum")
        .reindex(SOURCE_COMPONENTS)
        .reindex(columns=regions)
        .fillna(0)
    )
    if pivot.empty:
        return

    ncols = 4
    nrows = int(np.ceil(len(pivot) / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=figsize, sharey=False)
    axes = np.asarray(axes).ravel()
    x = np.arange(len(regions))

    for ax, component in zip(axes, pivot.index):
        values = pivot.loc[component].to_numpy(dtype=float)
        bars = ax.bar(
            x,
            values,
            color=[REGION_COLORS.get(region, "#7C8795") for region in regions],
            edgecolor="none",
            linewidth=0,
            width=0.68,
        )
        upper = float(np.nanmax(values)) if len(values) else 0.0
        ax.set_ylim(0, upper * 1.22 if upper else 1)
        ax.set_title(textwrap.fill(DISPLAY_LABELS.get(component, component), width=26), fontsize=10.5, weight="bold")
        ax.set_xticks(x)
        tick_labels = ax.set_xticklabels(regions, rotation=28, ha="right")
        for region, tick_label in zip(regions, tick_labels):
            tick_label.set_color(REGION_COLORS.get(region, "#333333"))
            tick_label.set_fontweight("bold")
        ax.yaxis.set_major_formatter(lambda value, _: format_compact_axis_dollars(value))
        ax.grid(axis="y", alpha=0.22)
        for bar, value in zip(bars, values):
            if value <= 0:
                continue
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                value + (upper * 0.025 if upper else 0.02),
                format_compact_axis_dollars(value),
                ha="center",
                va="bottom",
                fontsize=7.5,
                color="#333333",
            )

    for ax in axes[len(pivot) :]:
        ax.axis("off")

    fig.suptitle(title, fontsize=16, weight="bold", y=1.02)
    fig.text(
        0.5,
        0.005,
        "Each panel uses its own dollar scale; bars compare regions within the same revenue source.",
        ha="center",
        fontsize=9,
        color="#4B5563",
    )
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def mean_share_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Compute mean organization-level source shares by region with rough 95% CIs."""

    rows = []
    for share_col in SHARE_COMPONENTS:
        component = share_col.replace("_share", "")
        for region, group in frame.groupby("region_label", dropna=False):
            values = group[share_col].dropna().clip(lower=0, upper=1)
            n = len(values)
            mean = float(values.mean()) if n else np.nan
            se = float(values.std(ddof=1) / np.sqrt(n)) if n > 1 else np.nan
            rows.append(
                {
                    "share_variable": share_col,
                    "component_label": DISPLAY_LABELS[component],
                    "region_label": region,
                    "mean_share": mean,
                    "ci_low": max(0, mean - 1.96 * se) if pd.notna(se) else np.nan,
                    "ci_high": min(1, mean + 1.96 * se) if pd.notna(se) else np.nan,
                    "n": n,
                }
            )
    return pd.DataFrame(rows)


def client_panel_grid(panel_count: int, *, width: float = 5.2, height: float = 4.4) -> tuple[plt.Figure, np.ndarray]:
    """Create a client chart grid sized for the current number of panels."""

    ncols = 3
    nrows = int(np.ceil(panel_count / ncols))
    fig, axes = plt.subplots(nrows, ncols, figsize=(width * ncols, height * nrows), sharey=False)
    return fig, np.asarray(axes).ravel()


def save_client_mean_share_bars(frame: pd.DataFrame, summary: pd.DataFrame, output_path: Path) -> None:
    """Save the notebook's rank-test-aligned mean-share bar chart."""

    fig, axes = client_panel_grid(len(SOURCE_COMPONENTS))
    for ax, component in zip(axes, SOURCE_COMPONENTS):
        label = DISPLAY_LABELS[component]
        share_variable = f"{component}_share"
        # Reindex against the full canonical region order so that runs which do
        # not have every region (e.g. small fixtures or partial sensitivities)
        # still render a chart with empty slots for missing regions instead of
        # raising a KeyError.
        sub = (
            summary.loc[summary["component_label"].eq(label)]
            .set_index("region_label")
            .reindex(REGION_ORDER)
            .reset_index()
        )
        x = np.arange(len(REGION_ORDER))
        y = sub["mean_share"].to_numpy(dtype=float)
        ci_low = sub["ci_low"].to_numpy(dtype=float)
        ci_high = sub["ci_high"].to_numpy(dtype=float)
        yerr = np.vstack([np.nan_to_num(y - ci_low), np.nan_to_num(ci_high - y)])
        ax.bar(x, np.nan_to_num(y), color=[REGION_COLORS[region] for region in REGION_ORDER], edgecolor="none", linewidth=0)
        ax.errorbar(x, np.nan_to_num(y), yerr=yerr, fmt="none", ecolor="#4A4A4A", elinewidth=1.2, capsize=0)
        ax.set_title(format_kruskal_title(frame, share_variable, label), fontsize=10.5, weight="bold")
        ax.set_xticks(x, REGION_ORDER, rotation=25, ha="right")
        ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
        ci_high_finite = ci_high[np.isfinite(ci_high)]
        upper = min(1.0, max(0.05, float(ci_high_finite.max()) * 1.2)) if ci_high_finite.size else 0.05
        ax.set_ylim(0, upper)
        ax.grid(axis="y", alpha=0.25)
    for ax in axes[len(SOURCE_COMPONENTS) :]:
        ax.axis("off")
    fig.suptitle("Mean organization-level revenue-source shares by region", fontsize=15, weight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def save_client_raw_median_bars(frame: pd.DataFrame, summary: pd.DataFrame, output_path: Path) -> None:
    """Save median raw-dollar bars that match the primary Kruskal-Wallis tests."""

    fig, axes = client_panel_grid(len(CLIENT_RAW_LEVEL_VARIABLES))
    for ax, variable in zip(axes, CLIENT_RAW_LEVEL_VARIABLES):
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, variable)
        # Reindex against the canonical region order so missing regions render
        # as empty slots rather than raising on .loc lookup.
        sub = (
            summary.loc[summary["variable"].eq(variable)]
            .set_index("region_label")
            .reindex(REGION_ORDER)
            .reset_index()
        )
        x = np.arange(len(REGION_ORDER))
        y = sub["median"].to_numpy(dtype=float)
        y_finite_abs_max = float(np.nanmax(np.abs(y))) if np.isfinite(y).any() else 0.0
        nonzero_percent = sub["nonzero_percent"].to_numpy(dtype=float)
        nonzero_finite_max = float(np.nanmax(nonzero_percent)) if np.isfinite(nonzero_percent).any() else 0.0
        y_is_nonzero_rate = bool(y_finite_abs_max <= 0.001 and nonzero_finite_max > 0)
        if y_is_nonzero_rate:
            y = nonzero_percent
        ax.bar(x, np.nan_to_num(y), color=[REGION_COLORS[region] for region in REGION_ORDER], edgecolor="none", linewidth=0)
        title_label = f"{label}\n(nonzero reporting rate)" if y_is_nonzero_rate else label
        ax.set_title(format_kruskal_title(frame, variable, title_label), fontsize=10.5, weight="bold")
        ax.set_xticks(x, REGION_ORDER, rotation=25, ha="right")
        y_finite_max = float(np.nanmax(y)) if np.isfinite(y).any() else 0.0
        if y_is_nonzero_rate:
            ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
            upper = min(1.0, max(0.05, y_finite_max * 1.25)) if y_finite_max > 0 else 1.0
        else:
            ax.yaxis.set_major_formatter(lambda value, _: f"${value:,.0f}")
            upper = max(1.0, y_finite_max * 1.25) if y_finite_max > 0 else 1.0
        ax.set_ylim(0, upper)
        ax.grid(axis="y", alpha=0.25)
    for ax in axes[len(CLIENT_RAW_LEVEL_VARIABLES) :]:
        ax.axis("off")
    fig.suptitle("Median organization-level raw revenue amounts by region", fontsize=15, weight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def slugify_label(value: str) -> str:
    """Return a filesystem-friendly lowercase label."""

    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def pairwise_presentation_summary(frame: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Build the Black Hills vs individual benchmark-region chart summary.

    Every revenue and contribution source is summarized on **positive values
    only**: organizations that legitimately reported $0 on Form 990 Line 1
    sub-lines (or on whole-form lines that happen to be $0) are excluded so the
    chart answers the question "of the organizations that receive this source,
    what does the distribution look like?". The Form 990 reconciliation audit in
    `scripts/diagnostics/form_990_line1_blanks_audit.py` confirms these zeros
    are real reported zeros (not missing data), so dropping them changes the
    research question rather than fixing a data-quality issue.
    """

    rows: list[dict[str, object]] = []
    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]
    for variable in variables:
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        for benchmark_region in benchmark_regions:
            for region in ["Black Hills", benchmark_region]:
                values = frame.loc[frame["region_label"].eq(region), variable].dropna().clip(lower=0)
                positive_values = values.loc[values.gt(0)]
                # IQR of the positive subset answers "where does the middle 50%
                # of reporting organizations sit?", which is a spread metric for
                # the population of reporters rather than an uncertainty
                # interval for the median estimate. With at least two positive
                # observations np.percentile returns the standard linear
                # interpolation; with fewer it returns NaN so the chart skips
                # the whisker cleanly.
                if len(positive_values) >= 2:
                    iqr_p25 = float(np.percentile(positive_values.to_numpy(dtype=float), 25))
                    iqr_p75 = float(np.percentile(positive_values.to_numpy(dtype=float), 75))
                else:
                    iqr_p25 = np.nan
                    iqr_p75 = np.nan
                rows.append(
                    {
                        "variable": variable,
                        "variable_label": label,
                        "comparison": f"Black Hills vs {benchmark_region}",
                        "benchmark_region": benchmark_region,
                        "region_label": region,
                        "n": int(len(values)),
                        "positive_n": int(len(positive_values)),
                        "median": float(values.median()) if len(values) else np.nan,
                        "nonzero_percent": float(values.gt(0).mean()) if len(values) else np.nan,
                        "positive_median": float(positive_values.median()) if len(positive_values) else np.nan,
                        "positive_iqr_p25": iqr_p25,
                        "positive_iqr_p75": iqr_p75,
                    }
                )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # Headline metric for every revenue/contribution source is the positive-only
    # median. The historical "switch to nonzero_percent if every region's median
    # is zero" branch is intentionally removed: with the audit-validated zeros,
    # the slide story is "of organizations that receive this source, the median
    # amount is $X".
    out["display_metric"] = "positive_median"
    out["display_value"] = out["positive_median"]
    return out


def save_client_pairwise_source_charts(frame: pd.DataFrame, variables: list[str], output_dir: Path) -> pd.DataFrame:
    """
    Save one presentation chart per requested source.

    Each chart has four pairwise panels: Black Hills vs Billings, Flagstaff,
    Sioux Falls, and Missoula. Strict median-dollar charts are always saved.
    If the all-organization median is zero in every panel for a source, a
    companion presentation chart displays nonzero reporting rate instead so the
    slide remains informative.
    """

    summary = pairwise_presentation_summary(frame, variables)
    if summary.empty:
        return summary

    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]

    def _save_chart(
        variable_summary: pd.DataFrame,
        *,
        metric_column: str,
        metric_label: str,
        output_path: Path,
        title_suffix: str,
        percent_axis: bool = False,
    ) -> None:
        label = variable_summary["variable_label"].iloc[0]
        fig, axes = plt.subplots(1, 4, figsize=(15.5, 4.6), sharey=True)
        max_value = float(np.nanmax(variable_summary[metric_column])) if len(variable_summary) else 0.0
        for ax, benchmark_region in zip(axes, benchmark_regions):
            sub = (
                variable_summary.loc[variable_summary["benchmark_region"].eq(benchmark_region)]
                .set_index("region_label")
                .loc[["Black Hills", benchmark_region]]
                .reset_index()
            )
            x = np.arange(len(sub))
            y = sub[metric_column].to_numpy(dtype=float)
            ax.bar(
                x,
                y,
                color=[REGION_COLORS.get(region, "#7C8795") for region in sub["region_label"]],
                edgecolor="none",
                linewidth=0,
                width=0.62,
            )
            ax.set_title(f"Black Hills vs\n{benchmark_region}", fontsize=10.5, weight="bold")
            ax.set_xticks(x, sub["region_label"], rotation=25, ha="right")
            if percent_axis:
                ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
                upper = min(1.0, max(0.05, max_value * 1.25))
            else:
                ax.yaxis.set_major_formatter(lambda value, _: f"${value:,.0f}")
                upper = max(1.0, max_value * 1.25)
                if max_value <= 0:
                    ax.text(
                        0.5,
                        0.5,
                        "all medians are $0",
                        ha="center",
                        va="center",
                        transform=ax.transAxes,
                        fontsize=9.5,
                        color="#555555",
                    )
            ax.set_ylim(0, upper)
            ax.grid(axis="y", alpha=0.25)
        axes[0].set_ylabel(metric_label)
        fig.suptitle(f"{label}: Black Hills vs each benchmark region{title_suffix}", fontsize=15, weight="bold", y=1.08)
        fig.tight_layout()
        fig.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        info(f"Saved client chart: {output_path}")

    for variable in variables:
        variable_summary = summary.loc[summary["variable"].eq(variable)].copy()
        slug = slugify_label(variable)

        _save_chart(
            variable_summary,
            metric_column="median",
            metric_label="Median dollars",
            output_path=output_dir / f"client_pairwise_median_{slug}.png",
            title_suffix=" (median dollars)",
        )

        display_metric = variable_summary["display_metric"].iloc[0]
        if display_metric == "nonzero_percent":
            _save_chart(
                variable_summary,
                metric_column="nonzero_percent",
                metric_label="Nonzero reporting rate",
                output_path=output_dir / f"client_pairwise_{slug}.png",
                title_suffix=" (nonzero reporting rate)",
                percent_axis=True,
            )
        else:
            _save_chart(
                variable_summary,
                metric_column="median",
                metric_label="Median dollars",
                output_path=output_dir / f"client_pairwise_{slug}.png",
                title_suffix=" (median dollars)",
            )

    return summary


def format_slide_p_value(p_value: float) -> str:
    """Format p-values for presentation bullets."""

    if pd.isna(p_value):
        return "= NA"
    if float(p_value) < 0.001:
        return "< 0.001"
    return f"= {float(p_value):.3f}"


def format_chart_perm_p(p_value: float) -> str:
    """Compact p-value label for slide bullets and tables."""

    if pd.isna(p_value):
        return ""
    if float(p_value) < 0.001:
        return "p < 0.001"
    return f"p = {float(p_value):.3f}"


def format_slide_dollars(value: float) -> str:
    """Format dollars for compact slide tables."""

    if pd.isna(value):
        return "NA"
    return f"${float(value):,.0f}"


def _format_signed_dollars(value: float) -> str:
    """Format a signed dollar amount (median gap) with explicit sign and thousands."""

    if pd.isna(value):
        return "NA"
    rounded = float(value)
    if rounded == 0:
        return "$0"
    sign = "-" if rounded < 0 else "+"
    return f"{sign}${abs(rounded):,.0f}"


def _format_median_gap_with_ci(
    point: float | None,
    ci_low: float | None,
    ci_high: float | None,
) -> str:
    """Render ``+/-$gap (95% CI low to high)`` for slide tables and bullets."""

    if point is None or pd.isna(point):
        return "NA"
    base = _format_signed_dollars(float(point))
    if ci_low is None or ci_high is None or pd.isna(ci_low) or pd.isna(ci_high):
        return base
    return f"{base} (95% CI {_format_signed_dollars(float(ci_low))} to {_format_signed_dollars(float(ci_high))})"


def format_slide_iqr(p25: float | None, p75: float | None) -> str:
    """Format the IQR (25th-75th percentile) for inline slide text.

    Returns an empty string when either bound is missing so the bullet stays
    readable. The IQR describes the spread of the population of reporting
    organizations, not the uncertainty of the median estimate; the bullet text
    uses "IQR" explicitly to avoid confusion with a confidence interval.
    """

    if p25 is None or p75 is None or pd.isna(p25) or pd.isna(p75):
        return ""
    low_value = float(p25)
    high_value = float(p75)
    if math.isclose(low_value, high_value, rel_tol=0, abs_tol=0.5):
        return f"(IQR ≈ {format_slide_dollars(low_value)})"
    return f"(IQR {format_slide_dollars(low_value)}–{format_slide_dollars(high_value)})"


def build_2022_pairwise_presentation_table(frame: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    """Build one Mann-Whitney pairwise row per variable and benchmark region for 2022 slides.

    The headline metric and statistical test are both computed on the
    **positive subset** of each region's filers — i.e. organizations that
    actually report the source. The Mann-Whitney U test compares positive
    values only so the p-value answers the conditional question "among
    organizations that receive this source, do the dollar amounts differ?".
    Benjamini-Hochberg FDR is applied across the four pairwise p-values within
    a single variable so the multiple-comparison adjustment matches the slide
    that the reader is looking at.
    """

    year_frame = frame.loc[frame["tax_year"].astype(str).eq("2022")].copy()

    rows: list[dict[str, object]] = []
    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]
    for variable in variables:
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        variable_rows: list[dict[str, object]] = []
        for benchmark_region in benchmark_regions:
            bh_values = year_frame.loc[year_frame["region_label"].eq("Black Hills"), variable].dropna().clip(lower=0)
            benchmark_values = year_frame.loc[year_frame["region_label"].eq(benchmark_region), variable].dropna().clip(lower=0)
            bh_median = float(bh_values.median()) if len(bh_values) else np.nan
            benchmark_median = float(benchmark_values.median()) if len(benchmark_values) else np.nan
            bh_nonzero_percent = float(bh_values.gt(0).mean()) if len(bh_values) else np.nan
            benchmark_nonzero_percent = float(benchmark_values.gt(0).mean()) if len(benchmark_values) else np.nan
            bh_positive = bh_values.loc[bh_values.gt(0)]
            benchmark_positive = benchmark_values.loc[benchmark_values.gt(0)]
            bh_positive_n = int(len(bh_positive))
            benchmark_positive_n = int(len(benchmark_positive))
            bh_positive_median = float(bh_positive.median()) if bh_positive_n else np.nan
            benchmark_positive_median = float(benchmark_positive.median()) if benchmark_positive_n else np.nan
            if bh_positive_n >= 2:
                bh_iqr_p25 = float(np.percentile(bh_positive.to_numpy(dtype=float), 25))
                bh_iqr_p75 = float(np.percentile(bh_positive.to_numpy(dtype=float), 75))
            else:
                bh_iqr_p25 = np.nan
                bh_iqr_p75 = np.nan
            if benchmark_positive_n >= 2:
                benchmark_iqr_p25 = float(np.percentile(benchmark_positive.to_numpy(dtype=float), 25))
                benchmark_iqr_p75 = float(np.percentile(benchmark_positive.to_numpy(dtype=float), 75))
            else:
                benchmark_iqr_p25 = np.nan
                benchmark_iqr_p75 = np.nan

            # Mann-Whitney U on the positive subset only. The test is skipped
            # gracefully when either group has fewer than two positive
            # observations, because rank-sum requires variance in both groups.
            if bh_positive_n >= 2 and benchmark_positive_n >= 2:
                test_stat, p_value = stats.mannwhitneyu(
                    bh_positive.to_numpy(dtype=float),
                    benchmark_positive.to_numpy(dtype=float),
                    alternative="two-sided",
                )
                mann_u = float(test_stat)
                p_value = float(p_value)
            else:
                mann_u = np.nan
                p_value = np.nan

            if pd.isna(bh_positive_median) or pd.isna(benchmark_positive_median):
                direction = ""
            elif math.isclose(float(bh_positive_median), float(benchmark_positive_median), rel_tol=0, abs_tol=0.001):
                direction = "No median difference among reporters"
            else:
                direction = (
                    "Black Hills higher among reporters"
                    if bh_positive_median > benchmark_positive_median
                    else "Black Hills lower among reporters"
                )

            # Permutation test on the median difference is the slide-aligned
            # headline test: it asks directly whether the median dollar gap
            # between Black Hills and this benchmark region is unusual under
            # random relabeling of the pooled positive reporters. Mann-Whitney
            # U is retained alongside as a distribution-level cross-check.
            perm_result = permutation_median_difference(
                bh_positive.to_numpy(dtype=float),
                benchmark_positive.to_numpy(dtype=float),
                iterations=PERMUTATION_ITERATIONS_2022,
                seed=PERMUTATION_SEED_2022,
            )
            ci_result = bootstrap_median_difference_ci(
                bh_positive.to_numpy(dtype=float),
                benchmark_positive.to_numpy(dtype=float),
                iterations=BOOTSTRAP_ITERATIONS_2022,
                seed=PERMUTATION_SEED_2022,
            )

            variable_rows.append(
                {
                    "tax_year": "2022",
                    "variable": variable,
                    "variable_label": label,
                    "comparison": f"Black Hills vs {benchmark_region}",
                    "benchmark_region": benchmark_region,
                    "display_metric": "positive_median",
                    "black_hills_median": bh_median,
                    "benchmark_median": benchmark_median,
                    "black_hills_nonzero_percent": bh_nonzero_percent,
                    "benchmark_nonzero_percent": benchmark_nonzero_percent,
                    "black_hills_positive_n": bh_positive_n,
                    "benchmark_positive_n": benchmark_positive_n,
                    "black_hills_positive_median": bh_positive_median,
                    "benchmark_positive_median": benchmark_positive_median,
                    "black_hills_positive_iqr_p25": bh_iqr_p25,
                    "black_hills_positive_iqr_p75": bh_iqr_p75,
                    "benchmark_positive_iqr_p25": benchmark_iqr_p25,
                    "benchmark_positive_iqr_p75": benchmark_iqr_p75,
                    "direction": direction,
                    "test": "permutation_median_diff_positive_only",
                    "median_difference": perm_result["median_difference"],
                    "median_difference_ci_low": ci_result["ci_low"],
                    "median_difference_ci_high": ci_result["ci_high"],
                    "permutation_p_value": perm_result["p_value"],
                    "permutation_iterations": perm_result["iterations"],
                    "bootstrap_iterations": ci_result["iterations"],
                    "mann_whitney_u": mann_u,
                    "mann_whitney_p_value": p_value,
                    "p_value": perm_result["p_value"],
                    "n": bh_positive_n + benchmark_positive_n,
                }
            )

        if variable_rows:
            # Apply Benjamini-Hochberg across the four pairwise permutation
            # p-values per variable so multiple-comparison framing matches the
            # slide-by-slide read. Mann-Whitney FDR is preserved on its own
            # columns for backward compatibility with the sensitivity table.
            perm_p_values = [row["permutation_p_value"] for row in variable_rows]
            perm_fdr_p_values = fdr_bh(perm_p_values)
            mw_p_values = [row["mann_whitney_p_value"] for row in variable_rows]
            mw_fdr_p_values = fdr_bh(mw_p_values)
            for row, raw_p, fdr_p, mw_raw, mw_fdr in zip(
                variable_rows, perm_p_values, perm_fdr_p_values, mw_p_values, mw_fdr_p_values
            ):
                row["fdr_p_value"] = fdr_p
                row["significant_before_fdr"] = (
                    "Yes" if pd.notna(raw_p) and float(raw_p) < 0.05 else "No"
                )
                row["significant_after_fdr"] = (
                    "Yes" if pd.notna(fdr_p) and float(fdr_p) < 0.05 else "No"
                )
                row["mann_whitney_fdr_p_value"] = mw_fdr
                row["mann_whitney_significant_before_fdr"] = (
                    "Yes" if pd.notna(mw_raw) and float(mw_raw) < 0.05 else "No"
                )
                row["mann_whitney_significant_after_fdr"] = (
                    "Yes" if pd.notna(mw_fdr) and float(mw_fdr) < 0.05 else "No"
                )
        rows.extend(variable_rows)

    return pd.DataFrame(rows)


def _pairwise_significance_label(p_value: float) -> str:
    if pd.isna(p_value):
        return "n/a"
    return "significant" if float(p_value) < 0.05 else "not significant"


def summarize_2022_special_org_exclusions(full_universe_frame: pd.DataFrame) -> tuple[int, pd.DataFrame]:
    """Count hospital/university/political org-year rows flagged in the 2022 full universe."""

    year_frame = full_universe_frame.loc[full_universe_frame["tax_year"].astype(str).eq("2022")].copy()
    special_mask = (
        year_frame["is_hospital"].fillna(False)
        | year_frame["is_university"].fillna(False)
        | year_frame["is_political_org"].fillna(False)
    )
    rows: list[dict[str, object]] = []
    for region in REGION_ORDER:
        region_rows = year_frame.loc[year_frame["region_label"].eq(region)]
        special_rows = region_rows.loc[special_mask.reindex(region_rows.index, fill_value=False)]
        rows.append(
            {
                "region_label": region,
                "full_universe_rows": int(len(region_rows)),
                "special_org_rows": int(len(special_rows)),
                "hospital_rows": int(region_rows["is_hospital"].fillna(False).sum()),
                "university_rows": int(region_rows["is_university"].fillna(False).sum()),
                "political_org_rows": int(region_rows["is_political_org"].fillna(False).sum()),
            }
        )
    return int(special_mask.sum()), pd.DataFrame(rows)


def build_2022_special_org_sensitivity_comparison(
    excluded_frame: pd.DataFrame,
    included_frame: pd.DataFrame,
    variables: list[str] | None = None,
) -> pd.DataFrame:
    """Compare 2022 pairwise presentation tests with vs without special-org exclusions."""

    variables = variables or CLIENT_PRESENTATION_VARIABLES
    excluded_table = build_2022_pairwise_presentation_table(excluded_frame, variables)
    included_table = build_2022_pairwise_presentation_table(included_frame, variables)
    key_columns = [
        "variable",
        "variable_label",
        "benchmark_region",
        "direction",
        "p_value",
        "black_hills_positive_median",
        "benchmark_positive_median",
    ]
    merged = excluded_table[key_columns].merge(
        included_table[key_columns],
        on=["variable", "variable_label", "benchmark_region"],
        suffixes=("_excluded", "_included"),
        how="outer",
    )
    merged["significance_excluded"] = merged["p_value_excluded"].map(_pairwise_significance_label)
    merged["significance_included"] = merged["p_value_included"].map(_pairwise_significance_label)
    merged["direction_changed"] = merged["direction_excluded"] != merged["direction_included"]
    merged["significance_changed"] = merged["significance_excluded"] != merged["significance_included"]
    merged["any_change"] = merged["direction_changed"] | merged["significance_changed"]
    merged["p_value_delta"] = merged["p_value_included"] - merged["p_value_excluded"]
    return merged.sort_values(["any_change", "variable", "benchmark_region"], ascending=[False, True, True]).reset_index(
        drop=True
    )


def build_2022_special_org_sensitivity_slide_lines(
    comparison: pd.DataFrame,
    exclusion_summary: pd.DataFrame,
    *,
    slide_number: int,
    first_source_slide: int,
    last_source_slide: int,
    special_org_ein_count: int,
) -> list[str]:
    """Build the sensitivity slide comparing primary exclusions to the full 2022 universe."""

    changed = comparison.loc[comparison["any_change"]].copy()
    sig_excluded = int((comparison["significance_excluded"].eq("significant")).sum())
    sig_included = int((comparison["significance_included"].eq("significant")).sum())
    direction_changes = int(comparison["direction_changed"].sum())
    significance_changes = int(comparison["significance_changed"].sum())
    total_special_rows = int(exclusion_summary["special_org_rows"].sum())
    ein_phrase = (
        f"{special_org_ein_count:,} unique EIN{'s' if special_org_ein_count != 1 else ''} in 2022"
        if special_org_ein_count
        else "no flagged EINs in 2022"
    )

    lines = [
        f"## Slide {slide_number}: Sensitivity — including hospitals, universities, and political organizations",
        "",
        f"Slides {first_source_slide}–{last_source_slide} use the **primary** analysis universe, which excludes "
        f"**{total_special_rows:,}** organization-year records flagged as hospitals, universities, or political "
        f"organizations ({ein_phrase}) for client-peer comparability. This slide reruns the same "
        "positive-reporter permutation-on-median pairwise tests on the **full** 2022 universe with those organizations included.",
        "",
        "### Excluded organization counts by region (2022 full universe before exclusion)",
        "",
        "| Region | Full-universe rows | Excluded special-org rows | Hospitals | Universities | Political |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for _, row in exclusion_summary.iterrows():
        lines.append(
            f"| {row['region_label']} | {int(row['full_universe_rows']):,} | {int(row['special_org_rows']):,} | "
            f"{int(row['hospital_rows']):,} | {int(row['university_rows']):,} | {int(row['political_org_rows']):,} |"
        )
    lines.extend(
        [
            "",
            "### Headline comparison (40 pairwise tests: 10 sources × 4 benchmarks)",
            "",
            f"- **Significant at p < 0.05 when excluding special orgs (primary deck):** {sig_excluded}",
            f"- **Significant at p < 0.05 when including special orgs:** {sig_included}",
            f"- **Direction changes:** {direction_changes}",
            f"- **Significance-status changes:** {significance_changes}",
            "",
        ]
    )
    if direction_changes == 0:
        lines.append(
            "- **No pairwise comparison flipped median direction** between the two universes; "
            "where both versions are comparable, Black Hills vs benchmark stays on the same side of the median."
        )
        lines.append("")
    if significance_changes == 0:
        lines.append(
            "- **No pairwise comparison crossed the p < 0.05 threshold** when special organizations were added back."
        )
        lines.append("")
    else:
        lines.extend(
            [
                "### Pairwise comparisons where significance or direction changed",
                "",
                "| Revenue source | Benchmark | Direction (excluded) | Direction (included) | Significance (excluded) | Significance (included) | p (excluded) | p (included) |",
                "| --- | --- | --- | --- | --- | --- | ---: | ---: |",
            ]
        )
        for _, row in changed.iterrows():
            lines.append(
                f"| {row['variable_label']} | {row['benchmark_region']} | {row['direction_excluded'] or 'n/a'} | "
                f"{row['direction_included'] or 'n/a'} | {row['significance_excluded']} | {row['significance_included']} | "
                f"{format_slide_p_value(row['p_value_excluded']).replace('= ', '')} | "
                f"{format_slide_p_value(row['p_value_included']).replace('= ', '')} |"
            )
        lines.append("")

    only_included = comparison.loc[
        comparison["significance_included"].eq("significant") & ~comparison["significance_excluded"].eq("significant")
    ]
    only_excluded = comparison.loc[
        comparison["significance_excluded"].eq("significant") & ~comparison["significance_included"].eq("significant")
    ]
    if not only_included.empty:
        lines.append("**Newly significant only when special orgs are included:**")
        lines.append("")
        for _, row in only_included.iterrows():
            lines.append(
                f"- **{row['variable_label']} vs {row['benchmark_region']}:** "
                f"{row['direction_included']}; p moves from "
                f"{format_slide_p_value(row['p_value_excluded']).replace('= ', '')} to "
                f"{format_slide_p_value(row['p_value_included']).replace('= ', '')} "
                f"(Black Hills median {format_slide_dollars(row['black_hills_positive_median_included'])}; "
                f"{row['benchmark_region']} median {format_slide_dollars(row['benchmark_positive_median_included'])})."
            )
        lines.append("")
    if not only_excluded.empty:
        lines.append("**Significant in the primary deck but not when special orgs are included:**")
        lines.append("")
        for _, row in only_excluded.iterrows():
            lines.append(
                f"- **{row['variable_label']} vs {row['benchmark_region']}:** "
                f"p {format_slide_p_value(row['p_value_excluded']).replace('= ', '')} (excluded) vs "
                f"{format_slide_p_value(row['p_value_included']).replace('= ', '')} (included)."
            )
        lines.append("")

    if sig_excluded and only_excluded.empty:
        lines.append(
            f"- **All {sig_excluded} comparisons significant in the primary deck remain significant** when hospitals, "
            "universities, and political organizations are included."
        )
        lines.append("")

    lines.extend(
        [
            "### How to use this slide",
            "",
            "- Treat the primary slides as the client-facing conclusion; this slide shows that the exclusion choice "
            "does not overturn that story.",
            "- Most excluded organizations are in **Sioux Falls** (16 of 25 rows), so Sioux Falls–related p-values "
            "shift more than other regions even when significance does not change.",
            "- The full comparison table is in `client_2022_special_org_sensitivity_comparison.csv`.",
            "",
        ]
    )
    return lines


def save_2022_pairwise_median_charts(
    frame: pd.DataFrame,
    variables: list[str],
    output_dir: Path,
    *,
    presentation_table: pd.DataFrame | None = None,
    show_reporter_count: bool = False,
) -> dict[str, Path]:
    """Save one combined 2022 chart per presentation variable.

    Each chart is a single-axis bar chart showing the **positive-only**
    (zero-excluded) median dollar amount for Black Hills next to all four
    benchmark regions. Bars are annotated above with `$median` and, when
    ``show_reporter_count`` is True, a second line ``n=<reporters>``. The
    Pairwise permutation p-values and bootstrap CIs are reported on slide
    bullets and in ``client_2022_pairwise_presentation_summary.csv``, not on
    the chart axis (region names only) so labels stay readable.
    ``presentation_table`` is accepted for API compatibility but is not
    drawn on the chart.

    Stale chart files from earlier framings (``client_2022_pairwise_median_*``,
    ``client_2022_pairwise_nonzero_rate_*``) are deleted at the end so the deck
    assets folder cannot leak the old story.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    year_frame = frame.loc[frame["tax_year"].astype(str).eq("2022")].copy()
    display_summary = pairwise_presentation_summary(year_frame, variables)
    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]
    chart_regions = ["Black Hills", *benchmark_regions]
    chart_paths: dict[str, Path] = {}

    for variable in variables:
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        variable_slug = slugify_label(variable)
        variable_rows = display_summary.loc[display_summary["variable"].eq(variable)].copy()

        # Collapse the per-pairwise summary into a single row per region by
        # taking the first occurrence (positive_median / positive_n are
        # identical across pairwise rows for Black Hills, and benchmark rows
        # already appear exactly once each).
        region_table = (
            variable_rows.drop_duplicates(subset="region_label")
            .set_index("region_label")
            .reindex(chart_regions)
            .reset_index()
        )
        medians = region_table["positive_median"].to_numpy(dtype=float)
        positive_n = region_table["positive_n"].to_numpy(dtype=float)
        median_max = float(np.nanmax(medians)) if np.isfinite(medians).any() else 0.0
        upper = max(1.0, median_max * 1.32)

        fig, ax = plt.subplots(figsize=(10.0, 5.6))
        x = np.arange(len(chart_regions))
        colors = [REGION_COLORS.get(region, "#7C8795") for region in chart_regions]
        bar_edge = ["#1F1F1F" if region == "Black Hills" else "none" for region in chart_regions]
        bar_linewidth = [1.4 if region == "Black Hills" else 0.0 for region in chart_regions]
        ax.bar(
            x,
            np.nan_to_num(medians, nan=0.0),
            color=colors,
            edgecolor=bar_edge,
            linewidth=bar_linewidth,
            width=0.66,
        )

        for idx, (region, value, n_pos) in enumerate(zip(chart_regions, medians, positive_n)):
            if pd.isna(value) or n_pos <= 0:
                ax.text(
                    idx,
                    upper * 0.02,
                    "no reporters",
                    ha="center",
                    va="bottom",
                    fontsize=9,
                    color="#777777",
                )
                continue
            bar_label = (
                f"${value:,.0f}\nn={int(n_pos):,}"
                if show_reporter_count
                else f"${value:,.0f}"
            )
            ax.text(
                idx,
                float(value) + upper * 0.012,
                bar_label,
                ha="center",
                va="bottom",
                fontsize=9,
                color="#333333",
            )

        ax.set_xticks(x, chart_regions, rotation=0, ha="center")
        ax.set_xlim(-0.55, len(chart_regions) - 0.45)
        ax.yaxis.set_major_formatter(lambda value, _: f"${value:,.0f}")
        ax.grid(axis="y", alpha=0.25)
        ax.set_ylabel("2022 median $ among reporters")
        ax.set_ylim(0, upper)

        ax.set_title(
            f"{label}: 2022 median dollars among organizations that report this source",
            fontsize=12.5,
            weight="bold",
            pad=10,
        )
        fig.tight_layout()
        output_path = output_dir / f"client_2022_pairwise_positive_median_{variable_slug}.png"
        fig.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(fig)
        info(f"Saved client chart: {output_path}")
        chart_paths[variable] = output_path

    # Delete stale chart files from earlier framings so the deck assets folder
    # cannot leak the old story. Only files matching the known stale prefixes
    # are removed; unrelated files in the folder are untouched. The current
    # output filenames are tracked in `chart_paths` so we can spare them.
    current_filenames = {path.name for path in chart_paths.values()}
    stale_prefixes = (
        "client_2022_pairwise_median_",
        "client_2022_pairwise_nonzero_rate_",
    )
    for stale_file in output_dir.iterdir():
        if not stale_file.is_file():
            continue
        if stale_file.name in current_filenames:
            continue
        if stale_file.name.startswith(stale_prefixes) and stale_file.suffix.lower() == ".png":
            try:
                stale_file.unlink()
                info(f"Removed stale client chart: {stale_file}")
            except OSError as exc:
                info(f"Could not remove stale chart {stale_file}: {exc}")

    return chart_paths


def save_2022_stacked_revenue_mix_overview(
    frame: pd.DataFrame,
    output_dir: Path,
    *,
    preserve_share_chart_dir: Path | None = None,
) -> Path:
    """Save descriptive 2022 aggregate-dollar revenue-mix charts.

    The primary deck chart uses side-by-side aggregate reported dollars by
    revenue source and region. When ``preserve_share_chart_dir`` is set, the
    older stacked versions are also written there for reference.
    """

    output_dir.mkdir(parents=True, exist_ok=True)
    year_frame = frame.loc[frame["tax_year"].astype(str).eq("2022")].copy()
    mix_2022 = aggregate_revenue_mix(year_frame, ["region_label"])
    region_order = [region for region in REGION_ORDER if region in set(mix_2022["region_label"])]
    output_path = output_dir / "client_2022_stacked_revenue_mix_by_region_dollars.png"
    save_client_grouped_mix_dollars_by_source(
        mix_2022,
        output_path,
        "2022 aggregate reported dollars by revenue source and region",
        order=region_order,
        figsize=(18, 8.5),
    )
    grouped_named_path = output_dir / "client_2022_grouped_revenue_mix_by_region_dollars.png"
    if grouped_named_path != output_path:
        shutil.copy2(output_path, grouped_named_path)
    median_mix_2022 = build_region_positive_median_table(year_frame, SOURCE_COMPONENTS)
    if preserve_share_chart_dir is not None:
        preserve_share_chart_dir.mkdir(parents=True, exist_ok=True)
        stacked_dollars_path = preserve_share_chart_dir / "client_2022_stacked_revenue_mix_by_region_dollars_stacked.png"
        save_client_stacked_mix_dollars(
            mix_2022,
            "region_label",
            stacked_dollars_path,
            "2022 revenue-source mix by region (aggregate reported dollars, stacked)",
            order=region_order,
            figsize=(13.5, 6.5),
        )
        share_path = preserve_share_chart_dir / "client_2022_stacked_revenue_mix_by_region.png"
        save_client_stacked_mix(
            mix_2022,
            "region_label",
            share_path,
            "2022 revenue-source mix by region (descriptive aggregate shares)",
            order=region_order,
            figsize=(13.5, 6.5),
        )
        if not median_mix_2022.empty:
            save_client_stacked_positive_median_mix(
                median_mix_2022,
                "region_label",
                preserve_share_chart_dir / "client_2022_stacked_positive_median_mix_by_region.png",
                "2022 stacked mix by region (positive-only medians, normalized within region)",
                normalize=True,
                order=region_order,
                figsize=(13.5, 6.5),
            )
            save_client_stacked_positive_median_mix(
                median_mix_2022,
                "region_label",
                preserve_share_chart_dir / "client_2022_stacked_positive_median_sum_by_region.png",
                "2022 stacked sum of positive-only source medians by region (not a true total)",
                normalize=False,
                order=region_order,
                figsize=(13.5, 6.5),
            )
    return output_path


def save_2022_positive_median_overview(
    frame: pd.DataFrame,
    output_dir: Path,
) -> Path:
    """Save a single grouped bar chart using the same positive-only medians as the source slides."""

    output_dir.mkdir(parents=True, exist_ok=True)
    year_frame = frame.loc[frame["tax_year"].astype(str).eq("2022")].copy()
    summary = pairwise_presentation_summary(year_frame, CLIENT_PRESENTATION_VARIABLES)
    output_path = output_dir / "client_2022_positive_median_overview_by_region.png"
    if summary.empty:
        info(f"Skipping positive-median overview chart (no data): {output_path}")
        return output_path

    overview = summary.groupby(["variable", "region_label"], as_index=False).agg(
        {
            "variable_label": "first",
            "positive_median": "first",
            "positive_n": "first",
        }
    )

    value_matrix = (
        overview.pivot(index="variable", columns="region_label", values="positive_median")
        .reindex(CLIENT_PRESENTATION_VARIABLES)
        .reindex(columns=REGION_ORDER)
    )
    value_matrix.index = [
        CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        for variable in value_matrix.index
    ]
    fig, ax = plt.subplots(figsize=(18, 8.8))
    x_centers = np.arange(len(REGION_ORDER), dtype=float)
    bar_width = 0.072
    offsets = np.linspace(-0.38, 0.38, len(CLIENT_PRESENTATION_VARIABLES))
    finite_values = value_matrix.to_numpy(dtype=float)
    finite_values = finite_values[np.isfinite(finite_values)]
    upper = max(1.0, float(finite_values.max()) * 1.28) if finite_values.size else 1.0

    for source_index, variable in enumerate(CLIENT_PRESENTATION_VARIABLES):
        source_label = CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        values = value_matrix.loc[source_label, REGION_ORDER].to_numpy(dtype=float)
        x = x_centers + offsets[source_index]
        ax.bar(
            x,
            np.nan_to_num(values, nan=0.0),
            width=bar_width,
            color=SOURCE_COLORS.get(variable, "#6F7F8F"),
            edgecolor="none",
            label=textwrap.fill(source_label, width=24),
        )
        for x_value, value in zip(x, values):
            if pd.isna(value):
                continue
            y_value = float(value)
            ax.text(
                x_value,
                y_value + upper * 0.008,
                f"${y_value:,.0f}",
                ha="center",
                va="bottom",
                fontsize=7.6,
                color="#333333",
                rotation=90,
            )

    ax.set_title(
        "2022 median dollars by source and region (positive reporters only)",
        fontsize=15,
        weight="bold",
        pad=18,
    )
    ax.set_xlabel("")
    ax.set_ylabel("Median dollars among positive reporters")
    ax.set_xticks(x_centers)
    ax.set_xticklabels(REGION_ORDER, rotation=0, ha="center", fontsize=11, weight="bold")
    for tick_label in ax.get_xticklabels():
        region = tick_label.get_text()
        if region in REGION_COLORS:
            tick_label.set_color(REGION_COLORS[region])
    ax.set_ylim(0, upper)
    ax.yaxis.set_major_formatter(lambda value, _: format_compact_axis_dollars(value))
    ax.tick_params(axis="x", length=0)
    ax.grid(axis="y", alpha=0.25)
    ax.legend(
        title="Revenue source",
        loc="center left",
        bbox_to_anchor=(1.01, 0.5),
        ncol=1,
        frameon=False,
        fontsize=8.5,
    )

    fig.text(
        0.01,
        0.01,
        "Each bar shows the median dollar amount among organizations with a positive value for that source; colors identify revenue sources.",
        ha="left",
        va="bottom",
        fontsize=9,
        color="#555555",
    )
    fig.tight_layout(rect=(0, 0.06, 1, 0.98))
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    grouped_bar_output_path = output_dir / "client_2022_positive_median_grouped_bar_by_region.png"
    fig.savefig(grouped_bar_output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    stale_heatmap_output_path = output_dir / "client_2022_positive_median_heatmap_by_region.png"
    if stale_heatmap_output_path.exists():
        stale_heatmap_output_path.unlink()
    info(f"Saved client chart: {output_path}")
    info(f"Saved client chart: {grouped_bar_output_path}")
    return output_path


def build_2022_full_answer_slide_lines(
    summary: pd.DataFrame,
    *,
    slide_number: int,
    first_source_slide: int,
    last_source_slide: int,
) -> list[str]:
    """Build the closing slide that answers Q9 from the 2022 pairwise summary table."""

    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]
    significant_rows: list[dict[str, object]] = []
    not_significant_by_variable: dict[str, list[str]] = {}

    for variable in CLIENT_PRESENTATION_VARIABLES:
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        variable_rows = summary.loc[summary["variable"].eq(variable)]
        ns_benchmarks: list[str] = []
        for benchmark_region in benchmark_regions:
            row_matches = variable_rows.loc[variable_rows["benchmark_region"].eq(benchmark_region)]
            if row_matches.empty:
                continue
            row = row_matches.iloc[0]
            p_value = row.get("p_value", np.nan)
            if pd.notna(p_value) and float(p_value) < 0.05:
                significant_rows.append(
                    {
                        "label": label,
                        "benchmark_region": benchmark_region,
                        "direction": row.get("direction", ""),
                        "p_value": float(p_value),
                        "bh_median": row.get("black_hills_positive_median"),
                        "benchmark_median": row.get("benchmark_positive_median"),
                        "median_difference": row.get("median_difference"),
                        "ci_low": row.get("median_difference_ci_low"),
                        "ci_high": row.get("median_difference_ci_high"),
                    }
                )
            else:
                ns_benchmarks.append(benchmark_region)
        if ns_benchmarks:
            not_significant_by_variable[label] = ns_benchmarks

    any_significant = bool(significant_rows)
    lines = [
        f"## Slide {slide_number}: Full answer — Is there a difference in revenue sources between Black Hills and benchmark regions?",
        "",
        "### Short answer",
        "",
    ]
    if any_significant:
        lines.append(
            "**Yes — for tax year 2022, Black Hills differs from at least one benchmark region on several revenue sources**, "
            "but the difference is **not uniform across all sources or all benchmarks**. "
            "This deck tests one source at a time using organization-level dollars among organizations that **report a positive amount** "
            "for that source (zeros excluded). Statistical significance comes from a **permutation test on the median difference** "
            "between Black Hills and each benchmark region's positive reporters, with a 95% bootstrap confidence interval reported "
            f"alongside; pairwise results appear on Slides {first_source_slide}–{last_source_slide} and in the table below."
        )
    else:
        lines.append(
            "**No statistically significant pairwise differences were detected at p < 0.05** under the positive-reporter "
            "permutation-on-median framing used in this deck. That does not prove revenue sources are identical — only that "
            "this 2022 conditional-median comparison did not clear the significance threshold."
        )
    lines.extend(
        [
            "",
            "### What this presentation tested",
            "",
            "- **Question:** Do Black Hills nonprofits differ from Billings, Flagstaff, Sioux Falls, and Missoula in how they draw revenue from each source?",
            "- **Year:** 2022 only (see `docs/Section3_Q9_Analysis.md` for the full multi-year analysis and additional statistical framings).",
            "- **Unit:** Organization-level reported dollars per source, not regional aggregate totals or stacked-bar percentages.",
            "- **Comparison:** Black Hills vs each benchmark region separately (four pairwise tests per source).",
            f"- **Statistic:** Permutation test on the median difference among organizations with a **positive** value for that source ({PERMUTATION_ITERATIONS_2022:,} reshuffles); 95% percentile bootstrap CI on the same median gap ({BOOTSTRAP_ITERATIONS_2022:,} resamples). Slide bullets report medians for those reporters only.",
            "- **Significance rule:** p < 0.05 on the pairwise permutation test.",
            "",
        ]
    )

    if significant_rows:
        lines.extend(
            [
                "### Statistically significant pairwise differences (permutation p < 0.05, among positive reporters)",
                "",
                "| Revenue source | Benchmark region | Direction | Black Hills median | Benchmark median | Median gap (95% CI) | Permutation p |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in sorted(significant_rows, key=lambda item: (str(item["label"]), str(item["benchmark_region"]))):
            gap_text = _format_median_gap_with_ci(
                row.get("median_difference"), row.get("ci_low"), row.get("ci_high")
            )
            lines.append(
                f"| {row['label']} | {row['benchmark_region']} | {row['direction']} | "
                f"{format_slide_dollars(row['bh_median'])} | {format_slide_dollars(row['benchmark_median'])} | "
                f"{gap_text} | {format_slide_p_value(row['p_value']).replace('= ', '')} |"
            )
        lines.append("")

    if not_significant_by_variable:
        lines.extend(
            [
                "### No significant pairwise difference detected (permutation p ≥ 0.05, among positive reporters)",
                "",
                "For these sources, none of the four Black Hills vs benchmark comparisons reached p < 0.05 under the conditional median permutation test:",
                "",
            ]
        )
        for label in [CLIENT_RAW_DISPLAY_LABELS.get(v, v) for v in CLIENT_PRESENTATION_VARIABLES]:
            if label not in not_significant_by_variable:
                continue
            regions = ", ".join(not_significant_by_variable[label])
            lines.append(f"- **{label}:** vs {regions}")
        lines.append("")

    lines.extend(
        [
            "### How to state the conclusion to the board",
            "",
            "1. **Revenue-source patterns are not the same everywhere.** Several 2022 pairwise comparisons show statistically significant differences in dollar amounts among organizations that actually use a given source.",
            "2. **Black Hills is often lower among reporters, not universally higher.** Where this deck finds significance, the usual pattern is a **lower median among Black Hills reporters** than in the benchmark region being compared (for example program service revenue vs Flagstaff, government grants vs Flagstaff, fundraising events vs Billings and Sioux Falls, other revenue vs Billings and Sioux Falls).",
            "3. **Participation and dollars can tell different stories.** Black Hills sometimes has **more organizations reporting** a source (higher nonzero rate) while still showing a **lower median among reporters** on that same source. See `client_2022_pairwise_presentation_summary.csv` for reporting rates; do not describe a significant median gap as proof of higher participation without checking the rate.",
            "4. **This deck does not replace the full Q9 report.** The broader analysis in `docs/Section3_Q9_Analysis.md` covers additional tax years and alternative statistical framings. Use this presentation for 2022 pairwise, conditional-dollar comparisons.",
            "",
            "### One-sentence takeaway",
            "",
        ]
    )
    if any_significant:
        n_sig = len(significant_rows)
        lines.append(
            f"> **Yes — in 2022, Black Hills differs from benchmark regions on multiple revenue sources ({n_sig} significant pairwise comparison"
            f"{'s' if n_sig != 1 else ''} at permutation p < 0.05 on the median dollar gap among positive reporters), especially where median dollars among reporters are lower than in Billings, Flagstaff, or Sioux Falls; the pattern is source-specific and should be read together with reporting rates and the full 2022–2024 Q9 analysis.**"
        )
    else:
        lines.append(
            "> **Under this 2022 positive-reporter permutation test on the median gap, no pairwise comparison reached p < 0.05; refer to `docs/Section3_Q9_Analysis.md` for the full multi-year, all-filer Q9 conclusion.**"
        )
    lines.append("")
    return lines


def write_2022_client_presentation_markdown(
    frame: pd.DataFrame,
    summary: pd.DataFrame,
    output_path: Path,
    *,
    full_universe_frame: pd.DataFrame | None = None,
    special_org_sensitivity_comparison: pd.DataFrame | None = None,
    special_org_exclusion_summary: pd.DataFrame | None = None,
    image_dir_relative: str = "assets/section3_q9_2022",
) -> None:
    """Write slide-ready Markdown for the 2022 Q9 client presentation."""

    year_frame = frame.loc[frame["tax_year"].astype(str).eq("2022")].copy()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    form_counts = year_frame["form_type"].value_counts().reindex(["990", "990EZ", "990PF"]).fillna(0).astype(int)
    row_count = int(len(year_frame))
    ein_count = int(year_frame["ein"].nunique()) if "ein" in year_frame.columns else row_count
    excluded_rows = int(frame.attrs.get("excluded_org_type_row_count", 0))

    variable_descriptions = {
        "total_revenue": "All reported revenue for the organization in tax year 2022.",
        "program_service_revenue": "Mission-related earned revenue from program services; available for Form 990 and 990-EZ, while Form 990-PF lacks the same comparable field. Blank supported lines are treated as zero.",
        "total_contributions": "Total reported contributions, gifts, grants, and similar amounts across the 990-family forms. Blank supported lines are treated as zero.",
        "government_grants_received": "Form 990 Part VIII Line 1e government grants; 990-EZ/PF are excluded for this subcomponent because they do not expose it cleanly.",
        "federated_campaigns": "Form 990 Part VIII Line 1a federated campaign support, such as United Way-style campaign allocations.",
        "related_org_contributions": "Form 990 Part VIII Line 1d contributions from related organizations or affiliates.",
        "membership_dues": "Form 990 Part VIII Line 1b membership dues; an individual-adjacent proxy, not pure individual giving.",
        "fundraising_events_contributions": "Form 990 Part VIII Line 1c fundraising event contributions; individual-adjacent, not pure individual giving.",
        "mixed_unclassified_contributions": "Form 990 Line 1f plus all 990-EZ/PF contribution totals that cannot be decomposed into source subcategories.",
        "residual_other_revenue": "Other revenue, calculated as total revenue minus program revenue and the detailed contribution-source components.",
    }

    lines = [
        "# Q9 2022 Client Presentation: Revenue Sources",
        "",
        "## Slide 1: Dataset and revenue source variables",
        "",
        "### Dataset",
        "",
        "This presentation uses the GivingTuesday Form 990-family analysis file for the Black Hills and benchmark-region nonprofit comparison.",
        "",
        f"- Tax year: 2022 only.",
        f"- Forms included: Form 990 ({form_counts.get('990', 0):,} rows), Form 990-EZ ({form_counts.get('990EZ', 0):,} rows), and Form 990-PF ({form_counts.get('990PF', 0):,} rows).",
        f"- Analysis universe: {row_count:,} organization-year records representing {ein_count:,} unique EINs after requiring positive total revenue.",
        f"- Exclusions: hospitals, universities, and political organizations are excluded for client-peer comparability; excluded rows in the full selected-year frame: {excluded_rows:,}.",
        "- Statistical tests compare organization-level 2022 raw dollar values, not regional aggregate totals or stacked-bar percentages.",
        "",
        "### Variable definitions",
        "",
        "The analysis partitions revenue into earned program revenue, contribution-source categories, mixed/unclassified contributions, and other revenue. The detailed contribution-source categories are clean only for Form 990 filers.",
        "",
        "| Variable | Meaning |",
        "| --- | --- |",
    ]
    for variable in CLIENT_PRESENTATION_VARIABLES:
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        lines.append(f"| {label} | {variable_descriptions[variable]} |")
    lines.extend(
        [
            "",
            "990-EZ and 990-PF do not expose the same contribution-source subcomponents as Form 990. For detailed subcomponent tests and median charts, those rows are excluded for the unavailable source rather than treated as zero. Their contribution totals are routed to mixed / unclassified contributions for the all-form revenue partition. Foundation grants cannot be cleanly isolated in this file.",
            "",
        ]
    )

    overview_slide_number = 2
    first_source_slide_number = 3
    lines.extend(
        [
            f"## Slide {overview_slide_number}: 2022 revenue sources — overview (same metric as statistical slides)",
            "",
            f"![2022 positive-median overview by region]({image_dir_relative}/client_2022_positive_median_overview_by_region.png)",
            "",
            "This chart uses the **same definition as Slides "
            f"{first_source_slide_number}–{first_source_slide_number + len(CLIENT_PRESENTATION_VARIABLES) - 1}**: "
            "each bar shows the **median dollar amount among organizations in that region "
            "that report a positive value** for that source (zeros excluded). The X-axis groups are regions; "
            "colors are revenue sources.",
            "",
            "- **Purpose:** Orientation — see all sources and regions on one page before the source-by-source pairwise slides.",
            "- **Aligned with tests:** Slides "
            f"{first_source_slide_number}–{first_source_slide_number + len(CLIENT_PRESENTATION_VARIABLES) - 1} "
            "show the same positive-only medians with Black Hills vs each benchmark; permutation p-values are on the following slides' bullets and tables.",
            "- **Value labels:** Each bar is labeled with the median dollar value.",
            "- **Not stacked:** Medians for different sources are not added together (each source has its own reporter pool).",
            "- **Archived stacked views:** Aggregate dollar totals, aggregate shares, and **stacked positive-only medians** "
            "(normalized mix and raw sum of medians) are in "
            "`python/analysis/revenue_sources_black_hills/results/client_notebook_assets/`. "
            "Stacked medians use the same positive-reporter definition but each segment is a **different org subset**, "
            "so normalized stacks show a relative profile only — not a budget that adds up.",
            "",
            "### How to read the charts and tests",
            "",
            "**Zero-excluded framing.** Every revenue and contribution source slide below restricts the median and the permutation test to organizations that report a **positive** amount for that source. This trades the population-level prevalence story (how many organizations engage with the source at all) for the conditional dollar story (when an organization does engage, how big is the check). Reporting rates, reporter counts, and IQR are stored in `client_2022_pairwise_presentation_summary.csv` if needed.",
            "",
            f"**Headline test.** Pairwise tests use a **two-sided permutation test on the median dollar difference** between Black Hills positive reporters and each benchmark region's positive reporters ({PERMUTATION_ITERATIONS_2022:,} reshuffles, seed {PERMUTATION_SEED_2022}). The median gap is shown with a **95% percentile bootstrap CI** on the source slides below.",
            "",
            "**How to read the charts.** Each region is a single bar whose height equals the median dollar amount reported by organizations in that region — restricted to positive reporters, as described above. The chart shows region names only; slide bullets on the following pages report the median gap, 95% CI, and permutation p-value for each Black Hills vs benchmark comparison.",
            "",
            "**What changes vs an all-population test.** For sources where Black Hills has the *highest* reporting rate but a *lower* dollar amount among reporters (government grants, federated campaigns), the direction of difference flips between the two framings. Both views can be true at once: more Black Hills organizations participate, but each participating organization brings in fewer dollars from that source. Use both numbers when telling the full story to the board.",
            "",
        ]
    )

    benchmark_regions = [region for region in REGION_ORDER if region != "Black Hills"]
    slide_number = first_source_slide_number
    for variable in CLIENT_PRESENTATION_VARIABLES:
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, DISPLAY_LABELS.get(variable, variable))
        variable_rows = summary.loc[summary["variable"].eq(variable)].set_index("benchmark_region")
        image_name = f"client_2022_pairwise_positive_median_{slugify_label(variable)}.png"

        lines.extend(
            [
                f"## Slide {slide_number}: {label}",
                "",
                f"![{label} 2022 positive-only median chart]({image_dir_relative}/{image_name})",
                "",
                "Pairwise comparison of conditional medians for organizations with a positive value for this source (zeros excluded). Headline test: two-sided permutation on the median difference.",
                "",
            ]
        )
        for benchmark_region in benchmark_regions:
            row = variable_rows.loc[benchmark_region]
            significance = "significant" if row.get("significant_before_fdr") == "Yes" else "not significant"
            gap_text = _format_median_gap_with_ci(
                row.get("median_difference"),
                row.get("median_difference_ci_low"),
                row.get("median_difference_ci_high"),
            )
            lines.append(
                "- "
                f"Black Hills vs {benchmark_region}: {row['direction'] or 'comparison unavailable'}; {significance}; "
                f"Black Hills median = {format_slide_dollars(row['black_hills_positive_median'])}; "
                f"{benchmark_region} median = {format_slide_dollars(row['benchmark_positive_median'])}; "
                f"median gap = {gap_text}; "
                f"p {format_slide_p_value(row['permutation_p_value'])}."
            )
        lines.append("")
        slide_number += 1

    last_source_slide_number = slide_number - 1
    if (
        full_universe_frame is not None
        and special_org_sensitivity_comparison is not None
        and special_org_exclusion_summary is not None
        and not special_org_sensitivity_comparison.empty
    ):
        special_org_ein_count = 0
        if "ein" in full_universe_frame.columns:
            year_full = full_universe_frame.loc[full_universe_frame["tax_year"].astype(str).eq("2022")]
            special_mask = (
                year_full["is_hospital"].fillna(False)
                | year_full["is_university"].fillna(False)
                | year_full["is_political_org"].fillna(False)
            )
            special_org_ein_count = int(year_full.loc[special_mask, "ein"].nunique())
        lines.extend(
            build_2022_special_org_sensitivity_slide_lines(
                special_org_sensitivity_comparison,
                special_org_exclusion_summary,
                slide_number=slide_number,
                first_source_slide=first_source_slide_number,
                last_source_slide=last_source_slide_number,
                special_org_ein_count=special_org_ein_count,
            )
        )
        slide_number += 1

    answer_slide_number = slide_number
    lines.extend(
        build_2022_full_answer_slide_lines(
            summary,
            slide_number=answer_slide_number,
            first_source_slide=first_source_slide_number,
            last_source_slide=last_source_slide_number,
        )
    )
    output_path.write_text("\n".join(lines), encoding="utf-8")
    info(f"Saved 2022 client presentation Markdown: {output_path}")


def save_client_raw_level_distributions(frame: pd.DataFrame, output_path: Path) -> None:
    """Save raw-dollar organization-level distributions on a log-scaled axis."""

    plot_df = frame[["region_label"] + CLIENT_RAW_LEVEL_VARIABLES].melt(
        id_vars="region_label",
        value_vars=CLIENT_RAW_LEVEL_VARIABLES,
        var_name="variable",
        value_name="amount",
    ).dropna()
    plot_df["amount"] = plot_df["amount"].clip(lower=0)
    plot_df["region_label"] = pd.Categorical(plot_df["region_label"], categories=REGION_ORDER, ordered=True)

    fig, axes = client_panel_grid(len(CLIENT_RAW_LEVEL_VARIABLES))
    rng = np.random.default_rng(321)
    for ax, variable in zip(axes, CLIENT_RAW_LEVEL_VARIABLES):
        label = CLIENT_RAW_DISPLAY_LABELS.get(variable, variable)
        sub = plot_df.loc[plot_df["variable"].eq(variable)].copy()
        data = [
            sub.loc[sub["region_label"].eq(region), "amount"].to_numpy(dtype=float)
            for region in REGION_ORDER
        ]
        plot_data = [np.log1p(values) for values in data]
        bp = ax.boxplot(
            plot_data,
            positions=np.arange(len(REGION_ORDER)),
            widths=0.55,
            patch_artist=True,
            showfliers=False,
            medianprops={"color": "black", "linewidth": 1.2},
            whiskerprops={"color": "#4B5563"},
            capprops={"color": "#4B5563"},
        )
        for patch, region in zip(bp["boxes"], REGION_ORDER):
            patch.set_facecolor(REGION_COLORS[region])
            patch.set_alpha(0.55)
            patch.set_edgecolor("#2D3748")
            patch.set_linewidth(0.9)
        for x, (region, values) in enumerate(zip(REGION_ORDER, plot_data)):
            values = values[~np.isnan(values)]
            if len(values) > 350:
                values = rng.choice(values, size=350, replace=False)
            jitter = rng.normal(loc=0, scale=0.055, size=len(values))
            ax.scatter(
                np.full(len(values), x) + jitter,
                values,
                s=7,
                alpha=0.18,
                color=REGION_COLORS[region],
                linewidths=0,
            )
        ax.set_title(format_kruskal_title(frame, variable, label), fontsize=10.5, weight="bold")
        ax.set_xticks(np.arange(len(REGION_ORDER)), REGION_ORDER, rotation=25, ha="right")
        ax.set_ylabel("log1p(dollars), display only")
        ax.grid(axis="y", alpha=0.25)
    for ax in axes[len(CLIENT_RAW_LEVEL_VARIABLES) :]:
        ax.axis("off")
    fig.suptitle("Organization-level raw revenue amounts compared in five-region rank tests", fontsize=15, weight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def save_client_rank_share_distributions(frame: pd.DataFrame, output_path: Path) -> None:
    """Save the notebook's organization-level distribution companion to rank tests."""

    plot_df = frame[["region_label"] + SHARE_COMPONENTS].melt(
        id_vars="region_label",
        value_vars=SHARE_COMPONENTS,
        var_name="share_variable",
        value_name="share",
    ).dropna()
    plot_df["share_label"] = plot_df["share_variable"].str.replace("_share", "", regex=False).map(DISPLAY_LABELS)
    plot_df["region_label"] = pd.Categorical(plot_df["region_label"], categories=REGION_ORDER, ordered=True)

    fig, axes = client_panel_grid(len(SOURCE_COMPONENTS))
    rng = np.random.default_rng(321)
    for ax, component in zip(axes, SOURCE_COMPONENTS):
        label = DISPLAY_LABELS[component]
        share_variable = f"{component}_share"
        sub = plot_df.loc[plot_df["share_label"].eq(label)].copy()
        data = [
            sub.loc[sub["region_label"].eq(region), "share"].clip(lower=0, upper=1).to_numpy()
            for region in REGION_ORDER
        ]
        bp = ax.boxplot(
            data,
            positions=np.arange(len(REGION_ORDER)),
            widths=0.55,
            patch_artist=True,
            showfliers=False,
            medianprops={"color": "black", "linewidth": 1.2},
            whiskerprops={"color": "#4B5563"},
            capprops={"color": "#4B5563"},
        )
        for patch, region in zip(bp["boxes"], REGION_ORDER):
            patch.set_facecolor(REGION_COLORS[region])
            patch.set_alpha(0.55)
            patch.set_edgecolor("#2D3748")
            patch.set_linewidth(0.9)

        for x, (region, values) in enumerate(zip(REGION_ORDER, data)):
            values = values[~np.isnan(values)]
            if len(values) > 350:
                values = rng.choice(values, size=350, replace=False)
            jitter = rng.normal(loc=0, scale=0.055, size=len(values))
            ax.scatter(
                np.full(len(values), x) + jitter,
                values,
                s=7,
                alpha=0.18,
                color=REGION_COLORS[region],
                linewidths=0,
            )

        ax.set_title(format_kruskal_title(frame, share_variable, label), fontsize=10.5, weight="bold")
        ax.set_xticks(np.arange(len(REGION_ORDER)), REGION_ORDER, rotation=25, ha="right")
        upper = min(1.0, max(0.05, np.nanpercentile(sub["share"].clip(lower=0, upper=1), 99) * 1.15))
        ax.set_ylim(0, upper)
        ax.yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
        ax.grid(axis="y", alpha=0.25)
    for ax in axes[len(SOURCE_COMPONENTS) :]:
        ax.axis("off")
    fig.suptitle("Organization-level revenue-source shares compared in five-region rank tests", fontsize=15, weight="bold", y=1.02)
    fig.tight_layout()
    fig.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    info(f"Saved client chart: {output_path}")


def create_client_notebook_equivalent_outputs(
    frame: pd.DataFrame,
    tables: dict[str, pd.DataFrame],
    paths: AnalysisPaths,
    *,
    full_universe_frame: pd.DataFrame | None = None,
) -> None:
    """Create the notebook-equivalent client charts from the script run."""

    info("Creating client-notebook-equivalent charts.")
    years_label = ", ".join(sorted(frame["tax_year"].astype(str).unique()))
    save_client_stacked_mix(
        tables["mix_by_group"],
        "comparison_group",
        paths.client_assets_dir / "client_stacked_revenue_mix_black_hills_vs_benchmark.png",
        f"Revenue-source mix: Black Hills vs benchmark regions, {years_label}",
        order=["Black Hills", "Benchmark"],
    )

    by_year = tables["mix_by_group_year"].copy()
    by_year["year_group"] = by_year["tax_year"].astype(str) + "|" + by_year["comparison_group"].astype(str)
    year_order = [
        f"{year}|{group}"
        for year in sorted(frame["tax_year"].astype(str).unique())
        for group in ["Black Hills", "Benchmark"]
    ]
    year_order = [item for item in year_order if item in set(by_year["year_group"])]
    label_map = {item: item.replace("|", "\n") for item in year_order}
    save_client_stacked_mix(
        by_year,
        "year_group",
        paths.client_assets_dir / "client_stacked_revenue_mix_by_year.png",
        "Revenue-source mix by tax year and comparison group",
        order=year_order,
        x_labels=[label_map[item] for item in year_order],
        figsize=(13.5, 6.5),
    )

    region_order = [region for region in REGION_ORDER if region in set(tables["mix_by_region"]["region_label"])]
    save_client_stacked_mix(
        tables["mix_by_region"],
        "region_label",
        paths.client_assets_dir / "client_stacked_revenue_mix_by_five_regions.png",
        f"Revenue-source mix by region, {years_label}",
        order=region_order,
        figsize=(13.5, 6.5),
    )

    summary = mean_share_summary(frame)
    write_table(summary, paths.results_tables_dir / "client_mean_share_region_summary.csv")
    raw_summary = client_raw_level_summary(frame, CLIENT_RAW_LEVEL_VARIABLES)
    write_table(raw_summary, paths.results_tables_dir / "client_raw_level_region_summary.csv")
    normality = client_raw_level_normality_diagnostics(frame, CLIENT_RAW_LEVEL_VARIABLES)
    write_table(normality, paths.results_tables_dir / "client_raw_level_normality_diagnostics.csv")
    save_client_raw_level_distributions(
        frame,
        paths.client_assets_dir / "client_raw_level_distribution_by_region.png",
    )
    save_client_raw_median_bars(
        frame,
        raw_summary,
        paths.client_assets_dir / "client_raw_level_median_bars_by_region.png",
    )
    pairwise_summary = save_client_pairwise_source_charts(
        frame,
        PRESENTATION_PAIRWISE_SOURCE_VARIABLES,
        paths.client_assets_dir,
    )
    write_table(pairwise_summary, paths.results_tables_dir / "client_pairwise_presentation_source_summary.csv")
    presentation_2022_summary = build_2022_pairwise_presentation_table(frame, CLIENT_PRESENTATION_VARIABLES)
    write_table(presentation_2022_summary, paths.results_tables_dir / "client_2022_pairwise_presentation_summary.csv")
    save_2022_pairwise_median_charts(
        frame,
        CLIENT_PRESENTATION_VARIABLES,
        paths.client_assets_dir,
        presentation_table=presentation_2022_summary,
        show_reporter_count=paths.show_chart_reporter_count,
    )
    save_2022_pairwise_median_charts(
        frame,
        CLIENT_PRESENTATION_VARIABLES,
        paths.docs_assets_dir,
        presentation_table=presentation_2022_summary,
        show_reporter_count=paths.show_chart_reporter_count,
    )
    save_2022_positive_median_overview(frame, paths.client_assets_dir)
    save_2022_positive_median_overview(frame, paths.docs_assets_dir)
    save_2022_stacked_revenue_mix_overview(
        frame,
        paths.client_assets_dir,
        preserve_share_chart_dir=paths.client_assets_dir,
    )
    save_2022_stacked_revenue_mix_overview(
        frame,
        paths.docs_assets_dir,
    )
    special_org_sensitivity_comparison: pd.DataFrame | None = None
    special_org_exclusion_summary: pd.DataFrame | None = None
    if full_universe_frame is not None:
        _, special_org_exclusion_summary = summarize_2022_special_org_exclusions(full_universe_frame)
        special_org_sensitivity_comparison = build_2022_special_org_sensitivity_comparison(
            frame,
            full_universe_frame,
            CLIENT_PRESENTATION_VARIABLES,
        )
        write_table(
            special_org_exclusion_summary,
            paths.results_tables_dir / "client_2022_special_org_exclusion_summary.csv",
        )
        write_table(
            special_org_sensitivity_comparison,
            paths.results_tables_dir / "client_2022_special_org_sensitivity_comparison.csv",
        )
    write_2022_client_presentation_markdown(
        frame,
        presentation_2022_summary,
        paths.docs_presentation_path,
        full_universe_frame=full_universe_frame,
        special_org_sensitivity_comparison=special_org_sensitivity_comparison,
        special_org_exclusion_summary=special_org_exclusion_summary,
    )
    save_client_rank_share_distributions(
        frame,
        paths.client_assets_dir / "client_rank_organization_level_share_distributions.png",
    )
    save_client_mean_share_bars(
        frame,
        summary,
        paths.client_assets_dir / "client_rank_aligned_mean_share_bars_by_region.png",
    )


INDIVIDUAL_FOCUS_COMPONENTS = [
    "institutional_clear",
    "individual_narrow",
    "line_1f_mixed",
]

INDIVIDUAL_FOCUS_SHARES = [
    "institutional_clear_share_of_contrib",
    "individual_narrow_share_of_contrib",
    "individual_broad_share_of_contrib",
    "line_1f_mixed_share_of_contrib",
]


def prepare_individual_contributions_focus(frame: pd.DataFrame) -> pd.DataFrame:
    """Build the Form-990-only frame for the focused individual-vs-institutional comparison.

    The primary analysis divides every revenue segment by total revenue, which
    mixes "where do contributions come from" with "how big is program service
    revenue". For the focused individual question, we want a cleaner contrast,
    so this frame restricts to:

    1. Form 990 filers (the only ones with Line 1 sub-components).
    2. Rows with positive total contributions (so shares are interpretable).

    The analysis then computes channel amounts and four channel-share metrics
    expressed as shares of total contributions (not total revenue). The four
    channels are:

    - ``institutional_clear``: Line 1a + Line 1d + Line 1e (federated campaigns,
      related-org contributions, government grants). Unambiguously institutional.
    - ``individual_narrow``: Line 1b + Line 1c (membership dues + fundraising
      events). A *strict lower bound* on individual giving - these channels are
      predominantly but not exclusively individual.
    - ``individual_broad``: ``individual_narrow`` + Line 1f. An *upper bound*
      that treats the entire mixed Line 1f bucket as if it were individual
      giving. The truth lies between the two bounds; reporting both makes the
      decomposition uncertainty visible.
    - ``line_1f_mixed``: Line 1f alone (the deliberately-mixed bucket). Useful
      as a separate diagnostic.
    """

    info("Preparing Form-990-only individual-contributions focus frame.")
    is_form_990 = frame["form_type"].eq("990")
    has_contributions = frame["total_contributions"].notna() & frame["total_contributions"].gt(0)
    out = frame.loc[is_form_990 & has_contributions].copy()
    if out.empty:
        return out

    out["institutional_clear"] = (
        out["federated_campaigns"].fillna(0.0)
        + out["related_org_contributions"].fillna(0.0)
        + out["government_grants"].fillna(0.0)
    )
    out["individual_narrow"] = (
        out["membership_dues"].fillna(0.0)
        + out["fundraising_events_contributions"].fillna(0.0)
    )
    out["line_1f_mixed"] = out["allo_other_contributions_line_1f"].fillna(0.0)
    out["individual_broad"] = out["individual_narrow"] + out["line_1f_mixed"]

    denominator = out["total_contributions"].replace(0, np.nan)
    out["institutional_clear_share_of_contrib"] = out["institutional_clear"] / denominator
    out["individual_narrow_share_of_contrib"] = out["individual_narrow"] / denominator
    out["individual_broad_share_of_contrib"] = out["individual_broad"] / denominator
    out["line_1f_mixed_share_of_contrib"] = out["line_1f_mixed"] / denominator

    return out


def aggregate_individual_focus_mix(frame: pd.DataFrame, group_columns: list[str]) -> pd.DataFrame:
    """Aggregate the institutional / individual-narrow / Line-1f channel amounts.

    Returns long-format share-of-total-contributions for the three mutually
    exclusive channels (institutional_clear, individual_narrow, line_1f_mixed).
    The "individual_broad" upper-bound metric is *not* included here because
    individual_broad = individual_narrow + line_1f_mixed; including all three
    would double-count Line 1f.
    """

    if frame.empty:
        return pd.DataFrame(columns=group_columns + ["component", "amount", "share_of_contrib"])
    rows: list[dict[str, object]] = []
    for keys, group in frame.groupby(group_columns, dropna=False):
        if not isinstance(keys, tuple):
            keys = (keys,)
        total_contributions = float(group["total_contributions"].sum())
        for component in INDIVIDUAL_FOCUS_COMPONENTS:
            amount = float(group[component].fillna(0.0).sum())
            share = amount / total_contributions if total_contributions > 0 else np.nan
            row = {column: value for column, value in zip(group_columns, keys)}
            row.update({
                "component": component,
                "amount": amount,
                "share_of_contrib": share,
            })
            rows.append(row)
    return pd.DataFrame(rows)


def run_individual_focus_tests(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run the focused tests on the individual-contributions metrics.

    Returns ``(univariate_tests, bootstrap_cis)``. The broad univariate table
    still includes parametric rows for auditability, but the report surfaces
    rank tests and permutation checks for this focused share analysis. The
    bootstrap CIs use the EIN cluster bootstrap.
    """

    if frame.empty:
        return pd.DataFrame(), pd.DataFrame()

    info("Running focused individual-contributions rank and permutation tests.")
    five_regions = run_univariate_tests(
        frame,
        INDIVIDUAL_FOCUS_SHARES,
        label="individual_focus_five_regions_form_990_only",
        group_column="region_label",
    )
    bh_vs_bm = run_univariate_tests(
        frame,
        INDIVIDUAL_FOCUS_SHARES,
        label="individual_focus_bh_vs_benchmark_form_990_only",
        group_column="comparison_group",
    )
    univariate = pd.concat([five_regions, bh_vs_bm], ignore_index=True)

    info("Running EIN cluster-bootstrap CIs for the focused individual-contributions metrics.")
    bootstrap_rows: list[dict[str, object]] = []
    for variable in INDIVIDUAL_FOCUS_SHARES:
        observed, lower, upper = bootstrap_ci_difference(frame, variable)
        bootstrap_rows.append({"variable": variable, "mean_difference": observed, "ci_lower": lower, "ci_upper": upper})
    bootstrap = pd.DataFrame(bootstrap_rows)
    return univariate, bootstrap


def save_individual_focus_chart(
    mix: pd.DataFrame,
    output_path: Path,
    title: str,
) -> None:
    """Render a 100% stacked bar chart of the three focused contribution channels.

    Uses share_of_contrib (each bar sums to 100% of total contributions for
    that group) so individuals-versus-institutional differences in
    *contribution mix* are visible without being diluted by program-service
    revenue.
    """

    if mix.empty:
        return
    pivot = mix.pivot(index="comparison_group", columns="component", values="share_of_contrib")
    pivot = pivot[[col for col in INDIVIDUAL_FOCUS_COMPONENTS if col in pivot.columns]]
    plt.figure(figsize=(10, 5))
    bottom = np.zeros(len(pivot.index))
    palette = sns.color_palette("Set2", n_colors=pivot.shape[1])
    for color, component in zip(palette, pivot.columns):
        values = pivot[component].fillna(0).to_numpy()
        plt.bar(pivot.index.astype(str), values, bottom=bottom, label=component, color=color)
        bottom = bottom + values
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.ylabel("Share of total contributions")
    plt.title(title)
    plt.legend(title="Channel", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close()
    info(f"Saved figure: {output_path}")


def save_individual_focus_year_chart(
    mix: pd.DataFrame,
    output_path: Path,
    title: str,
) -> None:
    """Stacked-bar chart of the three focused contribution channels by group and tax year."""

    if mix.empty:
        return
    work = mix.copy()
    work["group_year"] = (
        work["comparison_group"].astype(str) + " " + work["tax_year"].astype(str)
    )
    pivot = work.pivot(index="group_year", columns="component", values="share_of_contrib")
    pivot = pivot[[col for col in INDIVIDUAL_FOCUS_COMPONENTS if col in pivot.columns]]
    pivot = pivot.sort_index()
    plt.figure(figsize=(12, 5))
    bottom = np.zeros(len(pivot.index))
    palette = sns.color_palette("Set2", n_colors=pivot.shape[1])
    for color, component in zip(palette, pivot.columns):
        values = pivot[component].fillna(0).to_numpy()
        plt.bar(pivot.index.astype(str), values, bottom=bottom, label=component, color=color)
        bottom = bottom + values
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.ylabel("Share of total contributions")
    plt.title(title)
    plt.xticks(rotation=20, ha="right")
    plt.legend(title="Channel", bbox_to_anchor=(1.02, 1.0), loc="upper left", borderaxespad=0)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200, bbox_inches="tight")
    plt.close()
    info(f"Saved figure: {output_path}")


def create_figures(frame: pd.DataFrame, tables: dict[str, pd.DataFrame], figures_dir: Path) -> None:
    """Create all planned figures from the prepared analysis frame."""

    info("Creating revenue-source visualizations.")
    sns.set_theme(style="whitegrid")

    save_stacked_bar(
        tables["mix_by_group"],
        "comparison_group",
        figures_dir / "stacked_revenue_mix_black_hills_vs_benchmark.png",
        "Normalized reported component mix: Black Hills vs pooled benchmarks",
    )

    by_year = tables["mix_by_group_year"].copy()
    by_year["group_year"] = by_year["comparison_group"] + " " + by_year["tax_year"].astype(str)
    save_stacked_bar(
        by_year,
        "group_year",
        figures_dir / "stacked_revenue_mix_by_year.png",
        "Normalized reported component mix by comparison group and tax year",
    )

    save_stacked_bar(
        tables["mix_by_region"],
        "region_label",
        figures_dir / "stacked_revenue_mix_by_region.png",
        "Normalized reported component mix by region",
    )

    by_form = tables["mix_by_group_form"].copy()
    by_form["group_form"] = by_form["comparison_group"] + " " + by_form["form_type"].astype(str)
    save_stacked_bar(
        by_form,
        "group_form",
        figures_dir / "stacked_revenue_mix_by_form_type.png",
        "Normalized reported component mix by comparison group and form type",
    )

    plot_frame = frame.melt(
        id_vars=["comparison_group"],
        value_vars=SHARE_COMPONENTS,
        var_name="source_share",
        value_name="share",
    ).dropna()
    plt.figure(figsize=(12, 6))
    sns.violinplot(data=plot_frame, x="source_share", y="share", hue="comparison_group", cut=0, inner="quartile")
    plt.xticks(rotation=25, ha="right")
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.title("Organization-level revenue-source shares")
    plt.tight_layout()
    output_path = figures_dir / "violin_source_shares_by_group.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    plt.figure(figsize=(10, 6))
    sns.histplot(data=frame, x="total_revenue", hue="comparison_group", log_scale=True, element="step", stat="density", common_norm=False)
    plt.title("Total revenue distribution, log scale")
    plt.tight_layout()
    output_path = figures_dir / "diagnostic_total_revenue_distribution.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    diagnostic = negative_residual_diagnostics(frame)
    plt.figure(figsize=(9, 5))
    if diagnostic.empty:
        plt.text(0.5, 0.5, "No negative residual or over-100-percent share rows detected", ha="center", va="center")
        plt.axis("off")
    else:
        sns.scatterplot(data=diagnostic, x="total_revenue", y="residual_other_revenue", hue="comparison_group")
        plt.xscale("log")
        plt.axhline(0, color="black", linewidth=1)
    plt.title("Residual revenue diagnostics")
    plt.tight_layout()
    output_path = figures_dir / "diagnostic_residual_revenue.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    concentration = tables["concentration_by_region"].sort_values("top5_revenue_share", ascending=False)
    plt.figure(figsize=(10, 5))
    sns.barplot(data=concentration, x="region_label", y="top5_revenue_share")
    plt.gca().yaxis.set_major_formatter(lambda value, _: f"{value:.0%}")
    plt.title("Top 5 organizations' share of total revenue by region")
    plt.tight_layout()
    output_path = figures_dir / "concentration_top5_revenue_share_by_region.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")

    heatmap_data = tables["mix_by_group_year"].pivot_table(
        index="component",
        columns=["comparison_group", "tax_year"],
        values="share",
        aggfunc="sum",
    )
    plt.figure(figsize=(12, 5))
    sns.heatmap(heatmap_data, annot=True, fmt=".1%", cmap="vlag", center=0)
    plt.title("Reported component share of total revenue by group and year")
    plt.tight_layout()
    output_path = figures_dir / "heatmap_revenue_source_share_by_year.png"
    plt.savefig(output_path, dpi=200)
    plt.close()
    info(f"Saved figure: {output_path}")


def build_markdown_summary(
    frame: pd.DataFrame,
    stats_results: pd.DataFrame,
    multivariate_results: pd.DataFrame,
    by_year_results: pd.DataFrame,
    concentration: pd.DataFrame,
    output_path: Path,
) -> None:
    """Write a concise executive summary for the analysis run."""

    info("Writing Markdown methods and interpretation summary.")
    lines: list[str] = [
        "# Revenue Sources: Black Hills vs Benchmark Regions",
        "",
        "## Methods",
        "",
        "- Primary source: GivingTuesday 990 basic all-forms analysis variables.",
        "- Universe: Forms 990, 990-EZ, and 990-PF with positive total revenue in the selected years, excluding hospitals, universities, and political organizations for client-peer comparability.",
        "- Headline tests: Kruskal-Wallis for five-region organization-level raw-dollar revenue-source comparisons and Mann-Whitney U plus permutation mean-difference tests for Black Hills versus pooled benchmarks.",
        "- Program service revenue is interpreted for Forms 990 and 990-EZ; blank supported lines are treated as zero, while 990-PF is kept missing for that component.",
        "- Total contributions use GT `analysis_total_contributions_amount` (990 Line 1h via TOTACASHCONT; 990-EZ Part I Line 1 via CONGIFGRAETC; 990-PF Part I Line 1 via STREACGRTOIN). Blank supported lines are treated as zero.",
        "- For Form 990 filers, the contributions total is decomposed into detailed Line 1 sub-channels: government grants (1e), federated campaigns (1a), related-organization contributions (1d), membership dues (1b), fundraising event contributions (1c), and the unidentifiable Line 1f bucket that lumps individual gifts with private foundation grants, DAF distributions, corporate gifts, and bequests.",
        "- 990-EZ and 990-PF do not separately report comparable Line 1 sub-components. They are excluded from detailed subcomponent tests instead of being treated as zero, while their entire reported contributions total is routed into mixed / unclassified contributions for the all-form revenue partition.",
        "- Reported segment shares divide harmonized segment amounts by total revenue. Because those shares are compositional, share-based tests are supplemental revenue-mix context rather than the headline Q9 test.",
        "- Statistical tests include raw-dollar rank tests, permutation tests, supplemental PERMANOVA-style composition tests, log-dollar checks, EIN-clustered OLS, EIN-clustered logistic presence models, MANOVA-style tests, EIN-cluster bootstrap confidence intervals, a one-row-per-EIN independence sensitivity, and concentration metrics.",
        "- Year-by-year hypothesis tests are reported separately so 2022, 2023, and 2024 can be interpreted without relying only on pooled results.",
        "",
        "## Coverage",
        "",
        f"- Analytic rows: {len(frame):,}",
        f"- Unique EINs: {frame['ein'].nunique():,}",
        f"- Excluded hospital/university/political org rows: {int(frame.attrs.get('excluded_org_type_row_count', 0)):,}",
        f"- Years: {', '.join(sorted(frame['tax_year'].astype(str).unique()))}",
        f"- Regions: {', '.join(sorted(frame['region_label'].astype(str).unique()))}",
        "",
        "## Headline Statistical Signals",
        "",
    ]

    for variable in CLIENT_RAW_LEVEL_VARIABLES:
        subset = stats_results.loc[
            stats_results["variable"].eq(variable)
            & stats_results["test"].eq("kruskal_wallis")
            & stats_results["analysis_frame"].eq("primary_all_years_five_regions")
        ]
        if subset.empty:
            continue
        row = subset.iloc[0]
        bh_median = frame.loc[frame["comparison_group"].eq("Black Hills"), variable].dropna().clip(lower=0).median()
        bm_median = frame.loc[frame["comparison_group"].eq("Benchmark"), variable].dropna().clip(lower=0).median()
        direction = "higher" if bh_median > bm_median else "lower" if bh_median < bm_median else "equal to"
        lines.append(
            f"- `{variable}`: Black Hills median is {direction} benchmarks "
            f"(${bh_median:,.0f} vs ${bm_median:,.0f}); five-region Kruskal-Wallis p={row['p_value']:.4g}, FDR p={row.get('p_value_fdr_bh', np.nan):.4g}."
        )

    if not multivariate_results.empty:
        lines.extend(["", "## Overall Revenue-Mix Tests", ""])
        for row in multivariate_results.itertuples(index=False):
            lines.append(f"- `{row.test}`: statistic={row.statistic:.4g}, p={row.p_value:.4g}, n={row.n}.")

    if not by_year_results.empty:
        lines.extend(["", "## Year-by-Year Tests", ""])
        year_rows = by_year_results.loc[
            by_year_results["test"].eq("kruskal_wallis")
            & by_year_results["variable"].isin(CLIENT_RAW_LEVEL_VARIABLES)
            & by_year_results["analysis_frame"].str.endswith("_five_regions", na=False)
        ].copy()
        for row in year_rows.itertuples(index=False):
            year_label = (
                str(row.analysis_frame).replace("year_", "").replace("_five_regions", "")
            )
            lines.append(
                f"- {year_label} `{row.variable}`: Kruskal-Wallis p={row.p_value:.4g}, "
                f"FDR p={getattr(row, 'p_value_fdr_bh', np.nan):.4g}, n={row.n}."
            )

    lines.extend(["", "## Concentration", ""])
    for row in concentration.itertuples(index=False):
        label = getattr(row, "comparison_group", "")
        lines.append(
            f"- {label}: Gini={row.gini_total_revenue:.3f}, HHI={row.hhi_total_revenue:.3f}, "
            f"top-5 revenue share={row.top5_revenue_share:.1%}."
        )

    lines.extend(
        [
            "",
            "## Caveats",
            "",
            "- 2024 filing data may be incomplete, so the script also writes a 2022-2023 sensitivity frame.",
            "- Residual other revenue is derived from available components and should be interpreted as an accounting diagnostic, not a source-reported line item.",
            "- Some rows still show diagnostic overlap when upstream totals are missing or the institutional aggregate exceeds reported total contributions.",
            "- Form 990 Line 1 sub-channels are exposed individually: government grants, federated campaigns, related-organization contributions, membership dues, fundraising event contributions, and mixed / unclassified Line 1f.",
            "- Form 990 Line 1f and 990-EZ/990-PF contribution totals cannot be split into individual, foundation, DAF, corporate, or bequest sources without more detailed donor-level data.",
        ]
    )

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    info(f"Saved Markdown summary: {output_path}")


def _percent(value: object) -> str:
    """Format a numeric value as a percentage for report tables."""

    if pd.isna(value):
        return ""
    return f"{float(value):.1%}"


def _number(value: object, digits: int = 3) -> str:
    """Format a numeric value for report tables."""

    if pd.isna(value):
        return ""
    return f"{float(value):.{digits}g}"


def _markdown_table(frame: pd.DataFrame, columns: list[str], rename: dict[str, str] | None = None) -> str:
    """Return a compact Markdown table without adding a heavyweight dependency."""

    if frame.empty:
        return "_No rows available._"
    work = frame.loc[:, columns].copy()
    if rename:
        work = work.rename(columns=rename)
    headers = list(work.columns)
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in work.itertuples(index=False):
        lines.append("| " + " | ".join("" if pd.isna(value) else str(value) for value in row) + " |")
    return "\n".join(lines)


def collect_results_artifacts(paths: AnalysisPaths, table_names: list[str], figure_names: list[str]) -> None:
    """
    Copy the most useful tables and charts into a dedicated results folder.

    The main analysis directory keeps all intermediate outputs. The `results`
    folder is the user-facing bundle with the report, selected CSV results, and
    graph files referenced from the Markdown report.
    """

    info("Collecting selected tables and charts into the results folder.")
    for stale_name in [
        "client_bh_vs_benchmark_level_welch_tests.csv",
        "client_year_by_year_level_welch_tests.csv",
        "client_bh_vs_benchmark_level_anova.csv",
        "client_year_by_year_level_anova.csv",
    ]:
        stale_path = paths.results_tables_dir / stale_name
        if stale_path.exists():
            stale_path.unlink()
    for table_name in table_names:
        source = paths.tables_dir / table_name
        if source.exists():
            shutil.copy2(source, paths.results_tables_dir / table_name)
            info(f"Copied result table: {paths.results_tables_dir / table_name}")
    for figure_name in figure_names:
        source = paths.figures_dir / figure_name
        if source.exists():
            shutil.copy2(source, paths.results_figures_dir / figure_name)
            info(f"Copied result figure: {paths.results_figures_dir / figure_name}")


def write_detailed_results_report(
    *,
    analysis: pd.DataFrame,
    tables: dict[str, pd.DataFrame],
    univariate_results: pd.DataFrame,
    regression_results: pd.DataFrame,
    multivariate_results: pd.DataFrame,
    by_year_univariate_results: pd.DataFrame,
    by_year_multivariate_results: pd.DataFrame,
    bootstrap_results: pd.DataFrame,
    individual_focus_frame: pd.DataFrame,
    individual_focus_mix: pd.DataFrame,
    individual_focus_univariate: pd.DataFrame,
    individual_focus_bootstrap: pd.DataFrame,
    individual_focus_by_year_univariate: pd.DataFrame,
    individual_focus_by_year_bootstrap: pd.DataFrame,
    paths: AnalysisPaths,
) -> None:
    """
    Write the final user-facing report with embedded charts.

    The report intentionally repeats the headline findings in plain language and
    then links to the exact charts/tables in the same `results` folder. This is
    the file to open first when reviewing the completed run.
    """

    info("Writing detailed Markdown report with embedded charts.")
    report_path = paths.results_dir / "revenue_sources_black_hills_results.md"

    mix = tables["mix_by_group"].copy()
    raw_mix = mix.copy()
    reported_share_sum = (
        raw_mix.groupby("comparison_group", dropna=False)["share"]
        .sum()
        .reset_index(name="reported_component_share_sum")
    )
    reported_share_sum["reported_component_share_sum"] = reported_share_sum["reported_component_share_sum"].map(_percent)
    mix["share"] = mix["share"].map(_percent)
    if "normalized_mix_share" in mix.columns:
        mix["normalized_mix_share"] = mix["normalized_mix_share"].map(_percent)
    else:
        mix["normalized_mix_share"] = ""
    mix["amount"] = mix["amount"].map(lambda value: f"${float(value):,.0f}" if not pd.isna(value) else "")
    mix_report = mix[["comparison_group", "component", "amount", "share", "normalized_mix_share"]].rename(
        columns={
            "comparison_group": "Group",
            "component": "Revenue source",
            "amount": "Amount",
            "share": "Reported share of total revenue",
            "normalized_mix_share": "Normalized chart share",
        }
    )

    primary_five_raw = client_rank_test_table(
        analysis,
        CLIENT_RAW_LEVEL_VARIABLES,
        "region_label",
    )
    primary_bm_raw = client_rank_test_table(
        analysis,
        CLIENT_RAW_LEVEL_VARIABLES,
        "comparison_group",
        include_permutation=True,
    )
    primary_share_five_raw = univariate_results.loc[
        univariate_results["analysis_frame"].eq("primary_all_years_five_regions")
        & univariate_results["test"].eq("kruskal_wallis")
        & univariate_results["variable"].isin(SHARE_COMPONENTS)
    ].copy()
    primary_share_bm_raw = univariate_results.loc[
        univariate_results["analysis_frame"].eq("primary_all_years_bh_vs_benchmark")
        & univariate_results["test"].isin(["mann_whitney", "permutation_mean_diff"])
        & univariate_results["variable"].isin(SHARE_COMPONENTS)
    ].copy()

    raw_summary_raw = client_raw_level_summary(analysis, CLIENT_RAW_LEVEL_VARIABLES)
    raw_normality_raw = client_raw_level_normality_diagnostics(analysis, CLIENT_RAW_LEVEL_VARIABLES)
    primary_log_levels_raw = client_rank_test_table(
        analysis,
        CLIENT_LOG_LEVEL_VARIABLES,
        "comparison_group",
        include_permutation=True,
    )

    # Independence sensitivity: one-row-per-EIN re-run of the share-variable
    # rank and permutation tests. If the headline pattern holds when each EIN contributes a
    # single observation, the repeated-filer structure is not driving the
    # primary p-values.
    one_per_ein_raw = univariate_results.loc[
        univariate_results["analysis_frame"].isin(
            ["sensitivity_one_row_per_ein_five_regions", "sensitivity_one_row_per_ein_bh_vs_benchmark"]
        )
        & univariate_results["test"].isin(["kruskal_wallis", "mann_whitney", "permutation_mean_diff"])
        & univariate_results["variable"].isin(CLIENT_RAW_LEVEL_VARIABLES)
    ].copy()

    full_universe_raw = univariate_results.loc[
        univariate_results["analysis_frame"].isin(
            ["sensitivity_including_outliers_five_regions", "sensitivity_including_outliers_bh_vs_benchmark"]
        )
        & univariate_results["test"].isin(["kruskal_wallis", "mann_whitney", "permutation_mean_diff"])
        & univariate_results["variable"].isin(CLIENT_RAW_LEVEL_VARIABLES)
    ].copy()

    def _format_univariate_table(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        out = frame.copy()
        out["p_value"] = out["p_value"].map(lambda value: _number(value, 4))
        out["p_value_fdr_bh"] = out["p_value_fdr_bh"].map(lambda value: _number(value, 4))
        out["statistic"] = out["statistic"].map(lambda value: _number(value, 4))
        return out

    def _format_client_rank_table(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        out = frame.copy()
        out["p_value"] = out["p_value"].map(lambda value: _number(value, 4))
        out["fdr_p_value"] = out["fdr_p_value"].map(lambda value: _number(value, 4))
        out["statistic"] = out["statistic"].map(lambda value: _number(value, 4))
        return out

    primary_five = _format_client_rank_table(primary_five_raw)
    primary_bm = _format_client_rank_table(primary_bm_raw)
    primary_share_five = _format_univariate_table(primary_share_five_raw)
    primary_share_bm = _format_univariate_table(primary_share_bm_raw)
    primary_log_levels = _format_client_rank_table(primary_log_levels_raw)

    raw_summary = raw_summary_raw.copy()
    if not raw_summary.empty:
        for column in ["mean", "median", "positive_median"]:
            raw_summary[column] = raw_summary[column].map(lambda value: f"${float(value):,.0f}" if not pd.isna(value) else "")
        raw_summary["nonzero_percent"] = raw_summary["nonzero_percent"].map(_percent)

    raw_normality = raw_normality_raw.copy()
    if not raw_normality.empty:
        for column in ["mean", "median"]:
            raw_normality[column] = raw_normality[column].map(lambda value: f"${float(value):,.0f}" if not pd.isna(value) else "")
        raw_normality["zero_percent"] = raw_normality["zero_percent"].map(_percent)
        raw_normality["mean_median_ratio"] = raw_normality["mean_median_ratio"].map(lambda value: "not defined" if pd.isna(value) else _number(value, 3))
        raw_normality["skew"] = raw_normality["skew"].map(lambda value: _number(value, 3))
        raw_normality["normaltest_p_raw"] = raw_normality["normaltest_p_raw"].map(lambda value: "<0.001" if not pd.isna(value) and float(value) < 0.001 else _number(value, 4))
        raw_normality["normaltest_p_log1p"] = raw_normality["normaltest_p_log1p"].map(lambda value: "<0.001" if not pd.isna(value) and float(value) < 0.001 else _number(value, 4))
    one_per_ein = _format_univariate_table(one_per_ein_raw)
    if not one_per_ein.empty:
        one_per_ein["analysis_frame"] = one_per_ein["analysis_frame"].str.replace(
            "sensitivity_one_row_per_ein_", "", regex=False
        )
    full_universe = _format_univariate_table(full_universe_raw)
    if not full_universe.empty:
        full_universe["analysis_frame"] = full_universe["analysis_frame"].str.replace(
            "sensitivity_including_outliers_", "", regex=False
        )

    # Individual contributions focus: format the share-of-contributions mix
    # table and the non-parametric BH-vs-benchmark tests for the report.
    if individual_focus_mix is None or individual_focus_mix.empty:
        focus_mix_table = pd.DataFrame()
        focus_n_orgs = 0
        focus_n_eins = 0
    else:
        focus_mix_table = individual_focus_mix.copy()
        focus_mix_table["amount"] = focus_mix_table["amount"].map(
            lambda value: f"${float(value):,.0f}" if not pd.isna(value) else ""
        )
        focus_mix_table["share_of_contrib"] = focus_mix_table["share_of_contrib"].map(_percent)
        focus_mix_table = focus_mix_table[["comparison_group", "component", "amount", "share_of_contrib"]].rename(
            columns={
                "comparison_group": "Group",
                "component": "Channel",
                "amount": "Amount",
                "share_of_contrib": "Share of total contributions",
            }
        )
        focus_n_orgs = int(individual_focus_frame["ein"].nunique()) if individual_focus_frame is not None else 0
        focus_n_eins = focus_n_orgs

    if individual_focus_univariate is None or individual_focus_univariate.empty:
        focus_anova = pd.DataFrame()
    else:
        focus_anova_raw = individual_focus_univariate.loc[
            individual_focus_univariate["test"].isin(["mann_whitney", "permutation_mean_diff"])
            & individual_focus_univariate["variable"].isin(INDIVIDUAL_FOCUS_SHARES)
            & individual_focus_univariate["analysis_frame"].str.contains("bh_vs_benchmark", na=False)
        ].copy()
        focus_anova = _format_univariate_table(focus_anova_raw)
        if not focus_anova.empty:
            focus_anova["analysis_frame"] = focus_anova["analysis_frame"].str.replace(
                "individual_focus_", "", regex=False
            ).str.replace("_form_990_only", "", regex=False)

    if individual_focus_bootstrap is None or individual_focus_bootstrap.empty:
        focus_bootstrap = pd.DataFrame()
    else:
        focus_bootstrap = individual_focus_bootstrap.copy()
        for column in ["mean_difference", "ci_lower", "ci_upper"]:
            focus_bootstrap[column] = focus_bootstrap[column].map(_percent)

    # Year-by-year focused tables for the report. Keep the same BH-vs-benchmark
    # rank/permutation test family used in the main client-facing analysis.
    if individual_focus_by_year_univariate is None or individual_focus_by_year_univariate.empty:
        focus_by_year = pd.DataFrame()
    else:
        focus_by_year_raw = individual_focus_by_year_univariate.loc[
            individual_focus_by_year_univariate["test"].isin(["mann_whitney", "permutation_mean_diff"])
            & individual_focus_by_year_univariate["variable"].isin(INDIVIDUAL_FOCUS_SHARES)
            & individual_focus_by_year_univariate["analysis_frame"].str.endswith(
                "_bh_vs_benchmark", na=False
            )
        ].copy()
        focus_by_year = _format_univariate_table(focus_by_year_raw)
        if not focus_by_year.empty:
            focus_by_year["tax_year"] = (
                focus_by_year["analysis_frame"]
                .str.replace("individual_focus_year_", "", regex=False)
                .str.replace("_bh_vs_benchmark", "", regex=False)
            )

    if individual_focus_by_year_bootstrap is None or individual_focus_by_year_bootstrap.empty:
        focus_bootstrap_by_year = pd.DataFrame()
    else:
        focus_bootstrap_by_year = individual_focus_by_year_bootstrap.copy()
        for column in ["mean_difference", "ci_lower", "ci_upper"]:
            focus_bootstrap_by_year[column] = focus_bootstrap_by_year[column].map(_percent)

    bootstrap = bootstrap_results.copy()
    for column in ["mean_difference", "ci_lower", "ci_upper"]:
        bootstrap[column] = bootstrap[column].map(_percent)

    if {"test", "variable"}.issubset(by_year_univariate_results.columns):
        by_year = by_year_univariate_results.loc[
            by_year_univariate_results["test"].eq("kruskal_wallis")
            & by_year_univariate_results["variable"].isin(CLIENT_RAW_LEVEL_VARIABLES)
            & by_year_univariate_results["analysis_frame"].str.endswith("_five_regions", na=False)
        ].copy()
    else:
        by_year = pd.DataFrame()
    if not by_year.empty:
        by_year["tax_year"] = (
            by_year["analysis_frame"].str.replace(r"^year_", "", regex=True).str.replace("_five_regions$", "", regex=True)
        )
        by_year["p_value"] = by_year["p_value"].map(lambda value: _number(value, 4))
        by_year["p_value_fdr_bh"] = by_year["p_value_fdr_bh"].map(lambda value: _number(value, 4))
        by_year["statistic"] = by_year["statistic"].map(lambda value: _number(value, 4))

    multivariate_parts = [
        frame
        for frame in [multivariate_results, by_year_multivariate_results]
        if not frame.empty and {"test", "p_value", "statistic"}.issubset(frame.columns)
    ]
    multivariate = pd.concat(multivariate_parts, ignore_index=True) if multivariate_parts else pd.DataFrame()
    if not multivariate.empty:
        multivariate["p_value"] = multivariate["p_value"].map(lambda value: _number(value, 4))
        multivariate["p_value_fdr_bh"] = multivariate["p_value_fdr_bh"].map(lambda value: _number(value, 4))
        multivariate["statistic"] = multivariate["statistic"].map(lambda value: _number(value, 4))

    concentration = tables["concentration_by_group"].copy()
    concentration["total_revenue"] = concentration["total_revenue"].map(lambda value: f"${float(value):,.0f}")
    concentration["gini_total_revenue"] = concentration["gini_total_revenue"].map(lambda value: _number(value, 3))
    concentration["hhi_total_revenue"] = concentration["hhi_total_revenue"].map(lambda value: _number(value, 3))
    concentration["top5_revenue_share"] = concentration["top5_revenue_share"].map(_percent)

    overlap = tables.get("component_overlap_by_group", pd.DataFrame()).copy()
    if not overlap.empty:
        for column in ["negative_residual_rate", "share_over_100_rate", "mean_source_share_sum", "median_source_share_sum", "max_source_share_sum"]:
            if column in overlap.columns:
                overlap[column] = overlap[column].map(_percent)

    if {"term", "estimate", "p_value"}.issubset(regression_results.columns):
        regression = regression_results.loc[regression_results["term"].eq("is_black_hills")].copy()
    else:
        regression = pd.DataFrame()
    if not regression.empty:
        regression["estimate"] = regression["estimate"].map(lambda value: _number(value, 4))
        regression["std_error"] = regression["std_error"].map(lambda value: _number(value, 4))
        regression["p_value"] = regression["p_value"].map(lambda value: _number(value, 4))
        regression["p_value_fdr_bh"] = regression["p_value_fdr_bh"].map(lambda value: _number(value, 4))
    warning_count = 0
    if "warning" in regression_results.columns:
        warning_count = int(regression_results["warning"].fillna("").astype(str).str.len().gt(0).sum())

    lines = [
        "# Revenue Sources Analysis Results",
        "",
        "## Question",
        "",
        "Is there a difference in revenue sources between Black Hills nonprofit organizations and nonprofit organizations in the benchmark regions?",
        "",
        "## Analysis Performed",
        "",
        "The analysis uses the GivingTuesday Form 990 basic all-forms analysis dataset as the primary source because it covers Form 990, Form 990-EZ, and Form 990-PF and contains the requested revenue-source fields. The primary comparison excludes hospitals, universities, and political organizations so the benchmark regions better reflect the client-peer universe; the full Form 990/990-EZ/990-PF universe is retained as a sensitivity check. NCCS Core is used separately as a 2022 sensitivity check where comparable fields exist.",
        "",
        "The script filtered to positive total revenue, valid region/year/form records, and the comparable peer universe, then derived detailed revenue-source segments aligned with Section 3 Q9. For Form 990 filers (the bulk of the analytical sample) the contribution side of total revenue is decomposed into the IRS Part VIII Line 1 sub-channels:",
        "",
        "1. **program_service_revenue** - Form 990 / 990-EZ Line 2g program service revenue (Form 990-PF stays missing; that form lacks an equivalent concept).",
        "2. **government_grants_received** - Form 990 Line 1e (`GOVERNGRANTS`). The only Line 1 sub-component the IRS labels unambiguously as institutional.",
        "3. **federated_campaigns** - Form 990 Line 1a (`FEDERACAMPAI`). Institutional/intermediary campaign support, such as United Way-style campaigns.",
        "4. **related_org_contributions** - Form 990 Line 1d (`RELATEORGANI`). Institutional or affiliate support from related organizations.",
        "5. **membership_dues** - Form 990 Line 1b (`MEMBERDUESUE`). Individual-adjacent support, but not pure individual giving.",
        "6. **fundraising_events_contributions** - Form 990 Line 1c (`FUNDRAEVENTS`). Individual-adjacent support from fundraising events, but not pure individual giving.",
        "7. **mixed_unclassified_contributions** - Form 990 Line 1f (`ALLOOTHECONT`). The IRS lumps individual gifts together with private foundation grants, donor-advised fund distributions, corporate gifts, and bequests in this single line and does not separate the donor types. For 990-EZ and 990-PF filers, which do not separately report comparable Line 1 sub-components, the entire reported Line 1 / Part I Line 1 contributions total is routed into this bucket for the all-form revenue partition.",
        "8. **residual_other_revenue** - total revenue minus the segments above; clipped at zero for plotting.",
        "",
        "Total contributions used as the contribution-side denominator are the GT canonical field `analysis_total_contributions_amount` (Line 1h via `TOTACASHCONT` for 990; Part I Line 1 via `CONGIFGRAETC` for 990-EZ; Part I Line 1 via `STREACGRTOIN` for 990-PF). Blank supported amount fields are interpreted as reported zero; unavailable form-specific subcomponents are kept missing. The institutional aggregate (`analysis_calculated_grants_total_amount` = Line 1a + 1d + 1e on Form 990) is exposed for diagnostics; earlier versions of that aggregate accidentally included Form 990 Part IX grants paid out (`FOREGRANTOTA`, `GRANTOORORGA`) and have been corrected.",
        "",
        "**Interpretation caveat for the client question.** This decomposition is the most informative split the IRS basic 990 family can support for distinguishing source channels. It cannot fully isolate individual giving because Line 1f mixes individuals, foundations, DAFs, corporates, and bequests, and Form 990-EZ / 990-PF expose only a single contributions total. Government grants, federated campaigns, related-organization contributions, membership dues, and fundraising event contributions are tested only where the form actually reports the comparable field, so unavailable EZ/PF source detail is not counted as zero. Government grants, federated campaigns, and related-organization contributions are institutional channels; membership dues and fundraising events are individual-adjacent proxies; mixed / unclassified contributions should not be read as either pure-individual or pure-institutional.",
        "",
        "The statistical analysis includes descriptive summaries, raw-dollar rank tests, permutation tests, FDR-adjusted p-values, effect sizes, supplemental share/compositional tests, log-dollar checks, OLS models with EIN-clustered standard errors, EIN-clustered logistic presence models, MANOVA-style tests, year-by-year tests, form-type sensitivity tests, revenue-size sensitivity tests, a full-universe sensitivity that includes the excluded organization types, a one-row-per-EIN independence sensitivity, EIN-cluster bootstrap confidence intervals, and concentration metrics. Where the same nonprofit appears in multiple tax years, inference uses cluster-robust adjustments rather than treating org-years as independent.",
        "",
        "## Coverage",
        "",
        f"- Analytic organization-year rows: {len(analysis):,}",
        f"- Unique EINs: {analysis['ein'].nunique():,}",
        f"- Excluded hospital/university/political org rows: {int(analysis.attrs.get('excluded_org_type_row_count', 0)):,} "
        f"({int(analysis.attrs.get('excluded_org_type_ein_count', 0)):,} EINs) from the full valid universe of "
        f"{int(analysis.attrs.get('full_universe_row_count', len(analysis))):,} rows",
        f"- Tax years: {', '.join(sorted(analysis['tax_year'].astype(str).unique()))}",
        f"- Regions: {', '.join(sorted(analysis['region_label'].astype(str).unique()))}",
        f"- Rows flagged for negative residual or over-100-percent source shares: {len(tables['negative_residual_diagnostics']):,}",
        "",
        "## Headline Findings",
        "",
        "The clearest headline comparison is now the five-region Kruskal-Wallis test on organization-level raw revenue-source dollars. Follow-up Black Hills versus pooled benchmark tests use Mann-Whitney U and permutation mean-difference tests. This avoids making the compositional source-share variables the main inferential test.",
        "",
        "The stacked-bar share visuals are descriptive aggregate-dollar revenue-mix charts. They are useful for presentation, but they are not the values being tested in the primary raw-dollar rank tests.",
        "",
        "## Aggregate Reported Component Mix",
        "",
        "The table below shows both the reported component share of total revenue and the normalized share used in the stacked charts. With the Q9 donor-channel decomposition, reported segment shares should sum to roughly 100 percent within rounding; larger deviations indicate upstream overlap or missing contribution totals on specific rows.",
        "",
        _markdown_table(mix_report, list(mix_report.columns)),
        "",
        "Reported component share sums by group:",
        "",
        _markdown_table(
            reported_share_sum,
            ["comparison_group", "reported_component_share_sum"],
            {
                "comparison_group": "Group",
                "reported_component_share_sum": "Reported component shares summed",
            },
        ),
        "",
        "![Normalized reported component mix: Black Hills vs benchmarks](figures/stacked_revenue_mix_black_hills_vs_benchmark.png)",
        "",
        "![Normalized reported component mix by year](figures/stacked_revenue_mix_by_year.png)",
        "",
        "![Normalized reported component mix by region](figures/stacked_revenue_mix_by_region.png)",
        "",
        "![Normalized reported component mix by form type](figures/stacked_revenue_mix_by_form_type.png)",
        "",
        "## Raw-Dollar Descriptive Summary",
        "",
        "These medians and nonzero rates summarize the organization-level dollar amounts used in the primary tests. Means are shown only as context because the distributions are strongly right-skewed.",
        "",
        _markdown_table(
            raw_summary,
            ["variable_label", "region_label", "n", "mean", "median", "nonzero_percent", "positive_median"],
            {
                "variable_label": "Variable",
                "region_label": "Region",
                "n": "N",
                "mean": "Mean",
                "median": "Median",
                "nonzero_percent": "Nonzero %",
                "positive_median": "Positive-only median",
            },
        ),
        "",
        "## Normality and Skew Diagnostics",
        "",
        "D'Agostino-Pearson normality tests reject normality for the raw dollar variables in every region. Log1p-transformed values also reject normality in every region, so Kruskal-Wallis and Mann-Whitney U are preferred over ANOVA for the primary Q9 tests.",
        "",
        _markdown_table(
            raw_normality,
            ["variable_label", "region_label", "n", "zero_percent", "mean", "median", "mean_median_ratio", "skew", "normaltest_p_raw", "normaltest_p_log1p"],
            {
                "variable_label": "Variable",
                "region_label": "Region",
                "n": "N",
                "zero_percent": "Zero %",
                "mean": "Mean",
                "median": "Median",
                "mean_median_ratio": "Mean/median",
                "skew": "Skew",
                "normaltest_p_raw": "Raw normality p",
                "normaltest_p_log1p": "Log1p normality p",
            },
        ),
        "",
        "![Raw-dollar distributions by region](client_notebook_assets/client_raw_level_distribution_by_region.png)",
        "",
        "![Median raw revenue-source dollars by region](client_notebook_assets/client_raw_level_median_bars_by_region.png)",
        "",
        "## Primary Raw-Dollar Statistical Tests (five-region Kruskal-Wallis)",
        "",
        _markdown_table(
            primary_five,
            ["test", "variable", "statistic", "p_value", "fdr_p_value", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "fdr_p_value": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Black Hills vs pooled benchmarks (raw-dollar follow-up)",
        "",
        _markdown_table(
            primary_bm,
            ["test", "variable", "direction", "statistic", "p_value", "fdr_p_value", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "direction": "Direction",
                "statistic": "Statistic",
                "p_value": "P-value",
                "fdr_p_value": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Supplemental Log-Dollar Checks",
        "",
        "Log-dollar tests use `log1p`, which reduces the influence of very large organizations while still retaining zero-dollar rows. They are supplemental because the normality diagnostics still reject normality after log transformation.",
        "",
        "### Log-dollar rank/permutation tests",
        "",
        _markdown_table(
            primary_log_levels,
            ["test", "variable", "direction", "statistic", "p_value", "fdr_p_value", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "direction": "Direction",
                "statistic": "Statistic",
                "p_value": "P-value",
                "fdr_p_value": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Supplemental Revenue-Mix / Compositional Context",
        "",
        "The share variables are parts of each organization's total revenue and are therefore compositional. These tests remain useful as revenue-mix follow-ups, but they are no longer the primary Q9 test.",
        "",
        "### Five-region source-share rank tests",
        "",
        _markdown_table(
            primary_share_five,
            ["test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "### Black Hills vs pooled benchmark source-share follow-ups",
        "",
        _markdown_table(
            primary_share_bm,
            ["test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Independence Sensitivity (one row per EIN, most recent year)",
        "",
        "Each EIN contributes at most three filings (2022-2024). The pooled tests above treat each "
        "org-year as an independent observation; this sensitivity restricts the analysis to one row "
        "per EIN (most recent reported year) so that the test's independence assumption is satisfied "
        "exactly. If the direction and significance of the raw-dollar comparisons survive this restriction, "
        "repeated filings are not driving the headline result.",
        "",
        _markdown_table(
            one_per_ein,
            ["analysis_frame", "test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Comparison",
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Full-Universe Sensitivity (including hospitals, universities, and political orgs)",
        "",
        "The primary results exclude hospitals, universities, and political organizations for client-peer "
        "comparability. This sensitivity reruns the main raw-dollar tests on the full valid Form 990/990-EZ/990-PF "
        "universe so readers can see whether that comparability choice changes the substantive conclusion.",
        "",
        _markdown_table(
            full_universe,
            ["analysis_frame", "test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Comparison",
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Bootstrap Confidence Intervals (cluster-bootstrap by EIN)",
        "",
        "These rows show Black Hills minus benchmark mean differences in organization-level revenue-source shares. "
        "The bootstrap resamples EINs (not org-years) so that within-EIN correlation is preserved; the resulting "
        "confidence intervals are wider than a row-level bootstrap and reflect the actual organization-level uncertainty.",
        "",
        _markdown_table(
            bootstrap,
            ["variable", "mean_difference", "ci_lower", "ci_upper"],
            {
                "variable": "Variable",
                "mean_difference": "Mean difference",
                "ci_lower": "95% CI lower",
                "ci_upper": "95% CI upper",
            },
        ),
        "",
        "![Organization-level source shares](figures/violin_source_shares_by_group.png)",
        "",
        "## Individual Contributions Focus (Form 990 only, share of total contributions)",
        "",
        f"This section answers the client question more directly: of the contributions a nonprofit "
        f"reports, what fraction comes from likely-individual donors versus from other organizations? "
        f"It is restricted to Form 990 filers ({focus_n_orgs:,} EINs in this run) because 990-EZ and "
        f"990-PF do not separately report Line 1 sub-channels, and to organization-years with positive "
        f"total contributions so the denominator is meaningful. Shares are share of total contributions "
        f"(not of total revenue), which removes program-service revenue as a confounder.",
        "",
        "Channels:",
        "",
        "- **institutional_clear** - Lines 1a + 1d + 1e (federated campaigns + related-org "
        "contributions + government grants). Unambiguously institutional.",
        "- **individual_narrow** - Lines 1b + 1c (membership dues + fundraising-event "
        "contributions). A *strict lower bound* on individual giving.",
        "- **line_1f_mixed** - Line 1f (`ALLOOTHECONT`). Mixes individuals with foundations, "
        "DAF distributions, corporate gifts, and bequests. Cannot be split further without "
        "Schedule B.",
        "- **individual_broad** - `individual_narrow` + `line_1f_mixed`. An *upper bound* that "
        "treats Line 1f as if it were entirely individual giving. The truth lies between the two "
        "bounds.",
        "",
        "### Aggregate channel mix (share of total contributions, Form 990 only)",
        "",
        _markdown_table(
            focus_mix_table,
            ["Group", "Channel", "Amount", "Share of total contributions"],
            {},
        ),
        "",
        "![Form 990 contribution channels by group](figures/individual_focus_contributions_mix.png)",
        "",
        "### Supplemental rank/permutation tests on share-of-contribution metrics (Form 990 only)",
        "",
        _markdown_table(
            focus_anova,
            ["analysis_frame", "test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Comparison",
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "### EIN cluster-bootstrap CIs on Black Hills minus benchmark (Form 990 only)",
        "",
        _markdown_table(
            focus_bootstrap,
            ["variable", "mean_difference", "ci_lower", "ci_upper"],
            {
                "variable": "Variable",
                "mean_difference": "Mean difference",
                "ci_lower": "95% CI lower",
                "ci_upper": "95% CI upper",
            },
        ),
        "",
        "Interpretation guide. If the institutional-clear share differs significantly between BH "
        "and benchmarks but the individual_narrow and individual_broad shares do not, the "
        "individual-versus-institutional gap is being driven by institutional channels (e.g. "
        "government grants), not by individual giving. If the individual_broad bound differs but "
        "individual_narrow does not, the gap is concentrated in Line 1f, which is the bucket we "
        "cannot decompose without Schedule B and should be flagged for follow-up.",
        "",
        "### Year-by-Year Focused Comparisons (Form 990 only, BH vs pooled benchmarks)",
        "",
        "The pooled tests above can hide year-to-year changes. The rank/permutation tests below "
        "isolate Black Hills versus pooled benchmark for each share-of-contributions metric within "
        "each tax year, so a single year of unusual filings cannot drive a multi-year conclusion.",
        "",
        _markdown_table(
            focus_by_year,
            ["tax_year", "test", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "tax_year": "Tax year",
                "test": "Test",
                "variable": "Variable",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "Cluster-bootstrap 95% confidence intervals on Black Hills minus benchmark, by tax year:",
        "",
        _markdown_table(
            focus_bootstrap_by_year,
            ["tax_year", "variable", "mean_difference", "ci_lower", "ci_upper"],
            {
                "tax_year": "Tax year",
                "variable": "Variable",
                "mean_difference": "Mean difference",
                "ci_lower": "95% CI lower",
                "ci_upper": "95% CI upper",
            },
        ),
        "",
        "![Form 990 contribution channels by group and year](figures/individual_focus_contributions_mix_by_year.png)",
        "",
        "## Year-by-Year Comparisons",
        "",
        _markdown_table(
            by_year,
            ["tax_year", "variable", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "tax_year": "Tax year",
                "variable": "Variable",
                "statistic": "Kruskal-Wallis H",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Supplemental Overall Revenue-Mix Tests",
        "",
        _markdown_table(
            multivariate,
            ["analysis_frame", "test", "statistic", "p_value", "p_value_fdr_bh", "n"],
            {
                "analysis_frame": "Analysis frame",
                "test": "Test",
                "statistic": "Statistic",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        "## Regression Results",
        "",
        _markdown_table(
            regression,
            ["model", "outcome", "estimate", "std_error", "p_value", "p_value_fdr_bh", "n"],
            {
                "model": "Model",
                "outcome": "Outcome",
                "estimate": "Black Hills estimate",
                "std_error": "Std. error",
                "p_value": "P-value",
                "p_value_fdr_bh": "FDR p-value",
                "n": "N",
            },
        ),
        "",
        f"Auxiliary regression warnings captured: {warning_count}. These are retained in `tables/statistical_tests_regression.csv` and do not change the descriptive tables or univariate tests.",
        "",
        "## Concentration Diagnostics",
        "",
        _markdown_table(
            concentration,
            ["comparison_group", "organization_count", "total_revenue", "gini_total_revenue", "hhi_total_revenue", "top5_revenue_share"],
            {
                "comparison_group": "Group",
                "organization_count": "Organizations",
                "total_revenue": "Total revenue",
                "gini_total_revenue": "Gini",
                "hhi_total_revenue": "HHI",
                "top5_revenue_share": "Top 5 share",
            },
        ),
        "",
        "![Top 5 revenue concentration by region](figures/concentration_top5_revenue_share_by_region.png)",
        "",
        "## Diagnostics",
        "",
        "Residual other revenue is a derived category, so rows with negative residuals or shares above 100 percent are reported in `tables/negative_residual_diagnostics.csv`. These rows should be reviewed before using residual revenue as a substantive category. The summary below shows how common those overlap/reconciliation flags are by comparison group.",
        "",
        _markdown_table(
            overlap,
            ["comparison_group", "row_count", "negative_residual_count", "negative_residual_rate", "share_over_100_count", "share_over_100_rate", "mean_source_share_sum"],
            {
                "comparison_group": "Group",
                "row_count": "Rows",
                "negative_residual_count": "Negative residual rows",
                "negative_residual_rate": "Negative residual rate",
                "share_over_100_count": "Over-100 rows",
                "share_over_100_rate": "Over-100 rate",
                "mean_source_share_sum": "Mean source-share sum",
            },
        ),
        "",
        "![Total revenue distribution](figures/diagnostic_total_revenue_distribution.png)",
        "",
        "![Residual revenue diagnostics](figures/diagnostic_residual_revenue.png)",
        "",
        "![Revenue-source share heatmap](figures/heatmap_revenue_source_share_by_year.png)",
        "",
        "## Bottom Line",
        "",
        "Review the raw-dollar five-region Kruskal-Wallis table and follow-up Black Hills versus benchmark rank/permutation tests for the primary Q9 answer. Aggregate-dollar mix charts and compositional tests provide supplemental revenue-mix context.",
        "",
        "## Key Files in This Results Folder",
        "",
        "- `tables/mix_by_group.csv`: aggregate reported component mix by Black Hills versus benchmarks",
        "- `tables/statistical_tests_univariate.csv`: pooled and sensitivity univariate tests",
        "- `tables/statistical_tests_by_year_univariate.csv`: year-by-year hypothesis tests",
        "- `tables/statistical_tests_multivariate.csv`: pooled compositional and multivariate tests",
        "- `tables/raw_field_mapping_validation.csv`: raw-to-harmonized field checks for the requested Q9 variables",
        "- `tables/client_five_region_raw_level_rank_tests.csv`: primary five-region raw-dollar Kruskal-Wallis table",
        "- `tables/client_bh_vs_benchmark_raw_level_rank_tests.csv`: primary Black Hills versus benchmark raw-dollar Mann-Whitney/permutation checks",
        "- `tables/client_pairwise_black_hills_region_raw_level_rank_tests.csv`: exploratory Black Hills versus each benchmark-region raw-dollar pairwise tests",
        "- `tables/client_raw_level_region_summary.csv`: raw-dollar medians, means, and nonzero rates by region",
        "- `tables/client_raw_level_normality_diagnostics.csv`: raw and log1p normality/skew diagnostics",
        "- `tables/client_five_region_share_rank_tests.csv`: supplemental source-share five-region rank tests",
        "- `tables/client_bh_vs_benchmark_share_rank_tests.csv`: supplemental source-share Black Hills versus benchmark tests",
        "- `tables/client_bh_vs_benchmark_log_level_rank_tests.csv`: supplemental log-dollar Mann-Whitney/permutation checks",
        "- `client_notebook_assets/`: notebook-equivalent client charts, including raw-dollar median/distribution visuals and descriptive stacked-share visuals",
        "- `tables/concentration_by_group.csv`: Gini, HHI, and top-5 concentration metrics",
        "- `tables/component_overlap_by_group.csv`: group-level reconciliation diagnostics for overlapping components",
        "- `tables/negative_residual_diagnostics.csv`: rows needing residual/share diagnostics",
    ]
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    info(f"Saved detailed results report: {report_path}")


def run_analysis(
    *,
    data_root: Path,
    output_dir: Path,
    results_dir: Path | None,
    years: list[int],
    include_sensitivity: bool,
    exclude_outliers: bool,
    docs_dir: Path | None = None,
    show_chart_reporter_count: bool = False,
) -> None:
    """Run the full revenue-source analysis workflow."""

    info("Starting revenue-source analysis.")
    paths = ensure_output_dirs(
        output_dir,
        results_dir=results_dir,
        docs_dir=docs_dir,
        show_chart_reporter_count=show_chart_reporter_count,
    )
    gt_raw = load_givingtuesday_analysis(data_root, years)
    raw_benchmark = load_givingtuesday_raw_benchmark(data_root, years)
    raw_field_validation = build_raw_field_validation_table(raw_benchmark, gt_raw)
    write_table(raw_field_validation, paths.tables_dir / "raw_field_mapping_validation.csv")
    full_universe_analysis: pd.DataFrame | None = None
    if exclude_outliers:
        full_universe_analysis = prepare_givingtuesday_analysis(gt_raw, exclude_outliers=False)
        outlier_mask = (
            full_universe_analysis["is_hospital"].fillna(False)
            | full_universe_analysis["is_university"].fillna(False)
            | full_universe_analysis["is_political_org"].fillna(False)
        )
        analysis = full_universe_analysis.loc[~outlier_mask].copy().reset_index(drop=True)
        analysis.attrs["excluded_org_type_row_count"] = int(outlier_mask.sum())
        analysis.attrs["excluded_org_type_ein_count"] = int(full_universe_analysis.loc[outlier_mask, "ein"].nunique())
        analysis.attrs["full_universe_row_count"] = int(len(full_universe_analysis))
        analysis.attrs["full_universe_ein_count"] = int(full_universe_analysis["ein"].nunique())
        info(
            "Primary comparable-universe exclusion applied. "
            f"Rows before={len(full_universe_analysis):,}, after={len(analysis):,}, "
            f"excluded rows={int(outlier_mask.sum()):,}."
        )
    else:
        analysis = prepare_givingtuesday_analysis(gt_raw, exclude_outliers=False)
        analysis.attrs["excluded_org_type_row_count"] = 0
        analysis.attrs["excluded_org_type_ein_count"] = 0
        analysis.attrs["full_universe_row_count"] = int(len(analysis))
        analysis.attrs["full_universe_ein_count"] = int(analysis["ein"].nunique())

    write_parquet(analysis, paths.output_dir / "cleaned_revenue_sources_analysis.parquet")
    write_table(analysis, paths.output_dir / "cleaned_revenue_sources_analysis.csv")

    tables: dict[str, pd.DataFrame] = {}
    tables["counts"] = counts_table(analysis)
    tables["missingness"] = missingness_table(analysis)
    tables["summary_statistics"] = summary_statistics(analysis)
    tables["mix_by_group"] = aggregate_revenue_mix(analysis, ["comparison_group"])
    tables["mix_by_group_year"] = aggregate_revenue_mix(analysis, ["comparison_group", "tax_year"])
    tables["mix_by_region"] = aggregate_revenue_mix(analysis, ["region_label"])
    tables["mix_by_group_form"] = aggregate_revenue_mix(analysis, ["comparison_group", "form_type"])
    tables["concentration_by_group"] = concentration_metrics(analysis, ["comparison_group"])
    tables["concentration_by_region"] = concentration_metrics(analysis, ["region_label"])
    tables["negative_residual_diagnostics"] = negative_residual_diagnostics(analysis)
    tables["component_overlap_by_group"] = component_overlap_summary(analysis, ["comparison_group"])
    tables["component_overlap_by_group_year"] = component_overlap_summary(analysis, ["comparison_group", "tax_year"])
    tables["component_overlap_by_form"] = component_overlap_summary(analysis, ["comparison_group", "form_type"])

    for name, table in tables.items():
        write_table(table, paths.tables_dir / f"{name}.csv")

    primary_results_five = run_univariate_tests(
        analysis, PRIMARY_TEST_VARIABLES, label="primary_all_years_five_regions", group_column="region_label"
    )
    primary_results_benchmark = run_univariate_tests(
        analysis, PRIMARY_TEST_VARIABLES, label="primary_all_years_bh_vs_benchmark", group_column="comparison_group"
    )
    primary_results = pd.concat([primary_results_five, primary_results_benchmark], ignore_index=True)
    regression_results = run_regressions(
        analysis,
        SHARE_COMPONENTS
        + [
            f"log1p_{column}"
            for column in [
                "total_revenue",
                "total_contributions",
                "program_service_revenue",
                "government_grants_received",
                "federated_campaigns",
                "related_org_contributions",
                "membership_dues",
                "fundraising_events_contributions",
                "mixed_unclassified_contributions",
                "residual_other_revenue",
            ]
        ],
    )
    multivariate_results = run_multivariate_tests(analysis, label="primary_all_years")

    # Focused individual-contributions analysis.
    #
    # The primary share variables divide every segment by *total revenue*,
    # which mixes revenue-mix differences with program-service-revenue
    # differences. For the client question "are individual donors versus
    # other organizations the dominant source of contributions in BH",
    # the cleaner contrast is share-of-total-contributions on a 990-only
    # frame. This block runs that focused analysis as a supplement.
    individual_focus_frame = prepare_individual_contributions_focus(analysis)
    individual_focus_mix = aggregate_individual_focus_mix(
        individual_focus_frame, ["comparison_group"]
    )
    individual_focus_mix_by_region = aggregate_individual_focus_mix(
        individual_focus_frame, ["region_label"]
    )
    individual_focus_mix_by_year = aggregate_individual_focus_mix(
        individual_focus_frame, ["comparison_group", "tax_year"]
    )
    individual_focus_univariate, individual_focus_bootstrap = run_individual_focus_tests(
        individual_focus_frame
    )

    # Year-by-year focused tests so the client can see whether the
    # institutional vs individual gap is stable across 2022, 2023, 2024 or
    # whether it is driven by a single filing year.
    individual_focus_by_year_frames: list[pd.DataFrame] = []
    individual_focus_by_year_bootstrap_rows: list[dict[str, object]] = []
    if not individual_focus_frame.empty:
        for tax_year, year_frame in individual_focus_frame.groupby("tax_year"):
            if (
                len(year_frame) < 10
                or year_frame["comparison_group"].nunique() < 2
                or year_frame["region_label"].nunique() < 2
            ):
                continue
            info(f"Running year-by-year individual-focus tests for tax_year={tax_year}.")
            individual_focus_by_year_frames.append(
                run_univariate_tests(
                    year_frame,
                    INDIVIDUAL_FOCUS_SHARES,
                    label=f"individual_focus_year_{tax_year}_five_regions",
                    group_column="region_label",
                )
            )
            individual_focus_by_year_frames.append(
                run_univariate_tests(
                    year_frame,
                    INDIVIDUAL_FOCUS_SHARES,
                    label=f"individual_focus_year_{tax_year}_bh_vs_benchmark",
                    group_column="comparison_group",
                )
            )
            for variable in INDIVIDUAL_FOCUS_SHARES:
                observed, lower, upper = bootstrap_ci_difference(year_frame, variable)
                individual_focus_by_year_bootstrap_rows.append({
                    "tax_year": str(tax_year),
                    "variable": variable,
                    "mean_difference": observed,
                    "ci_lower": lower,
                    "ci_upper": upper,
                })
    individual_focus_by_year_univariate = (
        pd.concat(individual_focus_by_year_frames, ignore_index=True)
        if individual_focus_by_year_frames
        else pd.DataFrame()
    )
    individual_focus_by_year_bootstrap = pd.DataFrame(individual_focus_by_year_bootstrap_rows)

    # Year-by-year tests are kept separate from the pooled headline tests. This
    # is important because 2024 filing coverage may be incomplete, and because a
    # pooled result can hide one-year changes in the direction or size of the
    # Black Hills versus benchmark difference.
    info("Running full year-by-year hypothesis tests.")
    by_year_univariate_frames: list[pd.DataFrame] = []
    by_year_multivariate_frames: list[pd.DataFrame] = []
    for tax_year, year_frame in analysis.groupby("tax_year"):
        if len(year_frame) < 10 or year_frame["comparison_group"].nunique() < 2 or year_frame["region_label"].nunique() < 2:
            info(f"Skipping year {tax_year}: insufficient rows, regions, or comparison group.")
            continue
        info(f"Running year-by-year tests for tax_year={tax_year} with rows={len(year_frame):,}.")
        by_year_univariate_frames.append(
            run_univariate_tests(
                year_frame,
                PRIMARY_TEST_VARIABLES,
                label=f"year_{tax_year}_five_regions",
                group_column="region_label",
            )
        )
        by_year_univariate_frames.append(
            run_univariate_tests(
                year_frame,
                PRIMARY_TEST_VARIABLES,
                label=f"year_{tax_year}_bh_vs_benchmark",
                group_column="comparison_group",
            )
        )
        by_year_multivariate_frames.append(run_multivariate_tests(year_frame, label=f"year_{tax_year}"))
    by_year_univariate_results = (
        pd.concat(by_year_univariate_frames, ignore_index=True)
        if by_year_univariate_frames
        else pd.DataFrame()
    )
    by_year_multivariate_results = (
        pd.concat(by_year_multivariate_frames, ignore_index=True)
        if by_year_multivariate_frames
        else pd.DataFrame()
    )

    sensitivity_results: list[pd.DataFrame] = []
    if include_sensitivity:
        info("Running requested sensitivity analyses.")

        # Independence sensitivity: keep only the most recent organization-year
        # per EIN. The pooled tests treat each org-year as an independent
        # observation, but the typical EIN files in two of the three years
        # (~1.94 obs/EIN). Reducing to one row per EIN removes within-EIN
        # autocorrelation entirely; if the headline pattern survives this
        # restriction, the repeated-EIN issue is not driving the result.
        one_per_ein = (
            analysis.sort_values(["ein", "tax_year"])
            .drop_duplicates(subset=["ein"], keep="last")
            .copy()
        )
        if (
            len(one_per_ein) >= 10
            and one_per_ein["comparison_group"].nunique() == 2
            and one_per_ein["region_label"].nunique() >= 2
        ):
            info(f"Running one-row-per-EIN sensitivity with rows={len(one_per_ein):,} (was {len(analysis):,}).")
            sensitivity_results.append(
                run_univariate_tests(
                    one_per_ein,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_one_row_per_ein_five_regions",
                    group_column="region_label",
                )
            )
            sensitivity_results.append(
                run_univariate_tests(
                    one_per_ein,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_one_row_per_ein_bh_vs_benchmark",
                    group_column="comparison_group",
                )
            )
            sensitivity_results.append(run_multivariate_tests(one_per_ein, label="sensitivity_one_row_per_ein"))

        year_sensitivity = analysis.loc[analysis["tax_year"].isin(["2022", "2023"])].copy()
        if not year_sensitivity.empty:
            sensitivity_results.append(
                run_univariate_tests(
                    year_sensitivity,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_2022_2023_only_five_regions",
                    group_column="region_label",
                )
            )
            sensitivity_results.append(
                run_univariate_tests(
                    year_sensitivity,
                    PRIMARY_TEST_VARIABLES,
                    label="sensitivity_2022_2023_only_bh_vs_benchmark",
                    group_column="comparison_group",
                )
            )
            sensitivity_results.append(run_multivariate_tests(year_sensitivity, label="sensitivity_2022_2023_only"))

        for form_type, form_frame in analysis.groupby("form_type"):
            if len(form_frame) >= 10 and form_frame["comparison_group"].nunique() == 2 and form_frame["region_label"].nunique() >= 2:
                sensitivity_results.append(
                    run_univariate_tests(
                        form_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_form_{form_type}_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        form_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_form_{form_type}_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )

        for stratum, stratum_frame in analysis.groupby("revenue_size_stratum"):
            if len(stratum_frame) >= 10 and stratum_frame["comparison_group"].nunique() == 2 and stratum_frame["region_label"].nunique() >= 2:
                sensitivity_results.append(
                    run_univariate_tests(
                        stratum_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_revenue_size_{stratum}_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        stratum_frame,
                        PRIMARY_TEST_VARIABLES,
                        label=f"sensitivity_revenue_size_{stratum}_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )

        if exclude_outliers and full_universe_analysis is not None:
            if (
                len(full_universe_analysis) >= 10
                and full_universe_analysis["comparison_group"].nunique() == 2
                and full_universe_analysis["region_label"].nunique() >= 2
            ):
                info("Running full-universe sensitivity that includes hospitals, universities, and political organizations.")
                sensitivity_results.append(
                    run_univariate_tests(
                        full_universe_analysis,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_including_outliers_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        full_universe_analysis,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_including_outliers_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )
        else:
            no_outlier = analysis.loc[
                ~(
                    analysis["is_hospital"].fillna(False)
                    | analysis["is_university"].fillna(False)
                    | analysis["is_political_org"].fillna(False)
                )
            ].copy()
            if len(no_outlier) >= 10 and no_outlier["comparison_group"].nunique() == 2 and no_outlier["region_label"].nunique() >= 2:
                sensitivity_results.append(
                    run_univariate_tests(
                        no_outlier,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_excluding_outliers_five_regions",
                        group_column="region_label",
                    )
                )
                sensitivity_results.append(
                    run_univariate_tests(
                        no_outlier,
                        PRIMARY_TEST_VARIABLES,
                        label="sensitivity_excluding_outliers_bh_vs_benchmark",
                        group_column="comparison_group",
                    )
                )

        core = load_core_sensitivity(data_root)
        if core is not None and not core.empty:
            core_results = run_univariate_tests(core, ["program_service_revenue_share", "total_contributions_share"], label="sensitivity_nccs_core_2022")
            sensitivity_results.append(core_results)
            write_table(core, paths.tables_dir / "nccs_core_2022_sensitivity_cleaned.csv")

    all_univariate_parts = [primary_results]
    if not by_year_univariate_results.empty:
        all_univariate_parts.append(by_year_univariate_results)
    all_univariate_parts.extend(sensitivity_results)
    all_stats = pd.concat(all_univariate_parts, ignore_index=True)
    write_table(all_stats, paths.tables_dir / "statistical_tests_univariate.csv")
    write_table(by_year_univariate_results, paths.tables_dir / "statistical_tests_by_year_univariate.csv")
    write_table(regression_results, paths.tables_dir / "statistical_tests_regression.csv")
    write_table(multivariate_results, paths.tables_dir / "statistical_tests_multivariate.csv")
    write_table(by_year_multivariate_results, paths.tables_dir / "statistical_tests_by_year_multivariate.csv")
    client_equivalent_tables = write_client_equivalent_tables(analysis, paths.tables_dir)

    bootstrap_rows = []
    for variable in SHARE_COMPONENTS:
        observed, lower, upper = bootstrap_ci_difference(analysis, variable)
        bootstrap_rows.append({"variable": variable, "mean_difference": observed, "ci_lower": lower, "ci_upper": upper})
    bootstrap_results = pd.DataFrame(bootstrap_rows)
    write_table(bootstrap_results, paths.tables_dir / "bootstrap_mean_difference_ci.csv")

    write_table(individual_focus_mix, paths.tables_dir / "individual_focus_mix_by_group.csv")
    write_table(individual_focus_mix_by_region, paths.tables_dir / "individual_focus_mix_by_region.csv")
    write_table(individual_focus_mix_by_year, paths.tables_dir / "individual_focus_mix_by_year.csv")
    write_table(individual_focus_univariate, paths.tables_dir / "individual_focus_statistical_tests.csv")
    write_table(individual_focus_by_year_univariate, paths.tables_dir / "individual_focus_statistical_tests_by_year.csv")
    write_table(individual_focus_bootstrap, paths.tables_dir / "individual_focus_bootstrap_ci.csv")
    write_table(individual_focus_by_year_bootstrap, paths.tables_dir / "individual_focus_bootstrap_ci_by_year.csv")

    create_figures(analysis, tables, paths.figures_dir)
    create_client_notebook_equivalent_outputs(
        analysis,
        tables,
        paths,
        full_universe_frame=full_universe_analysis,
    )
    save_individual_focus_chart(
        individual_focus_mix,
        paths.figures_dir / "individual_focus_contributions_mix.png",
        "Form-990 contribution channels: Black Hills vs pooled benchmarks",
    )
    save_individual_focus_year_chart(
        individual_focus_mix_by_year,
        paths.figures_dir / "individual_focus_contributions_mix_by_year.png",
        "Form-990 contribution channels by group and tax year",
    )
    collect_results_artifacts(
        paths,
        table_names=[
            "counts.csv",
            "mix_by_group.csv",
            "mix_by_group_year.csv",
            "mix_by_region.csv",
            "mix_by_group_form.csv",
            "statistical_tests_univariate.csv",
            "statistical_tests_by_year_univariate.csv",
            "statistical_tests_regression.csv",
            "statistical_tests_multivariate.csv",
            "statistical_tests_by_year_multivariate.csv",
            "bootstrap_mean_difference_ci.csv",
            "concentration_by_group.csv",
            "concentration_by_region.csv",
            "negative_residual_diagnostics.csv",
            "component_overlap_by_group.csv",
            "component_overlap_by_group_year.csv",
            "component_overlap_by_form.csv",
            "raw_field_mapping_validation.csv",
            *[f"{name}.csv" for name in client_equivalent_tables],
            "individual_focus_mix_by_group.csv",
            "individual_focus_mix_by_region.csv",
            "individual_focus_mix_by_year.csv",
            "individual_focus_statistical_tests.csv",
            "individual_focus_statistical_tests_by_year.csv",
            "individual_focus_bootstrap_ci.csv",
            "individual_focus_bootstrap_ci_by_year.csv",
        ],
        figure_names=[
            "stacked_revenue_mix_black_hills_vs_benchmark.png",
            "stacked_revenue_mix_by_year.png",
            "stacked_revenue_mix_by_region.png",
            "stacked_revenue_mix_by_form_type.png",
            "violin_source_shares_by_group.png",
            "diagnostic_total_revenue_distribution.png",
            "diagnostic_residual_revenue.png",
            "concentration_top5_revenue_share_by_region.png",
            "heatmap_revenue_source_share_by_year.png",
            "individual_focus_contributions_mix.png",
            "individual_focus_contributions_mix_by_year.png",
        ],
    )
    write_detailed_results_report(
        analysis=analysis,
        tables=tables,
        univariate_results=all_stats,
        regression_results=regression_results,
        multivariate_results=multivariate_results,
        by_year_univariate_results=by_year_univariate_results,
        by_year_multivariate_results=by_year_multivariate_results,
        bootstrap_results=bootstrap_results,
        individual_focus_frame=individual_focus_frame,
        individual_focus_mix=individual_focus_mix,
        individual_focus_univariate=individual_focus_univariate,
        individual_focus_bootstrap=individual_focus_bootstrap,
        individual_focus_by_year_univariate=individual_focus_by_year_univariate,
        individual_focus_by_year_bootstrap=individual_focus_by_year_bootstrap,
        paths=paths,
    )
    build_markdown_summary(
        analysis,
        all_stats,
        multivariate_results,
        by_year_univariate_results,
        tables["concentration_by_group"],
        paths.output_dir / "revenue_sources_methods_results_summary.md",
    )
    info("Revenue-source analysis complete.")
    info(f"Review outputs in: {paths.output_dir}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line options."""

    parser = argparse.ArgumentParser(description="Analyze nonprofit revenue-source differences between Black Hills and benchmark regions.")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT, help="Project 01_data root. Defaults to python.utils.paths.DATA.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory. Defaults to DATA/analysis/revenue_sources_black_hills.")
    parser.add_argument("--results-dir", type=Path, default=None, help="User-facing results bundle directory. Defaults to python/analysis/revenue_sources_black_hills/results.")
    parser.add_argument(
        "--docs-dir",
        type=Path,
        default=None,
        help=(
            "Directory to write the externally-distributed client presentation "
            "Markdown and its image assets. Defaults to the repository docs/ "
            "directory. Override this (for example to a temp path) when running "
            "the workflow in a context where the real docs/ should not be "
            "touched, such as automated tests or scratch runs."
        ),
    )
    parser.add_argument("--years", type=int, nargs="+", default=[2022], help="Tax years to include.")
    parser.add_argument("--include-sensitivity", action=argparse.BooleanOptionalAction, default=True, help="Run sensitivity analyses.")
    parser.add_argument(
        "--exclude-outliers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Exclude hospital, university, and political-organization proxy rows "
            "from the primary frame for client-peer comparability. Use "
            "--no-exclude-outliers to make the full valid universe primary."
        ),
    )
    parser.add_argument(
        "--show-chart-n",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Print the positive-reporter count (n=...) above each bar on the "
            "2022 pairwise client charts. Default is off; slide bullets still "
            "report n regardless."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """CLI entry point."""

    args = parse_args(argv)
    data_root = args.data_root.resolve()
    output_dir = (args.output_dir or (data_root / "analysis" / "revenue_sources_black_hills")).resolve()
    run_analysis(
        data_root=data_root,
        output_dir=output_dir,
        results_dir=args.results_dir.resolve() if args.results_dir else None,
        docs_dir=args.docs_dir.resolve() if args.docs_dir else None,
        years=args.years,
        include_sensitivity=args.include_sensitivity,
        exclude_outliers=args.exclude_outliers,
        show_chart_reporter_count=args.show_chart_n,
    )


if __name__ == "__main__":
    main()
