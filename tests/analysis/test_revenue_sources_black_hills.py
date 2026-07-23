"""Tests for the Black Hills revenue-source analysis script."""

from __future__ import annotations

import importlib.util
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_FILE_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _FILE_DIR.parent.parent
_ANALYSIS_PATH = _REPO_ROOT / "python" / "analysis" / "revenue_sources_black_hills" / "revenue_sources_black_hills.py"
_ASSUMPTIONS_PATH = _REPO_ROOT / "python" / "analysis" / "revenue_sources_black_hills" / "assumptions_check.py"
_PYTHON_DIR = _REPO_ROOT / "python"
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
_SPEC = importlib.util.spec_from_file_location("revenue_sources_black_hills", _ANALYSIS_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise ImportError(f"Unable to load analysis module from {_ANALYSIS_PATH}")
revenue_analysis = importlib.util.module_from_spec(_SPEC)
sys.modules.setdefault("revenue_sources_black_hills", revenue_analysis)
_SPEC.loader.exec_module(revenue_analysis)
_ASSUMPTIONS_SPEC = importlib.util.spec_from_file_location("assumptions_check", _ASSUMPTIONS_PATH)
if _ASSUMPTIONS_SPEC is None or _ASSUMPTIONS_SPEC.loader is None:
    raise ImportError(f"Unable to load assumptions module from {_ASSUMPTIONS_PATH}")
assumptions_check = importlib.util.module_from_spec(_ASSUMPTIONS_SPEC)
sys.modules.setdefault("assumptions_check", assumptions_check)
_ASSUMPTIONS_SPEC.loader.exec_module(assumptions_check)


def _synthetic_gt_frame() -> pd.DataFrame:
    """
    Small GT-like fixture covering the Section 3 Q9 donor-channel decomposition.

    Row layout:
      - ein 1: Form 990 with Line 1 sub-components fully reconciled to Line 1h
      - ein 2: Form 990-PF with only the contributions total (no sub-components)
      - ein 3: Form 990-EZ with only the contributions total (no sub-components)
      - ein 4: Form 990 with reconciled sub-components and a hospital flag
    """

    return pd.DataFrame(
        [
            {
                "ein": "1",
                "tax_year": "2022",
                "form_type": "990",
                "region": "BlackHills",
                "analysis_total_revenue_amount": 100.0,
                "analysis_program_service_revenue_amount": 20.0,
                "analysis_total_contributions_amount": 30.0,
                "analysis_cash_contributions_amount": 30.0,
                "analysis_noncash_contributions_amount": 5.0,
                # Form 990 Line 1 sub-components reconcile to Line 1h = 30
                "analysis_federated_campaigns_amount": 1.0,
                "analysis_membership_dues_amount": 4.0,
                "analysis_fundraising_events_contributions_amount": 5.0,
                "analysis_related_org_contributions_amount": 2.0,
                "analysis_government_grants_amount": 3.0,
                "analysis_other_contributions_amount": 15.0,
                "analysis_calculated_grants_total_amount": 6.0,  # 1a + 1d + 1e = 1+2+3
                "analysis_imputed_is_hospital": "false",
                "analysis_imputed_is_university": "false",
                "analysis_imputed_is_political_org": "false",
            },
            {
                "ein": "2",
                "tax_year": "2022",
                "form_type": "990PF",
                "region": "Billings",
                "analysis_total_revenue_amount": 200.0,
                "analysis_program_service_revenue_amount": 99.0,
                "analysis_total_contributions_amount": 50.0,
                "analysis_cash_contributions_amount": np.nan,
                "analysis_noncash_contributions_amount": np.nan,
                "analysis_federated_campaigns_amount": np.nan,
                "analysis_membership_dues_amount": np.nan,
                "analysis_fundraising_events_contributions_amount": np.nan,
                "analysis_related_org_contributions_amount": np.nan,
                "analysis_government_grants_amount": np.nan,
                "analysis_other_contributions_amount": np.nan,
                "analysis_calculated_grants_total_amount": np.nan,
                "analysis_imputed_is_hospital": "false",
                "analysis_imputed_is_university": "false",
                "analysis_imputed_is_political_org": "false",
            },
            {
                "ein": "3",
                "tax_year": "2023",
                "form_type": "990EZ",
                "region": "SiouxFalls",
                "analysis_total_revenue_amount": 50.0,
                "analysis_program_service_revenue_amount": 40.0,
                "analysis_total_contributions_amount": 35.0,
                "analysis_cash_contributions_amount": 35.0,
                "analysis_noncash_contributions_amount": 10.0,
                "analysis_federated_campaigns_amount": np.nan,
                "analysis_membership_dues_amount": np.nan,
                "analysis_fundraising_events_contributions_amount": np.nan,
                "analysis_related_org_contributions_amount": np.nan,
                "analysis_government_grants_amount": np.nan,
                "analysis_other_contributions_amount": np.nan,
                "analysis_calculated_grants_total_amount": np.nan,
                "analysis_imputed_is_hospital": "false",
                "analysis_imputed_is_university": "false",
                "analysis_imputed_is_political_org": "false",
            },
            {
                "ein": "4",
                "tax_year": "2023",
                "form_type": "990",
                "region": "BlackHills",
                "analysis_total_revenue_amount": 300.0,
                "analysis_program_service_revenue_amount": 120.0,
                "analysis_total_contributions_amount": 130.0,
                "analysis_cash_contributions_amount": 130.0,
                "analysis_noncash_contributions_amount": 20.0,
                # Reconciled Line 1 sub-components: 5+10+15+10+20+70 = 130
                "analysis_federated_campaigns_amount": 5.0,
                "analysis_membership_dues_amount": 10.0,
                "analysis_fundraising_events_contributions_amount": 15.0,
                "analysis_related_org_contributions_amount": 10.0,
                "analysis_government_grants_amount": 20.0,
                "analysis_other_contributions_amount": 70.0,
                "analysis_calculated_grants_total_amount": 35.0,  # 1a+1d+1e = 5+10+20
                "analysis_imputed_is_hospital": "true",
                "analysis_imputed_is_university": "false",
                "analysis_imputed_is_political_org": "false",
            },
        ]
    )


def test_prepare_decomposes_form_990_donor_channels() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())

    first = prepared.loc[prepared["ein"].eq("000000001")].iloc[0]
    assert first["total_contributions"] == 30.0
    assert first["government_grants_received"] == 3.0  # Line 1e
    assert first["other_institutional_contributions"] == 3.0  # Line 1a + 1d
    assert first["individual_likely_contributions"] == 9.0  # Line 1b + 1c
    assert first["mixed_other_contributions"] == 15.0  # Line 1f
    # Segments + program service should sum to total revenue (100) on a clean
    # 990 row; residual is 50 here because the contributions total is 30 and
    # program service is 20.
    assert first["residual_other_revenue"] == 50.0
    assert first["comparison_group"] == "Black Hills"


def test_prepare_routes_990ez_and_990pf_contributions_to_mixed_bucket() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())

    pf = prepared.loc[prepared["form_type"].eq("990PF")].iloc[0]
    # Form 990-PF does not have a comparable program-service revenue concept,
    # nor any of the Form 990 Part VIII Line 1 sub-components. Those analysis
    # fields must stay NaN so tests and medians do not confuse "unavailable"
    # with "reported zero".
    assert pd.isna(pf["program_service_revenue"])
    assert pd.isna(pf["government_grants_received"])
    assert pd.isna(pf["federated_campaigns"])
    assert pd.isna(pf["related_org_contributions"])
    assert pd.isna(pf["membership_dues"])
    assert pd.isna(pf["fundraising_events_contributions"])
    assert pd.isna(pf["other_institutional_contributions"])
    assert pd.isna(pf["individual_likely_contributions"])
    # The full Part I Line 1 total is the only contribution amount that is
    # actually reported, so it is routed into the undecomposable mixed bucket.
    assert pf["total_contributions"] == 50.0
    assert pf["mixed_unclassified_contributions"] == 50.0
    assert pf["mixed_other_contributions"] == 50.0
    assert pf["comparison_group"] == "Benchmark"

    ez = prepared.loc[prepared["form_type"].eq("990EZ")].iloc[0]
    # Form 990-EZ does not expose the detailed Line 1 sub-components either, so
    # those analysis fields are kept NaN even though program service revenue
    # itself remains reported on Form 990-EZ.
    assert pd.isna(ez["government_grants_received"])
    assert pd.isna(ez["federated_campaigns"])
    assert pd.isna(ez["related_org_contributions"])
    assert pd.isna(ez["membership_dues"])
    assert pd.isna(ez["fundraising_events_contributions"])
    assert pd.isna(ez["other_institutional_contributions"])
    assert pd.isna(ez["individual_likely_contributions"])
    # Program service revenue is reported on 990-EZ Line 2, and the entire
    # Part I Line 1 contributions total is routed into the mixed bucket.
    assert ez["program_service_revenue"] == 40.0
    assert ez["total_contributions"] == 35.0
    assert ez["mixed_unclassified_contributions"] == 35.0
    assert ez["mixed_other_contributions"] == 35.0


def test_well_behaved_row_share_sum_not_over_100_percent() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())
    row = prepared.loc[prepared["ein"].eq("000000001")].iloc[0]
    assert float(row["source_share_sum"]) <= 1.000001
    assert bool(row["share_over_100_flag"]) is False


def test_negative_residual_and_share_flags_are_reported() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())
    diagnostic = revenue_analysis.negative_residual_diagnostics(prepared)
    overlap = revenue_analysis.component_overlap_summary(prepared, ["comparison_group"])

    # ein 3 has program_service=40 + mixed=35 > total revenue 50 -> negative
    # residual; the named-segment shares add to 150 percent so the over-100
    # flag also fires.
    assert "000000003" in set(diagnostic["ein"])
    flagged = diagnostic.loc[diagnostic["ein"].eq("000000003")].iloc[0]
    assert bool(flagged["negative_residual_flag"]) is True
    assert bool(flagged["share_over_100_flag"]) is True
    assert {"negative_residual_rate", "share_over_100_rate", "mean_source_share_sum"}.issubset(overlap.columns)


def test_aggregate_mix_keeps_reported_and_normalized_shares() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())
    mix = revenue_analysis.aggregate_revenue_mix(prepared, ["comparison_group"])

    assert {"share", "normalized_mix_share", "reported_component_share_sum"}.issubset(mix.columns)
    normalized_sums = mix.groupby("comparison_group")["normalized_mix_share"].sum()
    assert np.allclose(normalized_sums.dropna(), 1.0)
    # The full set of detailed donor-channel components from SOURCE_COMPONENTS
    # should be present in the mix. This mirrors the Form 990 Part VIII Line 1
    # decomposition the headline analysis tests against.
    assert set(mix["component"]) >= set(revenue_analysis.SOURCE_COMPONENTS)
    assert set(mix["component"]) >= {
        "program_service_revenue",
        "government_grants_received",
        "federated_campaigns",
        "related_org_contributions",
        "membership_dues",
        "fundraising_events_contributions",
        "mixed_unclassified_contributions",
        "residual_other_revenue",
    }


def test_outlier_exclusion_removes_imputed_hospital() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame(), exclude_outliers=True)

    assert "000000004" not in set(prepared["ein"])
    assert len(prepared) == 3


def test_fdr_bh_handles_empty_missing_and_populated_values() -> None:
    assert revenue_analysis.fdr_bh([]) == []
    result = revenue_analysis.fdr_bh([0.01, np.nan, 0.04])

    assert len(result) == 3
    assert result[1] != result[1]
    assert result[0] <= result[2]


def test_univariate_and_multivariate_helpers_return_structured_rows() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())
    stats_result = revenue_analysis.run_univariate_tests(
        prepared,
        ["program_service_revenue_share", "mixed_other_contributions_share"],
        label="test_frame",
        group_column="comparison_group",
    )
    multivariate = revenue_analysis.run_multivariate_tests(prepared, label="test_frame")

    assert {"analysis_frame", "test", "variable", "statistic", "p_value", "n", "group_column"}.issubset(stats_result.columns)
    assert "welch_anova" in set(stats_result["test"])
    assert {"analysis_frame", "test", "statistic", "p_value", "n"}.issubset(multivariate.columns)
    assert "permanova_clr" in set(multivariate["test"])


def test_logistic_assumption_notes_report_actual_group_rates() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())
    rows = assumptions_check.check_logistic_separation(prepared)

    assert rows
    for row in rows:
        assert "BH=nan" not in str(row["notes"])
        assert "Benchmark=nan" not in str(row["notes"])


def test_five_region_anova_runs_when_region_label_has_multiple_groups() -> None:
    regions = ["BlackHills", "Billings", "Flagstaff", "Missoula", "SiouxFalls"]
    rows = []
    for idx, region in enumerate(regions):
        for dup in range(2):
            rows.append(
                {
                    "ein": f"{idx}{dup}".zfill(9),
                    "tax_year": "2022",
                    "form_type": "990",
                    "region": region,
                    "comparison_group": "Black Hills" if region == "BlackHills" else "Benchmark",
                    "analysis_total_revenue_amount": 100.0,
                    "analysis_program_service_revenue_amount": 20.0 + dup,
                    "analysis_total_contributions_amount": 40.0,
                    "analysis_federated_campaigns_amount": 2.0,
                    "analysis_membership_dues_amount": 5.0,
                    "analysis_fundraising_events_contributions_amount": 5.0,
                    "analysis_related_org_contributions_amount": 3.0,
                    "analysis_government_grants_amount": 5.0,
                    "analysis_other_contributions_amount": 20.0,
                    "analysis_calculated_grants_total_amount": 10.0,
                    "analysis_imputed_is_hospital": "false",
                    "analysis_imputed_is_university": "false",
                    "analysis_imputed_is_political_org": "false",
                }
            )
    frame_in = pd.DataFrame(rows)
    prepared = revenue_analysis.prepare_givingtuesday_analysis(frame_in)
    out = revenue_analysis.run_univariate_tests(
        prepared,
        ["program_service_revenue_share"],
        label="five_region_smoke",
        group_column="region_label",
    )
    welch = out.loc[out["test"].eq("welch_anova")]
    assert not welch.empty
    assert not pd.isna(welch.iloc[0]["p_value"])
    assert welch.iloc[0]["group_column"] == "region_label"


def test_concentration_metrics_are_bounded() -> None:
    prepared = revenue_analysis.prepare_givingtuesday_analysis(_synthetic_gt_frame())
    concentration = revenue_analysis.concentration_metrics(prepared, ["comparison_group"])

    assert not concentration.empty
    assert concentration["gini_total_revenue"].between(0, 1).all()
    assert concentration["hhi_total_revenue"].between(0, 1).all()
    assert concentration["top5_revenue_share"].between(0, 1).all()


def test_synthetic_run_writes_expected_outputs_and_prints_progress(tmp_path: Path, capsys) -> None:
    data_root = tmp_path / "data"
    gt_dir = data_root / "staging" / "filing"
    gt_dir.mkdir(parents=True)
    _synthetic_gt_frame().to_parquet(gt_dir / "givingtuesday_990_basic_allforms_analysis_variables.parquet", index=False)

    output_dir = tmp_path / "analysis" / "revenue_sources_black_hills"
    revenue_analysis.run_analysis(
        data_root=data_root,
        output_dir=output_dir,
        results_dir=tmp_path / "results",
        # Redirect the client-presentation docs into the test's tmp dir so the
        # synthetic-fixture run never overwrites the real client deliverables
        # in the repo's docs/ folder.
        docs_dir=tmp_path / "docs",
        years=[2022, 2023],
        include_sensitivity=False,
        exclude_outliers=False,
    )
    captured = capsys.readouterr()

    assert "[revenue-sources] Loading primary GivingTuesday analysis file" in captured.out
    assert "[revenue-sources] Running full year-by-year hypothesis tests." in captured.out
    assert (output_dir / "cleaned_revenue_sources_analysis.csv").exists()
    assert (output_dir / "tables" / "statistical_tests_univariate.csv").exists()
    assert (output_dir / "tables" / "statistical_tests_by_year_univariate.csv").exists()
    assert (output_dir / "tables" / "statistical_tests_by_year_multivariate.csv").exists()
    assert (output_dir / "tables" / "component_overlap_by_group.csv").exists()
    assert (output_dir / "figures" / "stacked_revenue_mix_black_hills_vs_benchmark.png").exists()
    assert (tmp_path / "results" / "revenue_sources_black_hills_results.md").exists()
    assert (output_dir / "revenue_sources_methods_results_summary.md").exists()
    # The client presentation and its image assets must land in the test's tmp
    # docs path, not the repo's real docs/ directory. This guards against the
    # synthetic fixture overwriting the client-facing deliverables.
    assert (
        tmp_path
        / "docs"
        / "analysis"
        / "revenue_sources"
        / "q9_2022_client_presentation.md"
    ).exists()
    assert (
        tmp_path / "docs" / "analysis" / "revenue_sources" / "assets" / "q9_2022"
    ).is_dir()


def test_default_results_dir_is_next_to_analysis_code() -> None:
    default_path = revenue_analysis.default_results_dir_for_output(Path("ignored"))

    assert default_path == _REPO_ROOT / "python" / "analysis" / "revenue_sources_black_hills" / "results"


def test_special_org_sensitivity_slide_summarizes_significance_change() -> None:
    comparison = pd.DataFrame(
        [
            {
                "variable": "total_revenue",
                "variable_label": "Total revenue",
                "benchmark_region": "Sioux Falls",
                "direction_excluded": "Black Hills lower among reporters",
                "direction_included": "Black Hills lower among reporters",
                "p_value_excluded": 0.0559,
                "p_value_included": 0.0244,
                "black_hills_positive_median_excluded": 176_760.0,
                "benchmark_positive_median_excluded": 208_149.0,
                "black_hills_positive_median_included": 176_966.0,
                "benchmark_positive_median_included": 217_451.0,
                "significance_excluded": "not significant",
                "significance_included": "significant",
                "direction_changed": False,
                "significance_changed": True,
                "any_change": True,
            }
        ]
    )
    exclusion_summary = pd.DataFrame(
        [
            {
                "region_label": "Sioux Falls",
                "full_universe_rows": 581,
                "special_org_rows": 16,
                "hospital_rows": 9,
                "university_rows": 7,
                "political_org_rows": 0,
            }
        ]
    )
    lines = revenue_analysis.build_2022_special_org_sensitivity_slide_lines(
        comparison,
        exclusion_summary,
        slide_number=13,
        first_source_slide=3,
        last_source_slide=12,
        special_org_ein_count=25,
    )
    text = "\n".join(lines)
    assert "## Slide 13: Sensitivity" in text
    assert "**Direction changes:** 0" in text
    assert "**Significance-status changes:** 1" in text
    assert "Total revenue vs Sioux Falls" in text


def test_permutation_median_difference_matches_observed_and_runs_seeded() -> None:
    rng = np.random.default_rng(0)
    higher = rng.normal(loc=200_000, scale=20_000, size=60)
    lower = rng.normal(loc=120_000, scale=20_000, size=60)

    result = revenue_analysis.permutation_median_difference(
        higher, lower, iterations=500, seed=42
    )

    assert result["iterations"] == 500
    assert result["n_a"] == 60 and result["n_b"] == 60
    assert result["median_difference"] == float(np.median(higher) - np.median(lower))
    assert 0.0 < result["p_value"] <= 1.0
    assert result["p_value"] < 0.05


def test_permutation_median_difference_returns_nan_for_tiny_samples() -> None:
    result = revenue_analysis.permutation_median_difference(
        np.array([100.0]), np.array([200.0, 300.0]), iterations=10
    )

    assert math.isnan(result["median_difference"])
    assert math.isnan(result["p_value"])


def test_bootstrap_median_difference_ci_contains_point_estimate() -> None:
    rng = np.random.default_rng(1)
    higher = rng.normal(loc=50_000, scale=5_000, size=80)
    lower = rng.normal(loc=30_000, scale=5_000, size=80)

    result = revenue_analysis.bootstrap_median_difference_ci(
        higher, lower, iterations=400, seed=2024
    )

    point = result["median_difference"]
    assert result["ci_low"] <= point <= result["ci_high"]
    assert result["ci_low"] > 0  # strictly positive separation should yield positive CI


def test_format_median_gap_with_ci_handles_sign_and_missing() -> None:
    assert (
        revenue_analysis._format_median_gap_with_ci(-35_242, -55_000, -5_000)
        == "-$35,242 (95% CI -$55,000 to -$5,000)"
    )
    assert revenue_analysis._format_median_gap_with_ci(12_345, None, None) == "+$12,345"
    assert revenue_analysis._format_median_gap_with_ci(None, None, None) == "NA"


def test_cli_excludes_org_type_outliers_by_default() -> None:
    default_args = revenue_analysis.parse_args([])
    full_universe_args = revenue_analysis.parse_args(["--no-exclude-outliers"])

    assert default_args.exclude_outliers is True
    assert full_universe_args.exclude_outliers is False
