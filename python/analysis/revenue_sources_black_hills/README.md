# Black Hills Revenue Sources Analysis

This folder contains the revenue-source comparison analysis for Black Hills
nonprofit organizations versus benchmark-region nonprofit organizations. The
analysis answers this client question:

> For the limited universe of organizations filing Form 990, Form 990-EZ, or
> Form 990-PF, is there a difference in revenue sources between Black Hills and
> the benchmark regions?

The analysis is implemented in:

- `revenue_sources_black_hills.py`: main analysis workflow.
- `assumptions_check.py`: empirical statistical-assumption diagnostics for the
  cleaned frame created by the main workflow.
- `results/`: user-facing report bundle with selected tables, figures, and
  assumption diagnostics.

## Current Primary Answer

The primary analysis excludes hospitals, universities, and political
organizations so the benchmark universe better reflects the client-peer
population. The full Form 990, 990-EZ, and 990-PF universe is still retained as
a sensitivity analysis.

Under the current primary comparable-universe frame:

- Analytic organization-year rows: 1,799.
- Unique EINs: 1,797.
- Tax year: 2022.
- Regions: Black Hills, Billings, Flagstaff, Missoula, and Sioux Falls.
- Excluded hospital/university/political organization rows: 25 rows, removed
  from the full valid 2022 universe of 1,824 rows.

The main statistical conclusion is:

- Several raw-dollar revenue-source distributions differ across the five
  regions in 2022, including total contributions, government grants,
  membership dues, fundraising-event contributions, mixed / unclassified
  contributions, and other revenue.
- In the Black Hills versus pooled-benchmark 2022 follow-up, raw p-values show
  Black Hills lower on total revenue and fundraising-event contributions, but
  these do not remain significant after FDR adjustment.
- A full-universe sensitivity that includes hospitals, universities, and
  political organizations does not change the substantive conclusion.

## Data Sources

The primary input is the GivingTuesday 990 basic all-forms analysis-variable
file:

```text
DATA/staging/filing/givingtuesday_990_basic_allforms_analysis_variables.parquet
```

`DATA` comes from `python/utils/paths.py` and defaults to:

```text
data/321_Black_Hills_Area_Community_Foundation_2025_08/01_data
```

The script can also use NCCS Core as a 2022 sensitivity check when this file is
available:

```text
DATA/staging/nccs_990/core/nccs_990_core_analysis_variables.parquet
```

The GivingTuesday file is the main source because it covers all three requested
form families:

- Form 990.
- Form 990-EZ.
- Form 990-PF.

NCCS Core is not the primary source because it does not provide the same all-form
coverage and donor-channel detail for the full requested universe. It is used
only as a sensitivity check where comparable fields exist.

## Geographic Scope

The script maps detailed region codes to report labels:

| Source region code | Report label |
| --- | --- |
| `BlackHills` | Black Hills |
| `SiouxFalls` | Sioux Falls |
| `Billings` | Billings |
| `Flagstaff` | Flagstaff |
| `Missoula` | Missoula |

It also creates a two-group comparison variable:

- `Black Hills`: rows where `region == "BlackHills"`.
- `Benchmark`: all other benchmark regions pooled together.

The analysis therefore has two complementary views:

- Five-region comparison: Black Hills, Sioux Falls, Billings, Flagstaff,
  Missoula.
- Follow-up comparison: Black Hills versus pooled benchmarks.

## Primary Universe Definition

The main script starts from the GivingTuesday all-forms analysis file, keeps the
requested tax years, derives analysis fields, and then filters to rows that can
answer the revenue-source question.

By default, the command-line entry point uses:

```text
--exclude-outliers
```

This is implemented as a client-peer comparability exclusion. It removes rows
flagged by upstream proxy variables as:

- Hospitals.
- Universities.
- Political organizations.

The current primary run removes 54 organization-year rows from 35 EINs. The
resulting primary cleaned frame has no hospital, university, or political rows
remaining.

Rows must also have:

- Positive total revenue.
- A valid region.
- A valid tax year.
- A valid form type.

The script still supports a full-universe primary run:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py --no-exclude-outliers
```

When the default exclusion is active, the analysis automatically runs a
full-universe sensitivity under these labels:

- `sensitivity_including_outliers_five_regions`
- `sensitivity_including_outliers_bh_vs_benchmark`

## Revenue Variables and Derivations

The analysis derives revenue-source components at the organization-year grain.
The detailed components are intended to partition revenue as cleanly as the
available IRS basic 990-family data allows:

```text
total_revenue =
  program_service_revenue
  + government_grants_received
  + federated_campaigns
  + related_org_contributions
  + membership_dues
  + fundraising_events_contributions
  + mixed_unclassified_contributions
  + residual_other_revenue
```

| Component | Meaning | Source/derivation |
| --- | --- | --- |
| `program_service_revenue` | Program service revenue | Form 990 / 990-EZ Line 2g through the GivingTuesday analysis field. Form 990-PF is kept missing because it does not have the same comparable concept in this file. Blank supported 990/990-EZ lines are treated as zero. |
| `total_contributions` | Total contributions | GivingTuesday `analysis_total_contributions_amount`, mapped upstream from `TOTACASHCONT` for Form 990, `CONGIFGRAETC` for Form 990-EZ, and `STREACGRTOIN` for Form 990-PF. Blank supported lines are treated as zero. |
| `government_grants_received` | Government grants received | Form 990 Part VIII Line 1e, `GOVERNGRANTS`, for Form 990 rows. 990-EZ and 990-PF rows are excluded for this variable because those forms do not expose this subcomponent in the same way. |
| `federated_campaigns` | Federated campaign contributions | Form 990 Part VIII Line 1a, `FEDERACAMPAI`. 990-EZ and 990-PF rows are excluded for this variable because source detail is unavailable. |
| `related_org_contributions` | Related organization contributions | Form 990 Part VIII Line 1d, `RELATEORGANI`. 990-EZ and 990-PF rows are excluded for this variable because source detail is unavailable. |
| `membership_dues` | Membership dues | Form 990 Part VIII Line 1b, `MEMBERDUESUE`. Individual-adjacent, not pure individual giving. 990-EZ and 990-PF rows are excluded for this variable. |
| `fundraising_events_contributions` | Fundraising event contributions | Form 990 Part VIII Line 1c, `FUNDRAEVENTS`. Individual-adjacent, not pure individual giving. 990-EZ and 990-PF rows are excluded for this variable. |
| `mixed_unclassified_contributions` | Mixed / unclassified contribution bucket | Form 990 Part VIII Line 1f, `ALLOOTHECONT`, for Form 990 rows. For 990-EZ and 990-PF, the full reported total contributions amount is routed here because those forms do not separately report the Line 1 subcomponents. |
| `residual_other_revenue` | Other revenue | `total_revenue - program_service_revenue - detailed contribution components`. Negative residuals are retained for diagnostics but clipped at zero for composition plots/transforms. |

Older aggregate aliases remain internally for backward compatibility, but
client-facing outputs and headline tests use the detailed categories above.

The analysis distinguishes blank supported amount lines from unavailable
form-specific fields. Blank supported lines are interpreted as reported zero;
fields that a form cannot report are kept missing and excluded from that
variable's tests and medians.

The main raw-dollar variables tested and summarized are:

- `total_revenue`
- `program_service_revenue`
- `total_contributions`
- `government_grants_received`
- `federated_campaigns`
- `related_org_contributions`
- `membership_dues`
- `fundraising_events_contributions`
- `mixed_unclassified_contributions`
- `residual_other_revenue`

The script also carries supporting diagnostic fields, such as
`cash_contributions`, `noncash_contributions`,
`allo_other_contributions_line_1f`, and
`calculated_institutional_contributions_total`. These are not treated as main
revenue-source categories because cash/noncash describes contribution form,
not donor source, and the other fields are reconciliation helpers.

For every main raw-dollar variable, the script also creates a log-transformed level
variable:

```text
log1p_<amount_variable>
```

For every source component, the script creates a revenue share:

```text
<component>_share = component amount / total revenue
```

For `residual_other_revenue_share`, negative residuals are clipped to zero for
the share calculation. Negative residual rows are still reported separately in
the diagnostic outputs.

## Why Line 1f Is Treated as Mixed

The analysis deliberately avoids treating Form 990 Line 1f as either individual
giving or institutional giving. Line 1f, `ALLOOTHECONT`, combines donor types
that cannot be separated in the GivingTuesday basic all-forms data:

- Individual gifts.
- Private foundation grants.
- Donor-advised fund distributions.
- Corporate gifts.
- Bequests.
- Other unclassified contributions.

Without Schedule B or donor-level data, the analysis cannot isolate foundation
grants or individual gifts from Line 1f. The correct interpretation is:

- `government_grants_received`: clearly government/institutional.
- `federated_campaigns` and `related_org_contributions`: clearly institutional
  channels.
- `membership_dues` and `fundraising_events_contributions`: individual-adjacent
  proxies, not confirmed individual giving.
- `mixed_unclassified_contributions`: unknown mixed bucket.

## Focused Individual-vs-Institutional Contribution Analysis

The main revenue-share analysis divides components by total revenue. That is
appropriate for the Section 3 revenue-source question, but it can mix
contribution-source differences with program-service-revenue differences.

To answer the client question more directly, the script also builds a focused
Form 990-only frame. This frame is limited to:

- Form 990 rows.
- Rows with positive total contributions.

This is necessary because Form 990 is the only form family in the input that
exposes the detailed Part VIII Line 1 subcomponents needed for a donor-channel
analysis.

The focused analysis creates these contribution-channel variables:

| Focus variable | Meaning |
| --- | --- |
| `institutional_clear` | Form 990 Lines 1a + 1d + 1e: federated campaigns, related-organization contributions, and government grants. This is the clearest institutional contribution bucket. |
| `individual_narrow` | Form 990 Lines 1b + 1c: membership dues and fundraising-event contributions. This is a conservative lower-bound individual-giving proxy. |
| `line_1f_mixed` | Form 990 Line 1f. This is the mixed donor bucket that cannot be decomposed without more detailed data. |
| `individual_broad` | `individual_narrow + line_1f_mixed`. This is an upper-bound scenario that treats all Line 1f dollars as if they were individual giving. It is intentionally a bound, not a factual donor classification. |

The focused share variables divide each channel by total contributions:

- `institutional_clear_share_of_contrib`
- `individual_narrow_share_of_contrib`
- `line_1f_mixed_share_of_contrib`
- `individual_broad_share_of_contrib`

The aggregate stacked chart includes only the mutually exclusive channels:

- `institutional_clear`
- `individual_narrow`
- `line_1f_mixed`

It does not stack `individual_broad` because `individual_broad` includes
`line_1f_mixed` and would double-count that amount.

## Statistical Methods

The script uses several statistical views because the data are skewed,
zero-inflated, repeated across EINs, and compositional.

### Descriptive outputs

The workflow writes:

- Counts by region, comparison group, tax year, and form type.
- Missingness/nonzero coverage for amount and share variables.
- Summary statistics for amounts and shares.
- Aggregate revenue mixes by group, year, region, and form type.
- Concentration metrics by group and region.
- Negative residual and over-100-percent share diagnostics.

### Primary univariate tests

For Q9, the headline tests use organization-level raw dollar values for:

- `total_revenue`
- `program_service_revenue`
- `total_contributions`
- `government_grants_received`
- `federated_campaigns`
- `related_org_contributions`
- `membership_dues`
- `fundraising_events_contributions`
- `mixed_unclassified_contributions`

The primary test family is non-parametric:

- Kruskal-Wallis for five-region raw-dollar comparisons.
- Mann-Whitney U for Black Hills versus pooled benchmarks.
- Permutation mean-difference tests as mean-based robustness checks for Black
  Hills versus pooled benchmarks.

Raw-dollar medians, means, nonzero rates, and positive-only medians are written
so the tests can be interpreted without relying on means alone. Share-based and
compositional tests are retained as supplemental revenue-mix context.

P-values from the main family of univariate tests are adjusted using
Benjamini-Hochberg false discovery rate correction.

### Effect sizes

For Black Hills versus pooled benchmarks, the script reports practical effect
size rows:

- Mean difference.
- Standardized mean difference.
- Cliff's delta.
- Eta-squared.
- Omega-squared.

### Regression models

The script fits:

- OLS models with EIN-clustered standard errors.
- Logistic-presence models using GLM binomial with EIN-clustered standard
  errors.

These models include:

- `is_black_hills`
- Tax-year controls.
- Form-type controls.

The OLS models estimate differences in continuous revenue shares, raw-dollar
variables, or log-level variables. The logistic-presence models estimate whether
a row reports any positive amount in a revenue source, separating source
presence from source magnitude.

### Bootstrap confidence intervals

The script reports Black Hills minus benchmark mean differences with
EIN-cluster bootstrap confidence intervals. The bootstrap samples EINs, not
individual rows, so repeated filings by the same organization stay together.

### Multivariate/compositional tests

The revenue-source components form a composition, so the script also runs
overall revenue-mix tests:

- PERMANOVA-style permutation test on centered log-ratio transformed component
  shares.
- MANOVA using additive log-ratio transformed component shares and Pillai's
  trace.

The composition matrix clips negative residuals to zero and normalizes each
row's nonnegative displayed components to sum to one.

### Year-by-year tests

The script reruns primary tests separately by tax year when a custom multi-year
run is requested. This prevents pooled results from hiding a year-specific
pattern or being driven by incomplete filing coverage.

### Sensitivity analyses

When sensitivity analysis is enabled, the script runs:

- One-row-per-EIN sensitivity using the most recent available year.
- 2022-2023-only sensitivity.
- Form-type-specific sensitivity for 990, 990-EZ, and 990-PF where enough rows
  exist.
- Revenue-size-stratum sensitivity using small/medium/large tertiles.
- Full-universe sensitivity including hospitals, universities, and political
  organizations when the primary comparable-universe exclusion is active.
- NCCS Core 2022 sensitivity when the comparable NCCS Core file exists.

## Assumption Checks

`assumptions_check.py` reads the cleaned parquet written by the main analysis
and writes:

```text
results/assumptions_check_report.md
results/tables/assumptions_check_details.csv
```

It checks:

- Repeated-EIN structure and intraclass correlation.
- Normality within comparison groups for share variables.
- Homogeneity of variance across regions.
- Zero inflation in share variables.
- OLS residual heteroscedasticity and residual autocorrelation diagnostics.
- Logistic-presence quasi-separation risk.
- PERMANOVA dispersion homogeneity in CLR space.
- MANOVA ALR-axis skew and kurtosis.
- Exchangeability assumptions for permutation tests.
- Cluster count adequacy for cluster bootstrap.
- Composition closure of revenue-source segments.

Statuses are:

- `OK`: assumption is acceptable.
- `CAVEAT`: assumption is formally imperfect but mitigated by analysis design.
- `VIOLATED`: assumption is violated without mitigation.

The current assumption report has no `VIOLATED` rows.

## Outputs

The main workflow writes two output areas.

### Full analysis output directory

By default:

```text
DATA/analysis/revenue_sources_black_hills/
```

This contains:

- `cleaned_revenue_sources_analysis.parquet`
- `cleaned_revenue_sources_analysis.csv`
- `revenue_sources_methods_results_summary.md`
- `tables/`
- `figures/`

This is the complete run output, including intermediate tables.

### User-facing results bundle

By default:

```text
python/analysis/revenue_sources_black_hills/results/
```

This contains the polished report and selected tables/figures:

- `revenue_sources_black_hills_results.md`
- `assumptions_check_report.md`
- `tables/`
- `figures/`

The report is the best file to open first.

## Important Result Tables

| File | Purpose |
| --- | --- |
| `results/tables/counts.csv` | Row and unique-EIN counts by group, region, year, and form type. |
| `results/tables/mix_by_group.csv` | Aggregate revenue mix for Black Hills versus pooled benchmarks. |
| `results/tables/mix_by_group_year.csv` | Aggregate revenue mix by comparison group and tax year. |
| `results/tables/mix_by_region.csv` | Aggregate revenue mix by each region. |
| `results/tables/mix_by_group_form.csv` | Aggregate revenue mix by comparison group and form type. |
| `results/tables/statistical_tests_univariate.csv` | Primary, year-specific, and sensitivity univariate tests. |
| `results/tables/statistical_tests_by_year_univariate.csv` | Year-by-year univariate test table. |
| `results/tables/statistical_tests_regression.csv` | Clustered OLS and GLM-binomial presence models. |
| `results/tables/statistical_tests_multivariate.csv` | Pooled PERMANOVA and MANOVA tests. |
| `results/tables/statistical_tests_by_year_multivariate.csv` | Year-by-year PERMANOVA and MANOVA tests. |
| `results/tables/client_five_region_raw_level_rank_tests.csv` | Primary five-region raw-dollar Kruskal-Wallis tests. |
| `results/tables/client_bh_vs_benchmark_raw_level_rank_tests.csv` | Primary Black Hills versus pooled benchmark raw-dollar follow-up tests. |
| `results/tables/client_raw_level_region_summary.csv` | Raw-dollar medians, means, nonzero rates, and positive-only medians by region. |
| `results/tables/client_raw_level_normality_diagnostics.csv` | Raw and log1p normality/skew diagnostics for the primary variables. |
| `results/tables/bootstrap_mean_difference_ci.csv` | EIN cluster-bootstrap confidence intervals for supplemental share differences. |
| `results/tables/individual_focus_mix_by_group.csv` | Focused Form 990 contribution-channel mix by Black Hills versus benchmarks. |
| `results/tables/individual_focus_statistical_tests.csv` | Focused Form 990 contribution-channel tests. |
| `results/tables/individual_focus_bootstrap_ci.csv` | Focused contribution-channel cluster-bootstrap intervals. |
| `results/tables/concentration_by_group.csv` | Gini, HHI, and top-five revenue concentration by comparison group. |
| `results/tables/concentration_by_region.csv` | Gini, HHI, and top-five revenue concentration by region. |
| `results/tables/component_overlap_by_group.csv` | Reconciliation diagnostics by comparison group. |
| `results/tables/negative_residual_diagnostics.csv` | Rows with negative residuals or source shares above 100 percent. |
| `results/tables/assumptions_check_details.csv` | Machine-readable assumption-check details. |

## Important Figures

| File | Purpose |
| --- | --- |
| `results/figures/stacked_revenue_mix_black_hills_vs_benchmark.png` | Main normalized stacked bar chart comparing Black Hills and pooled benchmarks. |
| `results/figures/stacked_revenue_mix_by_year.png` | Normalized stacked mix by comparison group and tax year. |
| `results/figures/stacked_revenue_mix_by_region.png` | Normalized stacked mix by each region. |
| `results/figures/stacked_revenue_mix_by_form_type.png` | Normalized stacked mix by comparison group and form type. |
| `results/figures/violin_source_shares_by_group.png` | Organization-level distribution of source shares by group. |
| `results/figures/diagnostic_total_revenue_distribution.png` | Log-scale total revenue distribution. |
| `results/figures/diagnostic_residual_revenue.png` | Negative residual and over-100-percent share diagnostic scatter. |
| `results/figures/concentration_top5_revenue_share_by_region.png` | Top-five revenue concentration by region. |
| `results/figures/heatmap_revenue_source_share_by_year.png` | Reported component share heatmap by group and year. |
| `results/figures/individual_focus_contributions_mix.png` | Form 990 contribution-channel mix by Black Hills versus benchmarks. |
| `results/figures/individual_focus_contributions_mix_by_year.png` | Form 990 contribution-channel mix by group and year. |

## How to Run

From the repository root:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py
```

This default run:

- Uses the default project data root from `python/utils/paths.py`.
- Includes tax year 2022.
- Excludes hospitals, universities, and political organizations from the
  primary frame.
- Runs sensitivity analyses.
- Writes full outputs to `DATA/analysis/revenue_sources_black_hills/`.
- Copies the user-facing report bundle to
  `python/analysis/revenue_sources_black_hills/results/`.

Then run the assumptions checker:

```powershell
python python\analysis\revenue_sources_black_hills\assumptions_check.py
```

Run tests:

```powershell
python -m pytest tests\analysis -q
```

## Useful Command-Line Options

Use a different data root:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py --data-root C:\path\to\01_data
```

Use a custom output directory:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py --output-dir C:\path\to\analysis_output
```

Use a custom user-facing results directory:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py --results-dir C:\path\to\results
```

Run a custom set of years:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py --years 2022 2023
```

Make the full universe primary, including hospitals, universities, and political
organizations:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py --no-exclude-outliers
```

Disable sensitivity analyses:

```powershell
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py --no-include-sensitivity
```

## Interpretation Guidance

Use the five-region raw-dollar Kruskal-Wallis table to answer whether reported
revenue-source dollar distributions differ anywhere across the region set. Use
the Black Hills versus pooled benchmark raw-dollar Mann-Whitney/permutation
table to answer what is specifically different about Black Hills compared with
the benchmark regions together.

Read the stacked bars as aggregate-dollar mix charts, not as substitutes for
organization-level statistical tests. A few very large organizations can
strongly influence aggregate dollars, which is why the report includes both
stacked bars and concentration diagnostics.

Read `mixed_unclassified_contributions` cautiously. It is not "foundation grants" and
it is not "individual giving." It is the best available label for the basic
990-family bucket that combines multiple donor types.

Read `residual_other_revenue` as a derived accounting remainder. It helps the
components sum toward total revenue, but it should not be interpreted as a
source-reported revenue line.

For the individual-versus-institutional question, the focused Form 990-only
analysis is the most direct view:

- `institutional_clear_share_of_contrib` is the clearest institutional share.
- `individual_narrow_share_of_contrib` is the conservative individual-giving
  proxy.
- `line_1f_mixed_share_of_contrib` is the unresolved mixed bucket.
- `individual_broad_share_of_contrib` is an upper-bound scenario, not a direct
  observed donor type.

## Current High-Level Findings

- Five-region raw-dollar Kruskal-Wallis tests are significant for
  `total_contributions`, `government_grants_received`, `federated_campaigns`,
  `membership_dues`, `fundraising_events_contributions`, and
  `mixed_unclassified_contributions`.
- Five-region raw-dollar Kruskal-Wallis tests are not significant for
  `total_revenue`, `program_service_revenue`, or
  `related_org_contributions`.
- In the Black Hills versus pooled-benchmark raw-dollar follow-up, the clearest
  direct Black Hills signal is `government_grants_received` under Mann-Whitney U.
  The government-grants permutation mean-difference check is not significant,
  so the result is best read as a rank/nonzero-rate difference rather than a
  stable mean-dollar difference.
- Supplemental source-share and compositional tests still show that revenue mix
  differs across the comparison universe, but those tests are no longer the
  headline Q9 method because the shares are compositional.
- In the focused Form 990 contribution-channel analysis, Black Hills has a
  higher `institutional_clear_share_of_contrib`, lower
  `line_1f_mixed_share_of_contrib`, and no significant difference in the
  narrow individual-giving proxy.

## Known Caveats

- 2024 filing coverage may be incomplete.
- The same EIN can appear in multiple tax years; the script addresses this with
  clustered standard errors, cluster bootstrap intervals, and a one-row-per-EIN
  sensitivity.
- Many raw-dollar variables are zero-inflated and non-normal; the script uses
  Kruskal-Wallis, Mann-Whitney U, permutation tests, logistic presence models,
  and assumption diagnostics to triangulate.
- The IRS basic 990-family data cannot cleanly split Form 990 Line 1f into
  individuals, foundations, DAFs, corporations, and bequests.
- Form 990-EZ and 990-PF do not expose the same detailed contribution
  subcomponents as Form 990.
- Some rows have negative residuals or source-share sums above 100 percent.
  These are explicitly reported in diagnostic tables and should be reviewed
  before making claims about residual revenue.

## Maintenance Notes

The tests in `tests/analysis/test_revenue_sources_black_hills.py` cover the
most important derivations and helper behavior:

- Form 990 donor-channel decomposition.
- 990-EZ and 990-PF routing into the mixed contribution bucket.
- Negative residual and over-100-percent share diagnostics.
- Aggregate mix normalization.
- Hospital/university/political organization exclusion behavior.
- FDR adjustment behavior.
- Univariate and multivariate helper output structure.
- Assumption-check group-rate reporting.
- Default CLI exclusion behavior.
- Synthetic end-to-end output creation.

When changing the analysis, rerun:

```powershell
python -m pytest tests\analysis -q
python python\analysis\revenue_sources_black_hills\revenue_sources_black_hills.py
python python\analysis\revenue_sources_black_hills\assumptions_check.py
```

Then review:

- `results/revenue_sources_black_hills_results.md`
- `results/assumptions_check_report.md`
- `results/tables/statistical_tests_univariate.csv`
- `results/tables/individual_focus_statistical_tests.csv`
- `results/tables/negative_residual_diagnostics.csv`
