# Q9: Is there a difference in revenue sources between Black Hills and benchmark regions?

> **Analysis artifact status reviewed 2026-07-23:** this document reports the retained analysis results; no data rerun occurred during the documentation review.

For the limited number of organizations filing **Form 990, Form 990-EZ, or Form 990-PF**.

- Primary dataset: GivingTuesday Form 990-family basic all-forms analysis file.
- Years included: 2022, 2023, and 2024.
- Regions compared: Black Hills, Billings, Flagstaff, Missoula, and Sioux Falls.
- Primary filtered universe excludes hospitals, universities, and political organizations because the client is not one of those organization types.
- Final primary analysis frame: 4,179 organization-year rows.

## Short answer

**Yes. Revenue-source dollar distributions differ across the five-region benchmark set.**

The analysis now uses more detailed contribution-source subcategories where the IRS fields support them. Using organization-level raw dollar amounts and p < 0.05:

- Across the five regions, **total contributions**, **government grants**, **federated campaign contributions**, **membership dues**, **fundraising event contributions**, and **mixed / unclassified contributions** differ significantly.
- **Total revenue**, **program service revenue**, and **related organization contributions** do not differ significantly across the five-region Kruskal-Wallis test.
- In the direct Black Hills vs pooled benchmark comparison, **government grants** remains the clearest Black Hills-specific result.

The clean client-facing conclusion is:

> Revenue-source patterns differ across the five-region benchmark set, especially within contribution sources. For Black Hills specifically, the clearest direct raw-dollar signal is government grants: more Black Hills organization-years report government grants or rank higher on government-grant dollars than the pooled benchmark universe. Because grant dollars are highly skewed, this should not be described as a higher average grant-dollar amount.

## Revenue-source categories

The final client-facing revenue partition is:

```text
Total revenue =
  earned program revenue
  + government grants
  + federated campaign contributions
  + related organization contributions
  + membership dues
  + fundraising event contributions
  + mixed / unclassified contributions
  + other revenue
```

Contribution dollars reconcile as:

```text
Total contributions =
  government grants
  + federated campaign contributions
  + related organization contributions
  + membership dues
  + fundraising event contributions
  + mixed / unclassified contributions
```

The current cleaned data passes this reconciliation exactly for every row.

| Client-facing category | Analysis variable | Source / calculation |
| --- | --- | --- |
| Total revenue | `total_revenue` | Form 990 `TOTREVCURYEA`; 990-EZ `TOTALRREVENU`; 990-PF `ANREEXTOREEX`. |
| Earned program revenue | `program_service_revenue` | Form 990 `TOTPROSERREV`; 990-EZ `PROGSERVREVE`; 990-PF kept missing because this file does not provide a comparable PF concept. Blank supported 990/990-EZ lines are treated as zero. |
| Total contributions | `total_contributions` | Form 990 `TOTACASHCONT`; 990-EZ `CONGIFGRAETC`; 990-PF `STREACGRTOIN`. Blank supported lines are treated as zero. |
| Government grants | `government_grants_received` | Form 990 Line 1e `GOVERNGRANTS`; 990-EZ/PF rows are excluded for this variable because those forms do not expose the same subcomponent. |
| Federated campaign contributions | `federated_campaigns` | Form 990 Line 1a `FEDERACAMPAI`; 990-EZ/PF rows are excluded for this variable because source detail is unavailable. |
| Related organization contributions | `related_org_contributions` | Form 990 Line 1d `RELATEORGANI`; 990-EZ/PF rows are excluded for this variable because source detail is unavailable. |
| Membership dues | `membership_dues` | Form 990 Line 1b `MEMBERDUESUE`; individual-adjacent, not pure individual giving. 990-EZ/PF rows are excluded for this variable. |
| Fundraising event contributions | `fundraising_events_contributions` | Form 990 Line 1c `FUNDRAEVENTS`; individual-adjacent, not pure individual giving. 990-EZ/PF rows are excluded for this variable. |
| Mixed / unclassified contributions | `mixed_unclassified_contributions` | Form 990 Line 1f `ALLOOTHECONT`, plus full 990-EZ/PF contribution totals because those forms cannot be decomposed into Form 990 Line 1a-1f detail. |
| Other revenue | `residual_other_revenue` | Total revenue minus the listed program-service and contribution-source components. |

Supported blank amount lines are interpreted as reported zero. Fields that a form cannot report are left missing and excluded from that variable's tests and medians, so unavailable 990-EZ/PF detailed source fields are not counted as zeros.

## Why the test uses raw dollars

The earlier share-based version was useful for describing revenue mix, but source shares are compositional. If all parts add to 1, then the parts are mathematically linked.

The primary Q9 test now uses the original organization-level **raw dollar values**:

```text
raw value tested = each organization's reported dollar amount for one revenue source
```

The tests are non-parametric because the dollar values are highly skewed.

## Normality and skew diagnostics

The diagnostics support using rank-based tests:

- D'Agostino-Pearson normality tests reject normality for every primary raw-dollar variable in every region.
- Applying `log1p` does not fix the issue; log-transformed values also reject normality.
- Means are much larger than medians, confirming strong right skew.
- Several contribution subcategories are zero-heavy, so nonzero rates are often more interpretable than medians.

Examples from the raw-dollar summaries:

| Variable | Group | Mean | Median | Nonzero % | Positive-only median |
| --- | --- | ---: | ---: | ---: | ---: |
| Total revenue | Black Hills | $1,627,070 | $166,429 | 100.0% | $166,429 |
| Total revenue | Benchmark pooled | $2,899,414 | $187,164 | 100.0% | $187,164 |
| Program service revenue | Black Hills | $1,076,974 | $69,260 | 85.0% | $102,946 |
| Program service revenue | Benchmark pooled | $2,874,091 | $92,582 | 83.0% | $141,208 |
| Total contributions | Black Hills | $1,019,109 | $95,663 | 96.3% | $105,190 |
| Total contributions | Benchmark pooled | $1,044,768 | $92,215 | 91.7% | $108,404 |
| Government grants | Black Hills | $430,911 | $0 | 25.6% | $162,584 |
| Government grants | Benchmark pooled | $269,963 | $0 | 18.8% | $227,258 |
| Membership dues | Black Hills | $5,453 | $0 | 11.7% | $21,825 |
| Membership dues | Benchmark pooled | $16,337 | $0 | 8.6% | $26,784 |
| Fundraising event contributions | Black Hills | $3,829 | $0 | 8.7% | $13,731 |
| Fundraising event contributions | Benchmark pooled | $11,883 | $0 | 10.4% | $45,360 |
| Mixed / unclassified contributions | Black Hills | $398,666 | $22,672 | 74.4% | $63,592 |
| Mixed / unclassified contributions | Benchmark pooled | $514,002 | $24,164 | 69.2% | $77,432 |

## Primary raw-dollar visualization

![Median raw revenue-source dollars by region](assets/q9/client_raw_level_median_bars_by_region.png)

This chart mirrors the primary statistical comparison. Most panels show the median organization-level raw dollar value for one revenue variable by region. For zero-heavy categories with a $0 median in every region, the panel shows the nonzero reporting rate instead.

![Raw-dollar organization-level distributions by region](assets/q9/client_raw_level_distribution_by_region.png)

This companion chart shows organization-level raw-dollar distributions on a `log1p` display scale. The log scale is only for readability; the primary Kruskal-Wallis tests are run on raw dollar values.

## Statistical method

The primary raw-dollar analysis uses:

- **Kruskal-Wallis** for five-region comparisons.
- **Mann-Whitney U** for the direct Black Hills vs pooled benchmark comparison.
- **Permutation mean-difference test** as a mean-based robustness check.

The tables below report p-values at the 0.05 threshold used in the primary tests.

## Five-region raw-dollar Kruskal-Wallis results

| Raw-dollar variable | Kruskal-Wallis H | p-value | Result |
| --- | ---: | ---: | --- |
| Total revenue | 6.772 | 0.1484 | Not significant |
| Program service revenue | 7.228 | 0.1243 | Not significant |
| Total contributions | 16.535 | 0.0024 | Significant |
| Government grants | 31.959 | <0.001 | Significant |
| Federated campaign contributions | 17.610 | 0.0015 | Significant |
| Related organization contributions | 6.189 | 0.1855 | Not significant |
| Membership dues | 39.370 | <0.001 | Significant |
| Fundraising event contributions | 31.266 | <0.001 | Significant |
| Mixed / unclassified contributions | 34.261 | <0.001 | Significant |

Interpretation:

- Contribution-related sources show more regional differentiation than total revenue or program service revenue.
- Kruskal-Wallis tells us that at least one region differs somewhere in the five-region set. It does not identify the specific pair without follow-up tests.

## Black Hills vs pooled benchmark raw-dollar follow-up

The direct client comparison is Black Hills versus Billings, Flagstaff, Sioux Falls, and Missoula pooled together.

| Raw-dollar variable | Test | Direction | p-value | Interpretation |
| --- | --- | --- | ---: | --- |
| Total revenue | Mann-Whitney U | Black Hills lower | 0.0237 | Significant by Mann-Whitney U only |
| Total revenue | Permutation mean difference | Black Hills lower | 0.1279 | Not significant |
| Program service revenue | Mann-Whitney U | Black Hills lower | 0.0895 | Not significant |
| Program service revenue | Permutation mean difference | Black Hills lower | 0.1469 | Not significant |
| Total contributions | Mann-Whitney U | Black Hills higher | 0.3934 | Not significant |
| Total contributions | Permutation mean difference | Black Hills lower | 0.9195 | Not significant |
| Government grants | Mann-Whitney U | Black Hills higher | <0.001 | **Significant** |
| Government grants | Permutation mean difference | Black Hills higher | 0.1139 | Not significant |
| Federated campaign contributions | Mann-Whitney U | Black Hills higher | 0.0352 | Significant by Mann-Whitney U only |
| Federated campaign contributions | Permutation mean difference | Black Hills lower | 0.1429 | Not significant |
| Related organization contributions | Mann-Whitney U | Black Hills lower | 0.1580 | Not significant |
| Related organization contributions | Permutation mean difference | Black Hills lower | 0.4643 | Not significant |
| Membership dues | Mann-Whitney U | Black Hills higher | 0.0051 | **Significant** |
| Membership dues | Permutation mean difference | Black Hills lower | 0.0190 | Significant by permutation mean difference |
| Fundraising event contributions | Mann-Whitney U | Black Hills lower | 0.0735 | Not significant |
| Fundraising event contributions | Permutation mean difference | Black Hills lower | 0.0010 | **Significant** |
| Mixed / unclassified contributions | Mann-Whitney U | Black Hills lower | 0.3832 | Not significant |
| Mixed / unclassified contributions | Permutation mean difference | Black Hills lower | 0.4873 | Not significant |

Direct pooled-benchmark interpretation:

- **Government grants** is the clearest Black Hills raw-dollar difference under the rank test.
- **Membership dues** also differs by Mann-Whitney U, but the mean-difference direction is lower for Black Hills because dollars are highly skewed.
- **Fundraising event contributions** is lower for Black Hills by the permutation mean-difference test.
- **Mixed / unclassified contributions** does not show a clear direct Black Hills difference.
- Total revenue is significant by Mann-Whitney U at p < 0.05, but not by the permutation mean-difference test, so it should be treated as suggestive rather than a headline finding.

## Descriptive revenue-mix chart

![Revenue-source mix by region](assets/q9/client_stacked_revenue_mix_by_five_regions.png)

The stacked bars show normalized **aggregate-dollar** revenue-source mix. They sum dollars within each region, then convert those regional totals to percentages. These charts are useful for presentation, but they are **not** the values used in the primary statistical tests.

## Supplemental compositional revenue-mix context

The share-based tests and PERMANOVA-style test remain available as supplemental context when the client wants to talk specifically about revenue mix.

These findings support the idea that revenue mix differs, but because the shares are compositional, they should be framed as context rather than the main Q9 test.

## Answer

**Yes. There are statistically significant differences in revenue-source dollar distributions across Black Hills and benchmark regions.**

The best primary answer is raw-dollar based:

- Five-region Kruskal-Wallis tests show significant differences for **total contributions**, **government grants**, **federated campaign contributions**, **membership dues**, **fundraising event contributions**, **mixed / unclassified contributions**, and **other revenue**.
- Five-region tests do **not** show significant differences for **total revenue**, **program service revenue**, or **related organization contributions**.
- In the direct Black Hills vs pooled benchmark comparison, the clearest Black Hills-specific result remains **government grants** under Mann-Whitney U.
- Because the permutation mean-difference test is not significant for government grants, the safest interpretation is not "Black Hills has a higher average grant dollar amount." It is: **Black Hills has a higher rank distribution / higher likelihood of reporting government grants than the pooled benchmark universe.**

## Caveat

- The available Form 990-family data cannot cleanly split individual giving from foundation grants and other institutional contributions.
- Form 990 Line 1f mixes individual gifts, private foundation grants, donor-advised fund distributions, corporate gifts, bequests, and other contribution types.
- Form 990-EZ and Form 990-PF report contribution totals that cannot be decomposed into the same detailed Form 990 subcomponents.
- Membership dues and fundraising event contributions are individual-adjacent proxies, not confirmed individual giving.
- Share variables are compositional because they are parts of a whole. The separate share tests are therefore supplemental, while the primary Q9 tests use raw dollar values.
