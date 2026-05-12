# Revenue Sources ANOVA Summary: Black Hills vs Benchmark Regions

## Client Question

For the limited number of organizations filing Form 990, Form 990-EZ, or
Form 990-PF:

> Is there a difference in the revenue sources between Black Hills and the
> benchmark regions?

## Short Answer

Yes. The revenue-source mix differs across the five regions in the analysis.

For the direct Black Hills versus pooled benchmark comparison, the clearest
statistically significant difference is:

- Black Hills organizations have a higher share of revenue from government
  grants received.

After false-discovery-rate adjustment, the other source-share differences are
not statistically significant in the direct Black Hills versus pooled benchmark
follow-up.

## Analysis Setup

| Requested item | Analysis implementation |
| --- | --- |
| Organization universe | Form 990, Form 990-EZ, and Form 990-PF organizations with positive total revenue and valid region/year/form data. |
| Primary peer-comparable frame | Excludes hospitals, universities, and political organizations because the client is not one of those organization types. |
| Full-universe check | Included as a sensitivity analysis; it does not change the substantive conclusion. |
| Dataset | GivingTuesday 990 basic all-forms analysis variables. |
| Years | 2022, 2023, and 2024. |
| Regions | Black Hills, Billings, Flagstaff, Missoula, and Sioux Falls. |
| Main statistical method | Welch ANOVA on organization-level revenue-source shares. |
| Follow-up method | Welch ANOVA for Black Hills versus pooled benchmark regions. |
| Presentation | Stacked bar graph of revenue-source mix by group and year. |

## Requested Variables

| Requested variable | How it was handled |
| --- | --- |
| Total revenue | Included as `total_revenue`; tested as `log1p_total_revenue` because revenue levels are highly skewed. |
| Program service revenue | Included as `program_service_revenue` for Form 990 and Form 990-EZ. Form 990-PF is kept missing for this component because the GivingTuesday basic file does not provide a comparable program-service revenue concept for PF filers. |
| Total contributions | Included as `total_contributions`; tested as `log1p_total_contributions`. Raw sources are `TOTACASHCONT` for 990, `CONGIFGRAETC` for 990-EZ, and `STREACGRTOIN` for 990-PF. |
| Other contributions, foundation grants, etc. | The available data cannot isolate foundation grants cleanly. Form 990 Line 1f is reported as `mixed_other_contributions` because it can include individual gifts, foundation grants, donor-advised fund distributions, corporate gifts, bequests, and other contribution types. |

## Revenue-Source Categories Used

The analysis used six revenue-source components:

| Component | Meaning |
| --- | --- |
| `program_service_revenue` | Program service revenue from Form 990 / 990-EZ. |
| `government_grants_received` | Form 990 government grants received, Line 1e. |
| `other_institutional_contributions` | Form 990 federated campaigns plus related-organization contributions, Lines 1a + 1d. |
| `individual_likely_contributions` | Form 990 membership dues plus fundraising-event contributions, Lines 1b + 1c. |
| `mixed_other_contributions` | Form 990 Line 1f, plus full total contributions for 990-EZ and 990-PF because those forms do not expose the same subcomponents. |
| `residual_other_revenue` | Total revenue not allocated to the five named components. |

## Year-by-Year Stacked Bar Chart

The stacked bar chart below shows the normalized revenue-source mix by
comparison group and tax year.

![Normalized reported component mix by comparison group and tax year](figures/stacked_revenue_mix_by_year.png)

Interpretation:

- The chart is an aggregate-dollar view.
- It shows how the revenue mix changes by year for Black Hills and benchmarks.
- It should be read together with the ANOVA results below because large
  organizations can strongly influence aggregate dollar charts.

## ANOVA Table 1: Five-Region Revenue-Source Share Differences

This is the main ANOVA table for whether revenue-source shares differ across
the five regions: Black Hills, Billings, Flagstaff, Missoula, and Sioux Falls.

The test is Welch ANOVA. The FDR-adjusted p-value controls for multiple
comparisons. Using FDR-adjusted p < 0.05, all six revenue-source share variables
show significant differences somewhere across the five regions.

| Revenue-source share variable | Welch statistic | P-value | FDR p-value | N | Significant after FDR? |
| --- | ---: | ---: | ---: | ---: | --- |
| `program_service_revenue_share` | 2.95 | 0.0194 | 0.0243 | 2,474 | Yes |
| `government_grants_received_share` | 9.89 | 6.59e-08 | 4.85e-07 | 4,179 | Yes |
| `other_institutional_contributions_share` | 3.07 | 0.0156 | 0.0203 | 4,179 | Yes |
| `individual_likely_contributions_share` | 5.59 | 0.000181 | 0.000462 | 4,179 | Yes |
| `mixed_other_contributions_share` | 6.96 | 1.48e-05 | 3.51e-05 | 4,179 | Yes |
| `residual_other_revenue_share` | 10.1 | 4.05e-08 | 2.80e-07 | 4,179 | Yes |

Conclusion from Table 1:

> Yes, revenue-source shares differ across the region set.

## ANOVA Table 2: Black Hills vs Pooled Benchmark Regions

This table answers the narrower follow-up question: which revenue-source shares
are different when Black Hills is compared directly with all benchmark regions
pooled together?

Using FDR-adjusted p < 0.05, only `government_grants_received_share` is
significantly different.

| Revenue-source share variable | Welch statistic | P-value | FDR p-value | N | Significant after FDR? | Direction |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| `program_service_revenue_share` | 3.34 | 0.0680 | 0.112 | 2,474 | No | Black Hills lower, not significant |
| `government_grants_received_share` | 26.4 | 3.21e-07 | 5.91e-06 | 4,179 | Yes | Black Hills higher |
| `other_institutional_contributions_share` | 0.0726 | 0.788 | 0.868 | 4,179 | No | No meaningful difference |
| `individual_likely_contributions_share` | 4.79 | 0.0286 | 0.0732 | 4,179 | No after FDR | Black Hills lower, not significant after adjustment |
| `mixed_other_contributions_share` | 3.57 | 0.0590 | 0.104 | 4,179 | No | Black Hills higher, not significant |
| `residual_other_revenue_share` | 5.54 | 0.0187 | 0.0556 | 4,179 | No after FDR | Black Hills lower, borderline after adjustment |

Conclusion from Table 2:

> In the direct Black Hills versus pooled benchmark comparison, the statistically
> significant revenue-source difference is higher government grants received
> share in Black Hills.

## ANOVA Table 3: Requested Level Variables

The revenue-source-share ANOVAs answer the composition question. The table below
also reports Welch ANOVA tests for the requested level variables, using
log-transformed amounts because revenue amounts are highly skewed.

| Level variable | Welch statistic | P-value | FDR p-value | N | Significant after FDR? |
| --- | ---: | ---: | ---: | ---: | --- |
| `log1p_total_revenue` | 2.19 | 0.0675 | 0.0776 | 4,179 | No |
| `log1p_program_service_revenue` | 1.57 | 0.181 | 0.192 | 2,474 | No |
| `log1p_total_contributions` | 9.81 | 7.85e-08 | 3.20e-07 | 3,463 | Yes |
| `log1p_government_grants_received` | 7.95 | 2.40e-06 | 6.91e-06 | 4,179 | Yes |
| `log1p_other_institutional_contributions` | 3.36 | 0.00942 | 0.0130 | 4,179 | Yes |
| `log1p_individual_likely_contributions` | 8.14 | 1.69e-06 | 5.57e-06 | 4,179 | Yes |
| `log1p_mixed_other_contributions` | 11.1 | 6.78e-09 | 7.88e-08 | 4,179 | Yes |

Interpretation:

- Total revenue levels do not significantly differ across regions after FDR
  adjustment.
- Program service revenue levels do not significantly differ across regions
  after FDR adjustment.
- Total contributions and several contribution subcomponents do differ across
  regions.
- Level differences are not the same as revenue-mix differences; the main
  answer to the client question should rely on the source-share tables.

## Final Answer

Yes, there is evidence that revenue sources differ between Black Hills and the
benchmark-region universe.

The strongest and most defensible finding is that Black Hills organizations have
a higher share of revenue from government grants received compared with the
pooled benchmark regions. Across the broader five-region ANOVA, all six
revenue-source share variables show statistically significant differences.

However, when Black Hills is compared directly with the pooled benchmark
regions, most individual source-share differences are not significant after
multiple-comparison adjustment. The direct significant difference is government
grants received share.

Important caveat: the available GivingTuesday basic 990-family data cannot
cleanly identify foundation grants as a standalone category. Those dollars may
be inside the mixed Form 990 Line 1f bucket, but Line 1f also includes other
donor types. Therefore, the analysis labels that category as
`mixed_other_contributions` rather than treating it as foundation grants.
