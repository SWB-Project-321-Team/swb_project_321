# Revenue-Sources Assumptions Check

This report empirically checks the assumptions behind every test reported in `revenue_sources_black_hills_results.md`. Each row in the table below is one assumption, evaluated for one variable (and where relevant, one group). The status column flags whether the assumption holds (`OK`), is formally violated but mitigated by the analysis design (`CAVEAT`), or is violated without any mitigation in place (`VIOLATED`).

- Org-years analyzed: 4,179
- Unique EINs: 2,155
- Tax years: ['2022', '2023', '2024']
- Regions: ['Billings', 'Black Hills', 'Flagstaff', 'Missoula', 'Sioux Falls']

## Summary

- OK rows: 36
- CAVEAT rows: 35
- VIOLATED rows: 0

**Verdict:** every assumption that is formally violated by the data is mitigated by the analysis design (cluster-robust SEs, Welch correction, distribution-free permutation, MANOVA-Pillai, one-row-per-EIN sensitivity, or cluster bootstrap). The headline finding (regions differ on revenue mix; Black Hills is distinguished by a higher government-grants share) is robust to the assumption violations identified below.

## all_pooled_tests

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| all_pooled_tests | independence_of_observations |  |  | 1.9392 |  | CAVEAT | 4,179 org-years from 2,155 EINs (1.94 filings/EIN). Mitigated by EIN-clustered OLS, EIN-clustered logistic, EIN-cluster bootstrap CIs, and a one-row-per-EIN sensitivity in the main script. The Welch ANOVA itself does not have a cluster-robust variant; the one-row-per-EIN sensitivity covers it. |

## welch_anova_share

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| welch_anova_share | independence_of_observations | program_service_revenue_share |  | 0.9083 |  | CAVEAT | ICC1 from one-way ANOVA decomposition by EIN. ICC near zero means org-years behave independently for this share; larger ICC means the same EIN tends to report similar shares across years. Cluster-robust SEs (OLS, logistic) and cluster bootstrap account for this; Welch ANOVA does not, but the one-row-per-EIN sensitivity in the main report does. |
| welch_anova_share | independence_of_observations | government_grants_received_share |  | 0.7998 |  | CAVEAT | ICC1 from one-way ANOVA decomposition by EIN. ICC near zero means org-years behave independently for this share; larger ICC means the same EIN tends to report similar shares across years. Cluster-robust SEs (OLS, logistic) and cluster bootstrap account for this; Welch ANOVA does not, but the one-row-per-EIN sensitivity in the main report does. |
| welch_anova_share | independence_of_observations | other_institutional_contributions_share |  | 0.6878 |  | CAVEAT | ICC1 from one-way ANOVA decomposition by EIN. ICC near zero means org-years behave independently for this share; larger ICC means the same EIN tends to report similar shares across years. Cluster-robust SEs (OLS, logistic) and cluster bootstrap account for this; Welch ANOVA does not, but the one-row-per-EIN sensitivity in the main report does. |
| welch_anova_share | independence_of_observations | individual_likely_contributions_share |  | 0.4405 |  | CAVEAT | ICC1 from one-way ANOVA decomposition by EIN. ICC near zero means org-years behave independently for this share; larger ICC means the same EIN tends to report similar shares across years. Cluster-robust SEs (OLS, logistic) and cluster bootstrap account for this; Welch ANOVA does not, but the one-row-per-EIN sensitivity in the main report does. |
| welch_anova_share | independence_of_observations | mixed_other_contributions_share |  | 0.851 |  | CAVEAT | ICC1 from one-way ANOVA decomposition by EIN. ICC near zero means org-years behave independently for this share; larger ICC means the same EIN tends to report similar shares across years. Cluster-robust SEs (OLS, logistic) and cluster bootstrap account for this; Welch ANOVA does not, but the one-row-per-EIN sensitivity in the main report does. |
| welch_anova_share | independence_of_observations | residual_other_revenue_share |  | 0.8987 |  | CAVEAT | ICC1 from one-way ANOVA decomposition by EIN. ICC near zero means org-years behave independently for this share; larger ICC means the same EIN tends to report similar shares across years. Cluster-robust SEs (OLS, logistic) and cluster bootstrap account for this; Welch ANOVA does not, but the one-row-per-EIN sensitivity in the main report does. |
| welch_anova_share | approximate_normality_within_group | program_service_revenue_share | Benchmark | 1833.8385 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=3.94, excess_kurtosis=71.24. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | program_service_revenue_share | Black Hills | 10688.9917 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=0.31, excess_kurtosis=-1.41. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | government_grants_received_share | Benchmark | 3233.1498 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=4.90, excess_kurtosis=41.49. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | government_grants_received_share | Black Hills | 680.7178 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=3.06, excess_kurtosis=12.58. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | other_institutional_contributions_share | Benchmark | 4889.7452 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=9.79, excess_kurtosis=107.31. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | other_institutional_contributions_share | Black Hills | 1671.8686 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=10.90, excess_kurtosis=123.40. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | individual_likely_contributions_share | Benchmark | 6975.1087 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=19.37, excess_kurtosis=659.21. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | individual_likely_contributions_share | Black Hills | 1509.5722 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=8.91, excess_kurtosis=102.95. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | mixed_other_contributions_share | Benchmark | 6325.0037 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=15.40, excess_kurtosis=523.84. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | mixed_other_contributions_share | Black Hills | 665.3136 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=2.59, excess_kurtosis=21.15. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | residual_other_revenue_share | Benchmark | 527.4067 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=1.06, excess_kurtosis=-0.63. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | approximate_normality_within_group | residual_other_revenue_share | Black Hills | 166.9641 | 0.0 | CAVEAT | D'Agostino-Pearson omnibus test; skew=1.25, excess_kurtosis=-0.05. Welch ANOVA with thousands of observations per group is robust to non-normality, and the analysis triangulates with rank tests and permutation tests; treat as CAVEAT, not blocker. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | program_service_revenue_share | Benchmark | 0.1702 |  | OK | 17.0% of 1,909 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | program_service_revenue_share | Black Hills | 0.1504 |  | OK | 15.0% of 565 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | government_grants_received_share | Benchmark | 0.8119 |  | CAVEAT | 81.2% of 3,180 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | government_grants_received_share | Black Hills | 0.7437 |  | CAVEAT | 74.4% of 999 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | other_institutional_contributions_share | Benchmark | 0.9481 |  | CAVEAT | 94.8% of 3,180 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | other_institutional_contributions_share | Black Hills | 0.9439 |  | CAVEAT | 94.4% of 999 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | individual_likely_contributions_share | Benchmark | 0.8302 |  | CAVEAT | 83.0% of 3,180 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | individual_likely_contributions_share | Black Hills | 0.8208 |  | CAVEAT | 82.1% of 999 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | mixed_other_contributions_share | Benchmark | 0.3082 |  | OK | 30.8% of 3,180 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | mixed_other_contributions_share | Black Hills | 0.2563 |  | OK | 25.6% of 999 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | residual_other_revenue_share | Benchmark | 0.2028 |  | OK | 20.3% of 3,180 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |
| welch_anova_share | continuous_outcome_minimal_zero_inflation | residual_other_revenue_share | Black Hills | 0.1882 |  | OK | 18.8% of 999 rows have a zero share. The analysis pairs each share Welch ANOVA with a logistic presence model (does the org report any of this source) so size and presence effects can be separated. |

## anova_share

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| anova_share | homogeneity_of_variance | program_service_revenue_share | region_label | 3.3805 | 0.0091 | CAVEAT | Variance is unequal across regions, so classical ANOVA's homoscedasticity assumption is violated. The headline test in this analysis is Welch ANOVA, which does not assume equal variances; treat the classical ANOVA row as secondary. |
| anova_share | homogeneity_of_variance | government_grants_received_share | region_label | 11.3519 | 0.0 | CAVEAT | Variance is unequal across regions, so classical ANOVA's homoscedasticity assumption is violated. The headline test in this analysis is Welch ANOVA, which does not assume equal variances; treat the classical ANOVA row as secondary. |
| anova_share | homogeneity_of_variance | other_institutional_contributions_share | region_label | 1.3565 | 0.2465 | OK | Variances comparable across regions. |
| anova_share | homogeneity_of_variance | individual_likely_contributions_share | region_label | 5.8694 | 0.0001 | CAVEAT | Variance is unequal across regions, so classical ANOVA's homoscedasticity assumption is violated. The headline test in this analysis is Welch ANOVA, which does not assume equal variances; treat the classical ANOVA row as secondary. |
| anova_share | homogeneity_of_variance | mixed_other_contributions_share | region_label | 3.5351 | 0.0069 | CAVEAT | Variance is unequal across regions, so classical ANOVA's homoscedasticity assumption is violated. The headline test in this analysis is Welch ANOVA, which does not assume equal variances; treat the classical ANOVA row as secondary. |
| anova_share | homogeneity_of_variance | residual_other_revenue_share | region_label | 10.996 | 0.0 | CAVEAT | Variance is unequal across regions, so classical ANOVA's homoscedasticity assumption is violated. The headline test in this analysis is Welch ANOVA, which does not assume equal variances; treat the classical ANOVA row as secondary. |

## ols_clustered_by_ein

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ols_clustered_by_ein | homoscedasticity_of_residuals | program_service_revenue_share |  | 4.2684 | 0.3709 | OK | Breusch-Pagan test for heteroscedasticity. Cluster-robust SEs in the main analysis already adjust for both heteroscedasticity and within-EIN correlation, so a CAVEAT here does not invalidate inference. |
| ols_clustered_by_ein | independence_of_residuals_durbin_watson | program_service_revenue_share |  | 2.0183 |  | OK | Durbin-Watson statistic. Values far from 2 indicate residual autocorrelation (typically within EIN across years). Cluster-robust SEs handle this; reported for transparency. |
| ols_clustered_by_ein | homoscedasticity_of_residuals | government_grants_received_share |  | 103.5853 | 0.0 | CAVEAT | Breusch-Pagan test for heteroscedasticity. Cluster-robust SEs in the main analysis already adjust for both heteroscedasticity and within-EIN correlation, so a CAVEAT here does not invalidate inference. |
| ols_clustered_by_ein | independence_of_residuals_durbin_watson | government_grants_received_share |  | 1.9838 |  | OK | Durbin-Watson statistic. Values far from 2 indicate residual autocorrelation (typically within EIN across years). Cluster-robust SEs handle this; reported for transparency. |
| ols_clustered_by_ein | homoscedasticity_of_residuals | other_institutional_contributions_share |  | 29.2224 | 0.0 | CAVEAT | Breusch-Pagan test for heteroscedasticity. Cluster-robust SEs in the main analysis already adjust for both heteroscedasticity and within-EIN correlation, so a CAVEAT here does not invalidate inference. |
| ols_clustered_by_ein | independence_of_residuals_durbin_watson | other_institutional_contributions_share |  | 2.0498 |  | OK | Durbin-Watson statistic. Values far from 2 indicate residual autocorrelation (typically within EIN across years). Cluster-robust SEs handle this; reported for transparency. |
| ols_clustered_by_ein | homoscedasticity_of_residuals | individual_likely_contributions_share |  | 5.9814 | 0.308 | OK | Breusch-Pagan test for heteroscedasticity. Cluster-robust SEs in the main analysis already adjust for both heteroscedasticity and within-EIN correlation, so a CAVEAT here does not invalidate inference. |
| ols_clustered_by_ein | independence_of_residuals_durbin_watson | individual_likely_contributions_share |  | 1.9837 |  | OK | Durbin-Watson statistic. Values far from 2 indicate residual autocorrelation (typically within EIN across years). Cluster-robust SEs handle this; reported for transparency. |
| ols_clustered_by_ein | homoscedasticity_of_residuals | mixed_other_contributions_share |  | 6.0505 | 0.3013 | OK | Breusch-Pagan test for heteroscedasticity. Cluster-robust SEs in the main analysis already adjust for both heteroscedasticity and within-EIN correlation, so a CAVEAT here does not invalidate inference. |
| ols_clustered_by_ein | independence_of_residuals_durbin_watson | mixed_other_contributions_share |  | 1.9821 |  | OK | Durbin-Watson statistic. Values far from 2 indicate residual autocorrelation (typically within EIN across years). Cluster-robust SEs handle this; reported for transparency. |
| ols_clustered_by_ein | homoscedasticity_of_residuals | residual_other_revenue_share |  | 342.4993 | 0.0 | CAVEAT | Breusch-Pagan test for heteroscedasticity. Cluster-robust SEs in the main analysis already adjust for both heteroscedasticity and within-EIN correlation, so a CAVEAT here does not invalidate inference. |
| ols_clustered_by_ein | independence_of_residuals_durbin_watson | residual_other_revenue_share |  | 1.9492 |  | OK | Durbin-Watson statistic. Values far from 2 indicate residual autocorrelation (typically within EIN across years). Cluster-robust SEs handle this; reported for transparency. |

## logistic_presence_clustered_by_ein

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| logistic_presence_clustered_by_ein | absence_of_quasi_separation | program_service_revenue_share |  | 0.8298 |  | OK | Presence rates by group: BH=0.850, Benchmark=0.830. Rates near 0 or 1 indicate quasi-separation risk; large coefficients with large SEs in the regression table for this variable should be interpreted with caution. |
| logistic_presence_clustered_by_ein | absence_of_quasi_separation | government_grants_received_share |  | 0.1881 |  | OK | Presence rates by group: BH=0.256, Benchmark=0.188. Rates near 0 or 1 indicate quasi-separation risk; large coefficients with large SEs in the regression table for this variable should be interpreted with caution. |
| logistic_presence_clustered_by_ein | absence_of_quasi_separation | other_institutional_contributions_share |  | 0.0519 |  | OK | Presence rates by group: BH=0.056, Benchmark=0.052. Rates near 0 or 1 indicate quasi-separation risk; large coefficients with large SEs in the regression table for this variable should be interpreted with caution. |
| logistic_presence_clustered_by_ein | absence_of_quasi_separation | individual_likely_contributions_share |  | 0.1698 |  | OK | Presence rates by group: BH=0.179, Benchmark=0.170. Rates near 0 or 1 indicate quasi-separation risk; large coefficients with large SEs in the regression table for this variable should be interpreted with caution. |
| logistic_presence_clustered_by_ein | absence_of_quasi_separation | mixed_other_contributions_share |  | 0.6918 |  | OK | Presence rates by group: BH=0.744, Benchmark=0.692. Rates near 0 or 1 indicate quasi-separation risk; large coefficients with large SEs in the regression table for this variable should be interpreted with caution. |
| logistic_presence_clustered_by_ein | absence_of_quasi_separation | residual_other_revenue_share |  | 0.7972 |  | OK | Presence rates by group: BH=0.812, Benchmark=0.797. Rates near 0 or 1 indicate quasi-separation risk; large coefficients with large SEs in the regression table for this variable should be interpreted with caution. |

## permanova_clr

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| permanova_clr | similar_within_group_dispersion | clr_program_service_revenue |  | 0.0 | 0.9983 | OK | Levene-on-CLR-axis as a coarse PERMDISP-style check. CAVEAT means part of the PERMANOVA signal could reflect spread differences rather than location differences; the MANOVA-Pillai test triangulates the conclusion. |
| permanova_clr | similar_within_group_dispersion | clr_government_grants_received |  | 0.0048 | 0.9451 | OK | Levene-on-CLR-axis as a coarse PERMDISP-style check. CAVEAT means part of the PERMANOVA signal could reflect spread differences rather than location differences; the MANOVA-Pillai test triangulates the conclusion. |
| permanova_clr | similar_within_group_dispersion | clr_other_institutional_contributions |  | 0.0047 | 0.946 | OK | Levene-on-CLR-axis as a coarse PERMDISP-style check. CAVEAT means part of the PERMANOVA signal could reflect spread differences rather than location differences; the MANOVA-Pillai test triangulates the conclusion. |
| permanova_clr | similar_within_group_dispersion | clr_individual_likely_contributions |  | 0.09 | 0.7662 | OK | Levene-on-CLR-axis as a coarse PERMDISP-style check. CAVEAT means part of the PERMANOVA signal could reflect spread differences rather than location differences; the MANOVA-Pillai test triangulates the conclusion. |
| permanova_clr | similar_within_group_dispersion | clr_mixed_other_contributions |  | 1.2475 | 0.2729 | OK | Levene-on-CLR-axis as a coarse PERMDISP-style check. CAVEAT means part of the PERMANOVA signal could reflect spread differences rather than location differences; the MANOVA-Pillai test triangulates the conclusion. |
| permanova_clr | similar_within_group_dispersion | clr_residual_other_revenue |  | 0.1029 | 0.7506 | OK | Levene-on-CLR-axis as a coarse PERMDISP-style check. CAVEAT means part of the PERMANOVA signal could reflect spread differences rather than location differences; the MANOVA-Pillai test triangulates the conclusion. |
| permanova_clr | exchangeability_under_H0 |  |  |  |  | CAVEAT | Same row-level permutation caveat as above. PERMDISP-style dispersion check is reported per CLR axis; the MANOVA-Pillai test provides parametric triangulation. |

## manova_alr_pillai

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| manova_alr_pillai | approximate_multivariate_normality | alr_program_service_revenue |  | 0.2517 |  | OK | Per-axis skew=0.25, excess_kurtosis=-0.55. Pillai's trace is the most robust MANOVA statistic to multivariate non-normality, which is why the main analysis uses it; treat CAVEAT as informational. |
| manova_alr_pillai | approximate_multivariate_normality | alr_government_grants_received |  | -0.6397 |  | OK | Per-axis skew=-0.64, excess_kurtosis=0.39. Pillai's trace is the most robust MANOVA statistic to multivariate non-normality, which is why the main analysis uses it; treat CAVEAT as informational. |
| manova_alr_pillai | approximate_multivariate_normality | alr_other_institutional_contributions |  | 0.4464 |  | OK | Per-axis skew=0.45, excess_kurtosis=0.18. Pillai's trace is the most robust MANOVA statistic to multivariate non-normality, which is why the main analysis uses it; treat CAVEAT as informational. |
| manova_alr_pillai | approximate_multivariate_normality | alr_individual_likely_contributions |  | -0.2724 |  | OK | Per-axis skew=-0.27, excess_kurtosis=0.45. Pillai's trace is the most robust MANOVA statistic to multivariate non-normality, which is why the main analysis uses it; treat CAVEAT as informational. |
| manova_alr_pillai | approximate_multivariate_normality | alr_mixed_other_contributions |  | 0.7955 |  | OK | Per-axis skew=0.80, excess_kurtosis=0.82. Pillai's trace is the most robust MANOVA statistic to multivariate non-normality, which is why the main analysis uses it; treat CAVEAT as informational. |

## permutation_mean_diff

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| permutation_mean_diff | exchangeability_under_H0 |  |  |  |  | CAVEAT | Row-level permutation. Strict exchangeability with repeated EINs would require permuting at the EIN level. The cluster bootstrap CIs and the one-row-per-EIN sensitivity provide cluster-aware triangulation; both confirm the headline government-grants result. |

## cluster_bootstrap_ci

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cluster_bootstrap_ci | sufficient_cluster_count_per_group |  | Benchmark | 1647.0 |  | OK | 1647 unique EINs in group 'Benchmark'. Cluster bootstrap CIs are reliable from roughly 30+ clusters per group; below that, treat the interval width as a lower bound on uncertainty. |
| cluster_bootstrap_ci | sufficient_cluster_count_per_group |  | Black Hills | 509.0 |  | OK | 509 unique EINs in group 'Black Hills'. Cluster bootstrap CIs are reliable from roughly 30+ clusters per group; below that, treat the interval width as a lower bound on uncertainty. |

## all_share_tests

| test | assumption | variable | group | statistic | p_value | status | notes |
| --- | --- | --- | --- | --- | --- | --- | --- |
| all_share_tests | mutually_exclusive_segments_sum_to_total_revenue |  |  | 1.0203 |  | OK | Mean component-share sum = 1.020. 1.9% of rows are above 105% of total revenue; 0.0% of rows are below 95%. Detail rows are listed in tables/negative_residual_diagnostics.csv. |

