# Q9 2022 Sample-Size Diagnostics

This report checks whether the 2022 client-presentation tests have a sample-size problem. The source slides use positive-only medians and a two-sided permutation test on the median difference, so the key sample size is the number of organizations in each region that report a positive dollar amount for that source.

## Bottom Line

- No positive-only median comparison is invalidated by sample size: every comparison has at least 2 positive reporters in both Black Hills and the benchmark region.
- Sample-size concern counts: low=29, moderate=4, high=7, invalid=0.
- Small positive-reporter counts are not a formal permutation-test assumption violation. They mainly reduce power and widen uncertainty, especially for rare contribution channels.
- With 10,000 random reshuffles, the Monte Carlo standard error near p=0.05 is about 0.002; borderline p-values can move slightly with a different random seed, but the main issue for rare sources is the low number of positive reporters.

## Comparisons Most Limited By Sample Size

| Source | Comparison | BH positive n | Benchmark positive n | BH reporting rate | Benchmark reporting rate | Concern |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Federated campaign contributions | Black Hills vs Billings | 18 | 5 | 7.0% | 2.4% | high |
| Federated campaign contributions | Black Hills vs Flagstaff | 18 | 3 | 7.0% | 2.5% | high |
| Federated campaign contributions | Black Hills vs Missoula | 18 | 5 | 7.0% | 2.8% | high |
| Related organization contributions | Black Hills vs Billings | 10 | 15 | 3.9% | 7.3% | high |
| Related organization contributions | Black Hills vs Flagstaff | 10 | 5 | 3.9% | 4.1% | high |
| Related organization contributions | Black Hills vs Missoula | 10 | 8 | 3.9% | 4.5% | high |
| Related organization contributions | Black Hills vs Sioux Falls | 10 | 15 | 3.9% | 4.6% | high |
| Federated campaign contributions | Black Hills vs Sioux Falls | 18 | 23 | 7.0% | 7.1% | moderate |
| Fundraising event contributions | Black Hills vs Flagstaff | 39 | 17 | 15.2% | 14.0% | moderate |
| Membership dues | Black Hills vs Flagstaff | 52 | 20 | 20.3% | 16.5% | moderate |
| Membership dues | Black Hills vs Missoula | 52 | 30 | 20.3% | 16.9% | moderate |

## Source-Level Summary

| Source | Smallest positive n in any pair | Median of pairwise minimum n | Max tie rate | Worst concern |
| --- | ---: | ---: | ---: | --- |
| Federated campaign contributions | 3 | 5.0 | 0.0% | high |
| Related organization contributions | 5 | 9.0 | 13.3% | high |
| Fundraising event contributions | 17 | 39.0 | 0.0% | moderate |
| Membership dues | 20 | 33.5 | 0.0% | moderate |
| Government grants received | 49 | 72.5 | 4.6% | low |
| Program service revenue | 93 | 172.0 | 0.8% | low |
| Other revenue | 143 | 247.5 | 6.5% | low |
| Mixed / unclassified contributions | 153 | 222.5 | 4.3% | low |
| Total contributions | 165 | 246.5 | 3.3% | low |
| Total revenue | 197 | 307.5 | 0.9% | low |

## Borderline P-Values

| Source | Comparison | p-value | Concern |
| --- | --- | ---: | --- |
| Government grants received | Black Hills vs Billings | 0.0430 | low |

## Interpretation

The rare contribution subcategories are the places where sample size most affects interpretation. Federated campaign contributions and related organization contributions have the smallest positive reporter counts; membership dues and fundraising event contributions have more information, but some benchmark pairs are still modest. For total revenue, total contributions, program service revenue, mixed / unclassified contributions, and other revenue, non-significant results are less likely to be explained mainly by sample size because positive reporter counts are much larger.
