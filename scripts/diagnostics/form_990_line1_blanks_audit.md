# Form 990 Part VIII Line 1 blanks audit

This audit empirically checks whether the analysis's `fillna(0.0)` for Form 990 Part VIII Line 1 sub-components on Form 990 rows is consistent with how filers actually use the form. We compare populated 1a-1f values against the reported Line 1h total contributions and look for rows where Line 1h is positive but every sub-line is blank.

- Form 990 rows audited: 2,439
- Tax years: ['2022', '2023', '2024']
- Regions: ['Billings', 'BlackHills', 'Flagstaff', 'Missoula', 'SiouxFalls']

## Blank, explicit-zero, and positive rates by sub-line

| Sub-line | Raw column | Blank | Explicit zero | Positive | Negative |
| --- | --- | --- | --- | --- | --- |
| 1a federated_campaigns | FEDERACAMPAI | 90.7% (2,212) | 4.6% (112) | 4.7% (115) | 0 |
| 1b membership_dues | MEMBERDUESUE | 80.1% (1,953) | 3.9% (94) | 16.1% (392) | 0 |
| 1c fundraising_events_contributions | FUNDRAEVENTS | 79.1% (1,929) | 3.3% (81) | 17.5% (428) | 1 |
| 1d related_org_contributions | RELATEORGANI | 90.0% (2,194) | 4.4% (107) | 5.7% (138) | 0 |
| 1e government_grants_received | GOVERNGRANTS | 60.3% (1,471) | 3.6% (88) | 36.1% (880) | 0 |
| 1f all_other_contributions | ALLOOTHECONT | 23.1% (563) | 0.8% (20) | 76.1% (1,856) | 0 |

## Reconciliation of populated 1a-1f against Line 1h

Each row class below is computed on the same Form 990 universe. The reconciliation columns treat blanks as zero (matching the current analysis) and compare the sum of 1a through 1f to the reported Line 1h total contributions on the same return.

| Row class | Rows | Share of Form 990 | Line 1h > 0 | Line 1h = 0 or blank | Median Line 1h | Median abs gap (sum 1a-1f vs Line 1h) | Reconciled within $1 | Reconciled within 1% |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| all_subs_blank (every 1a-1f is NULL) | 296 | 12.1% | 0.0% | 100.0% | $0 | $0 | 100.0% | 0.0% |
| some_subs_blank (1-5 blanks among 1a-1f) | 2,028 | 83.1% | 100.0% | 0.0% | $252,406 | $0 | 100.0% | 100.0% |
| all_subs_populated (every 1a-1f is non-blank) | 115 | 4.7% | 93.0% | 7.0% | $207,144 | $0 | 100.0% | 93.0% |

## Spotlight: every sub-line blank but Line 1h is positive

If blanks really meant zero, then a return with every sub-line blank should also report a $0 Line 1h. Any rows here are evidence that the `fillna(0.0)` choice converts real missing data into spurious zeros for the affected sub-components.

- Rows with every 1a-1f blank: 296
- Of those, rows where Line 1h > 0: 0 (0.00% of Form 990 rows)
