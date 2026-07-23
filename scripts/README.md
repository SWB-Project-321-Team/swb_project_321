# Scripts

Scripts are grouped by how they are used:

- [`reporting/`](reporting/) contains final-report, DOCX, and presentation generation tools.
- [`diagnostics/`](diagnostics/) contains manual data-quality checks and analysis validations.
- [`diagnostics/irs_990/`](diagnostics/irs_990/) contains IRS 990 parser benchmarks, historical S3 inspections, and sample-data utilities that are not automated tests. The former project bucket was intentionally deleted; inspection scripts must not recreate it.

Run scripts from the repository root so their documented relative paths and environment loading remain consistent. Automated checks belong under [`../tests/`](../tests/).
