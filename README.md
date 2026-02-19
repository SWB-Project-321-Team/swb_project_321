# SWB Project 321

This repository contains the **code, SQL, and analytical logic** for *Statistics Without Borders (SWB) Project 321*, which analyzes IRS Form 990 filings to profile and benchmark nonprofit landscapes in the **Black Hills region** and comparable peer regions.

The project supports SWB’s mission to provide pro bono statistical and data science support to nonprofit organizations.

---

## Project Overview

The goal of this project is to:
- Build a clear, data-driven profile of the nonprofit ecosystem in the Black Hills region
- Compare nonprofit scale, composition, and financial capacity against selected benchmark regions
- Deliver transparent, reproducible analysis and analysis-ready datasets to the client

The analysis is based primarily on **IRS Form 990 filings** (Forms 990, 990-EZ, and 990-PF), supplemented with geographic reference data and **U.S. Census American Community Survey (ACS)** data for benchmark region comparisons.

---

## Architecture (Separation of Concerns)

This project intentionally separates **code**, **data**, and **outputs**:

| Component | Location |
|---|---|
| Code & analysis logic | **GitHub (this repository)** |
| Raw & curated data | **OneDrive** |
| Analysis outputs | **OneDrive (CSV / Excel / report)** |

⚠️ **No raw or curated data is stored in this repository.**

---

## Data Policy

- Raw source data (IRS 990 extracts, Census data) live in **OneDrive**
- Curated, analysis-ready datasets are versioned and stored in **OneDrive**
- This repository contains **no CSVs, Parquet files, Excel files, or credentials**
- All local paths and secrets are environment-specific and excluded via `.gitignore`

---

## Repository Structure
```
swb_project_321/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── sql/ 
│ ├── staging/ # Load and normalize raw source extracts
│ ├── curated/ # Build analysis-ready tables
│ └── meta/ # ETL logs, metadata
│
├── python/
│ ├── ingest/ # Source ingestion scripts
│ ├── transform/ # Data extraction, aggregation, and summarization
│ │ ├── extract_acs_variables.py # ACS DP03 county-level extraction (raw → staging)
│ │ └── compute_regional_summary.py # Weighted regional averages (staging → curated)
│ ├── export/ # Export CSV / Excel deliverables
│ └── utils/ # Shared helper functions
│
├── notebooks/ # Analysis and communication
│ ├── 01_landscape_summary_example.ipynb
│ └── 02_benchmark_comparison_example.ipynb
│
├── docs/
│ ├── methodology.md
│ ├── benchmark_regions.md
│ ├── data_sources.md
│ └── decisions.md
│
└── tests/
```
---

## Collaboration Guidelines

- The `main` branch contains stable, review-ready code
- Use feature branches for new work
- Open pull requests for:
  - schema changes
  - ETL logic changes
  - methodology updates
- Do **not** commit data files or credentials

---

## License

This project is released under the **MIT License**.

---

## Attribution

This repository is maintained by volunteers for  
**Statistics Without Borders (ASA) — Project 321**.

---

## Questions

For questions about methodology, data access, or infrastructure, contact the current project leads or refer to the documentation in `docs/`.
