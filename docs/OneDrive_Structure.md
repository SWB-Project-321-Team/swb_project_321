# Data and Repository Structure

This document describes how **OneDrive** and **GitHub** are used together in
**Statistics Without Borders (SWB) Project 321**, and what types of files belong
in each location.

The goal is to ensure:
- reproducibility
- safe collaboration
- clear separation of responsibilities
- no accidental data loss or corruption

---

## High-level architecture

This project separates **code**, **data**, and **outputs**:

| Component | Purpose | Location |
|---|---|---|
| Code & analysis logic | SQL, Python, notebooks | GitHub |
| Raw & curated data | Source data and analysis-ready datasets | OneDrive |
| Client deliverables | CSVs, Excel files, figures, reports | OneDrive |

**No raw or curated data is stored in GitHub.**

---

## OneDrive structure (data & outputs)

OneDrive is the **system of record for data**. All project data and outputs live under:

**`SWB Operations/Project Data/321_Black_Hills_Area_Community_Foundation_2025_08/`**

---

### Folder structure (tree)

```
SWB Operations/Project Data/321_Black_Hills_Area_Community_Foundation_2025_08/
│
├── 01_data/
│   ├── raw/
│   │   ├── irs_990/
│   │   ├── givingtuesday_990/
│   │   └── census_acs/
│   │
│   ├── reference/
│   │   ├── zip_to_county_fips.csv
│   │   ├── county_to_region_map.csv
│   │   ├── ntee_codes.csv
│   │   └── benchmark_regions.csv
│   │
│   ├── staging/
│   │   ├── org/
│   │   ├── filing/
│   │   ├── financial_fact/
│   │   └── geo_bridge/
│   │
│   ├── curated/
│   │   ├── v1_YYYY-MM-DD/
│   │   │   ├── org.parquet
│   │   │   ├── filing.parquet
│   │   │   ├── financial_fact.parquet
│   │   │   └── geo_bridge.parquet
│   │   └── CURRENT.txt
│   │
│   └── database/
│       └── (e.g. swb_321.duckdb — built from curated Parquet; for read-only querying)
│
├── 02_outputs/
│   ├── exports/
│   ├── tables/
│   ├── figures/
│   └── report/
│       ├── draft/
│       └── final/
│
└── 03_docs/
    ├── data_dictionary.xlsx
    ├── methodology.md
    └── limitations.md
```

---

### What goes in each folder

#### **01_data/** — All project data (medallion: raw → staging → curated)

| Folder | Layer | Purpose |
|--------|--------|--------|
| **raw/** | Bronze | Immutable source data as received. Do not edit or overwrite. |
| **reference/** | — | Lookup tables and mappings used by staging and curated. |
| **staging/** | Silver | Cleaned, normalized tables from ingest + sql/staging. |
| **curated/** | Gold | Versioned, analysis-ready datasets; system of record for analytics. |
| **database/** | Optional | Read-only DB files (e.g. DuckDB) built from curated for ad-hoc SQL. |

---

#### **01_data/raw/** (bronze — immutable)

Source data exactly as obtained from providers. Used only as input by ingest and staging scripts.

| Subfolder | Contents |
|-----------|----------|
| **irs_990/** | IRS Form 990 extracts (Forms 990, 990-EZ, 990-PF) — bulk downloads or API pulls. |
| **givingtuesday_990/** | Giving Tuesday or other 990-derived datasets, if used. |
| **census_acs/** | U.S. Census American Community Survey (or other census) files used for context. |

**Rules:** Do not modify, rename, or delete files. If you need a new source, add a new subfolder or new files; keep history.

---

#### **01_data/reference/**

Lookup and mapping tables that staging and curated logic depend on (geography, NTEE, benchmark regions). Update in place when definitions change; not versioned like curated.

| Example file | Purpose |
|--------------|---------|
| **zip_to_county_fips.csv** | Mapping from ZIP codes to county FIPS codes. |
| **county_to_region_map.csv** | Mapping from counties to project regions (e.g. Black Hills, benchmark regions). |
| **ntee_codes.csv** | NTEE classification codes and descriptions. |
| **benchmark_regions.csv** | Final list of benchmark regions (from the benchmark-region team). |

---

#### **01_data/staging/** (silver)

Output of **python/ingest** and **sql/staging**: cleaned, normalized, one row per entity or filing. Not yet filtered or aggregated for a specific report. **sql/curated** reads from here and writes to **curated/**.

| Subfolder | Contents |
|-----------|----------|
| **org/** | Organization-level staging (one row per org, normalized fields). |
| **filing/** | Filing-level staging (one row per 990 filing). |
| **financial_fact/** | Financial facts (revenue, expenses, assets, etc.) linked to org/filing. |
| **geo_bridge/** | Geography bridge tables (e.g. org/filing to county/region). |

Use staging for exploratory analysis, different cuts, or building multiple curated datasets from one silver layer.

---

#### **01_data/curated/** (gold — versioned)

Analysis-ready datasets. Each release is a **dated snapshot** so you can reproduce past results.

| Item | Purpose |
|------|---------|
| **v1_YYYY-MM-DD/** | One snapshot per release date. Contains e.g. `org.parquet`, `filing.parquet`, `financial_fact.parquet`, `geo_bridge.parquet`. |
| **CURRENT.txt** | Text file pointing to the active version (e.g. `v1_2025-02-10`). Scripts and notebooks read this to find the latest curated path. |

Notebooks and **python/export** read from curated (or from **database/** if present). Curated is the system of record; do not overwrite a dated folder.

---

#### **01_data/database/** (optional)

Holds database files (e.g. **swb_321.duckdb**) built **from** curated Parquet for convenient ad-hoc SQL. Not part of the main ETL.

**Rules:** Build from curated only. Open **read-only** when querying; close when not in use so OneDrive sync does not corrupt the file. No ETL should write to DB files in this folder.

---

#### **02_outputs/** — Client-facing and internal deliverables

All exported tables, figures, and reports. No code; only outputs.

| Subfolder | Contents |
|-----------|----------|
| **exports/** | CSV, Excel, or other export files for the client or partners (e.g. summary tables, list exports). |
| **tables/** | Publication-ready tables (e.g. for reports, slides, or appendices). |
| **figures/** | Charts, maps, and other graphics. |
| **report/draft/** | Report drafts and work-in-progress versions. |
| **report/final/** | Approved, final report versions for delivery. |

---

#### **03_docs/** — Project documentation that lives with the data

Data-specific docs that belong next to the dataset (as opposed to repo-level docs in GitHub **docs/**).

| Example file | Purpose |
|--------------|---------|
| **data_dictionary.xlsx** | Variable definitions, codes, and field descriptions for curated/staging. |
| **methodology.md** | Methodological notes (sources, definitions, assumptions). |
| **limitations.md** | Known data limitations and caveats. |

---

### OneDrive rules
- `raw/` is immutable
- `curated/` is versioned
- Database files (`.sqlite`, `.db`, `.duckdb`) are allowed **only** in `01_data/database/`. Build them from curated Parquet; open **read-only** when querying so sync does not corrupt the file. Close the DB when not in use.
- Outputs live only in `02_outputs/`

---

## GitHub repository structure (code only)

GitHub is the **system of record for logic**.

```
form-990-benchmarking/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── sql/
│   ├── staging/
│   ├── curated/
│   └── meta/
│
├── python/
│   ├── ingest/
│   ├── transform/
│   ├── export/
│   └── utils/
│
├── notebooks/
│   ├── 01_landscape_summary.ipynb
│   └── 02_benchmark_comparison.ipynb
│
├── docs/
│   ├── methodology.md
│   ├── benchmark_regions.md
│   ├── data_sources.md
│   └── decisions.md
│
└── tests/
```

### GitHub rules
- No data files or credentials
- SQL logic in `.sql` files
- Python scripts must be reproducible
- Notebooks are exploratory, not pipelines

---

## How GitHub and OneDrive interact

```
OneDrive/raw
   ↓
python/ingest + sql/staging
   ↓
OneDrive/staging
   ↓
sql/curated
   ↓
OneDrive/curated
   ├── build .duckdb / .db from curated  →  OneDrive/database   [read-only querying]
   └── notebooks/  (read Parquet or DB)
        ↓
   python/export
        ↓
   OneDrive/outputs
```

---

## Summary

GitHub contains all code and analytical logic, while OneDrive contains all data and deliverables.
