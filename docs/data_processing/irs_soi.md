# Notebook Documentation: IRS_SOI_Processing.ipynb

## 1. Overview

This notebook processes the IRS Statistics of Income (SOI) Individual Income Tax Statistics. It performs FIPS normalization, handles data suppression, and integrates rurality codes (RUCC) for benchmark counties.

## 2. Data Sources

* **Files:** `22incyallagi.csv` (AGI groups) and `22incyallnoagi.csv` (County totals).
* **Encoding:** Loaded via `latin1` to handle special characters.

## 3. Processing Logic

### A. FIPS Normalization (Primary Link Key)

* **5-Digit FIPS:** Concatenates `STATEFIPS` (2-digits) and `COUNTYFIPS` (3-digits).
* **Manual Correction:** Recodes Shannon County, SD (`46113`) to **Oglala Lakota County (`46102`)** to reflect the 2015 name/FIPS change.

### B. Suppression & Cleaning

* **IRS Masking:** Federal law prohibits disclosure of cells with $N < 10$. These are marked as `0` in raw data.
* **Logic:** The notebook converts these `0` values to `NaN` in all `N` (counts) and `A` (amounts) columns to distinguish suppressed data from actual zero-dollar activity.

### C. RUCC Integration

* **RUCC 2013:** Manually joins the Rural-Urban Continuum Codes (1–9) for the 18 target counties.
* **Granularity:** Maintains the `agi_stub` column to allow for stratified analysis by income level.

### D. Benchmark Flagging

* Groups FIPS codes into the five primary study regions (Sioux Falls, Billings, Flagstaff, Missoula, Black Hills).

## 4. Output

* **File:** `staging/irs_soi_benchmark_combined.parquet`
* **Tier:** Silver (Analysis-Ready)
