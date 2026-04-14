# Notebook Documentation: USAspending_Processing.ipynb

## 1. Overview

This notebook processes raw federal contract and assistance data from USAspending.gov. It consolidates multiple raw exports into a single "Silver Tier" dataset filtered by the project's specific benchmark regions.

## 2. Data Sources

* **Path:** `raw/PrimeAwardSummariesAndSubawards_2026-03-29/`
* **Format:** Multiple CSV files (Contracts and Assistance)

## 3. Processing Logic

### A. Consolidation

* Merges disparate CSV files into a unified pandas DataFrame.
* Standardizes column headers across "Assistance" and "Contract" schemas.

### B. Geographic Mapping (Benchmark Regions)

* Applies a custom mapping function based on the **Primary Place of Performance**.
* **Target Regions:**
  * Sioux Falls, SD+MN
  * Billings, MT
  * Flagstaff, AZ
  * Missoula, MT
  * Black Hills, SD (ref)

### C. Data Cleaning

* **Financials:** Converts obligated and outlayed amounts to numeric floats; fills `NaN` values with `0.00`.
* **Dates:** Standardizes `action_date` and `start_date` to datetime objects.
* **Type Hardening:** Casts all categorical columns (e.g., Award IDs, Recipient Names) to strings to ensure `pyarrow` compatibility.

## 4. Output

* **File:** `staging/usaspending_benchmark_awards_consolidated.parquet`
* **Tier:** Silver (Analysis-Ready)
