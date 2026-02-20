"""
Step 2: Compute weighted regional averages from the county-level extract.

Reads county_acs_extract.csv (produced by extract_acs_variables.py),
groups counties by their assigned cluster (region), computes population-
weighted averages for each region, and outputs a summary CSV.

The weighting scheme follows the analysis plan in the Benchmark Regions
document:
  - Occupation percentages weighted by civilian employed population
  - Armed forces percentage weighted by population 16+ in labor force
  - Unemployment rate weighted by population 16+ in labor force
  - Household income bracket percentages weighted by total households
  - Retirement income percentage weighted by total households
  - Mean earnings weighted by number of households with earnings

Output:
  - 01_data/curated/census_acs/regional_summary.csv
"""

import pandas as pd
import numpy as np
from pathlib import Path

# ── File paths ────────────────────────────────────────────────────────────────
# All paths are relative to the project root (two levels up from python/transform/).

BASE = Path(__file__).resolve().parent.parent.parent
DATA = BASE / "data" / "321_Black_Hills_Area_Community_Foundation_2025_08" / "01_data"

# Input: the county-level extract produced by extract_acs_variables.py
EXTRACT_CSV = DATA / "staging" / "census_acs" / "county_acs_extract.csv"

# Output: analysis-ready regional summary in the curated layer
OUT_DIR = DATA / "curated" / "census_acs"

# ── Variable groups (must match extract_acs_variables.py) ─────────────────────
# These lists are duplicated here so that this script can run independently
# without importing from extract_acs_variables.py.

# Count variables are summed across counties within each cluster to produce
# regional totals. They also serve as denominators (weights) for the
# weighted average calculations.
COUNT_VARS = [
    "DP03_0002E",   # Population 16+ in labor force
    "DP03_0026E",   # Civilian employed population 16+
    "DP03_0051E",   # Total households
    "DP03_0064E",   # Households with earnings
]

# Percent variables represent shares (e.g., "34.2% of civilian employed
# population works in management"). These are averaged across counties
# using population-weighted means, not simple averages, because counties
# vary widely in size.
PERCENT_VARS = [
    "DP03_0027PE",  # % Management, business, science, and arts occupations
    "DP03_0028PE",  # % Service occupations
    "DP03_0029PE",  # % Sales and office occupations
    "DP03_0030PE",  # % Natural resources, construction, and maintenance occupations
    "DP03_0006PE",  # % Armed forces
    "DP03_0052PE",  # % Household income less than $10,000
    "DP03_0053PE",  # % Household income $10,000 to $14,999
    "DP03_0054PE",  # % Household income $15,000 to $24,999
    "DP03_0055PE",  # % Household income $25,000 to $34,999
    "DP03_0056PE",  # % Household income $35,000 to $49,999
    "DP03_0057PE",  # % Household income $50,000 to $74,999
    "DP03_0058PE",  # % Household income $75,000 to $99,999
    "DP03_0059PE",  # % Household income $100,000 to $149,999
    "DP03_0060PE",  # % Household income $150,000 to $199,999
    "DP03_0061PE",  # % Household income $200,000 or more
    "DP03_0068PE",  # % Households with retirement income
]

RATE_VARS = [
    "DP03_0009PE",  # Civilian labor force unemployment rate
]

DOLLAR_VARS = [
    "DP03_0065E",   # Mean earnings for households with earnings
]

# ── Weighting map ─────────────────────────────────────────────────────────────
# Maps each percent/rate/dollar variable to the count variable that should
# be used as its weight when computing a regional average.
#
# Formula: weighted_avg = sum(value_i * weight_i) / sum(weight_i)
#   where i iterates over counties within a cluster.
#
# Weighting rationale:
#   - Occupation percentages (0027PE-0030PE): weighted by DP03_0026E
#     (civilian employed population), since occupations are expressed as
#     shares of that population.
#   - Armed forces and unemployment rate: weighted by DP03_0002E
#     (population 16+ in labor force), since both are shares of that base.
#   - Income bracket percentages and retirement income: weighted by
#     DP03_0051E (total households), since those are shares of all households.
#   - Mean earnings: weighted by DP03_0064E (households with earnings),
#     since the mean only applies to that subset of households.

WEIGHT_MAP = {
    "DP03_0009PE": "DP03_0002E",
    "DP03_0027PE": "DP03_0026E",
    "DP03_0028PE": "DP03_0026E",
    "DP03_0029PE": "DP03_0026E",
    "DP03_0030PE": "DP03_0026E",
    "DP03_0006PE": "DP03_0002E",
    "DP03_0052PE": "DP03_0051E",
    "DP03_0053PE": "DP03_0051E",
    "DP03_0054PE": "DP03_0051E",
    "DP03_0055PE": "DP03_0051E",
    "DP03_0056PE": "DP03_0051E",
    "DP03_0057PE": "DP03_0051E",
    "DP03_0058PE": "DP03_0051E",
    "DP03_0059PE": "DP03_0051E",
    "DP03_0060PE": "DP03_0051E",
    "DP03_0061PE": "DP03_0051E",
    "DP03_0068PE": "DP03_0051E",
    "DP03_0065E":  "DP03_0064E",
}

# ── Cluster display order and labels ──────────────────────────────────────────
# Cluster IDs and human-readable region names, matching the table shell
# in the Benchmark Regions document. Cluster 0 (Black Hills) is the
# reference region; Clusters 1-4 are the benchmark regions.

CLUSTER_ORDER = [
    (0, "Black Hills, SD"),
    (1, "Sioux Falls, SD"),
    (2, "Billings, MT"),
    (3, "Flagstaff, AZ"),
    (4, "Missoula, MT"),
]

# ── Income bracket aggregation ────────────────────────────────────────────────
# The table shell groups the 10 individual income brackets into three tiers.
LOW_INCOME_VARS = [
    "DP03_0052PE", "DP03_0053PE", "DP03_0054PE", "DP03_0055PE", "DP03_0056PE",
]
MID_INCOME_VARS = [
    "DP03_0057PE", "DP03_0058PE", "DP03_0059PE",
]
HIGH_INCOME_VARS = [
    "DP03_0060PE", "DP03_0061PE",
]

# ── Human-readable column names ───────────────────────────────────────────────
# Maps each ACS variable code to a label matching the table shell in the
# Benchmark Regions document. The final CSV columns use the format
# "Label | Variable_Code" so both are visible.
COLUMN_LABELS = {
    "Cluster_ID":   "Cluster_ID",
    "Region":       "Region",
    "DP03_0009PE":  "Unemployment Rate (%) | DP03_0009PE",
    "DP03_0002E":   "Population 16+ in Labor Force | DP03_0002E",
    "DP03_0026E":   "Civilian Employed Population (16+) | DP03_0026E",
    "DP03_0006PE":  "Armed Forces (%) | DP03_0006PE",
    "DP03_0027PE":  "Management, Business, Science & Arts (%) | DP03_0027PE",
    "DP03_0028PE":  "Service Occupations (%) | DP03_0028PE",
    "DP03_0029PE":  "Sales & Office Occupations (%) | DP03_0029PE",
    "DP03_0030PE":  "Natural Resources, Construction & Maintenance (%) | DP03_0030PE",
    "DP03_0051E":   "Total Households | DP03_0051E",
    "DP03_0064E":   "Households with Earnings | DP03_0064E",
    "DP03_0065E":   "Mean Earnings ($) | DP03_0065E",
    "DP03_0068PE":  "Households with Retirement Income (%) | DP03_0068PE",
    "DP03_0052PE":  "Income < $10k (%) | DP03_0052PE",
    "DP03_0053PE":  "Income $10k-$15k (%) | DP03_0053PE",
    "DP03_0054PE":  "Income $15k-$25k (%) | DP03_0054PE",
    "DP03_0055PE":  "Income $25k-$35k (%) | DP03_0055PE",
    "DP03_0056PE":  "Income $35k-$50k (%) | DP03_0056PE",
    "DP03_0057PE":  "Income $50k-$75k (%) | DP03_0057PE",
    "DP03_0058PE":  "Income $75k-$100k (%) | DP03_0058PE",
    "DP03_0059PE":  "Income $100k-$150k (%) | DP03_0059PE",
    "DP03_0060PE":  "Income $150k-$200k (%) | DP03_0060PE",
    "DP03_0061PE":  "Income $200k+ (%) | DP03_0061PE",
}


def weighted_avg(group: pd.DataFrame, val_col: str, wt_col: str) -> float:
    """Compute a weighted average of val_col using wt_col as weights.

    Rows where either the value or weight is NaN are excluded.
    Returns NaN if no valid rows remain or if the total weight is zero.
    """
    valid = group[[val_col, wt_col]].dropna()
    if valid.empty or valid[wt_col].sum() == 0:
        return np.nan
    return (valid[val_col] * valid[wt_col]).sum() / valid[wt_col].sum()


def compute_regional_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate county-level data into one row per regional cluster.

    Count variables are summed (to get regional totals).
    Percent, rate, and dollar variables are computed as weighted averages
    using the WEIGHT_MAP to determine which count column serves as
    the weight for each variable.

    After computing per-variable results, the 10 individual income bracket
    percentages are aggregated into Low (<$50k), Middle ($50k-$149k), and
    High ($150k+) columns.
    """
    rows = []
    for cluster_id, label in CLUSTER_ORDER:
        g = df[df["Cluster_ID"] == cluster_id]
        row = {"Cluster_ID": cluster_id, "Region": label}

        # Sum count variables across counties to get cluster-wide totals
        for var in COUNT_VARS:
            row[var] = g[var].sum()

        # Compute weighted averages for all percent, rate, and dollar variables
        for var in PERCENT_VARS + RATE_VARS + DOLLAR_VARS:
            row[var] = weighted_avg(g, var, WEIGHT_MAP[var])

        # Aggregate income brackets into Low / Middle / High tiers
        row["Income_Low_Pct"] = sum(row[v] for v in LOW_INCOME_VARS)
        row["Income_Mid_Pct"] = sum(row[v] for v in MID_INCOME_VARS)
        row["Income_High_Pct"] = sum(row[v] for v in HIGH_INCOME_VARS)

        rows.append(row)

    return pd.DataFrame(rows)


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load the county-level extract produced by extract_acs_variables.py
    print(f"Loading county extract from:\n  {EXTRACT_CSV}")
    df = pd.read_csv(EXTRACT_CSV)
    print(f"  {len(df)} county rows loaded")

    # Compute weighted averages for each regional cluster
    print("\nComputing weighted regional averages...")
    summary = compute_regional_summary(df)

    # Add human-readable labels for the aggregated income columns
    labels = dict(COLUMN_LABELS)
    labels["Income_Low_Pct"] = "Income Low <$50k (%)"
    labels["Income_Mid_Pct"] = "Income Middle $50k-$149k (%)"
    labels["Income_High_Pct"] = "Income High $150k+ (%)"

    summary = summary.rename(columns=labels)

    # Save the summary as a CSV
    summary_path = OUT_DIR / "regional_summary.csv"
    summary.to_csv(summary_path, index=False)
    print(f"Regional summary saved to:\n  {summary_path}")


if __name__ == "__main__":
    main()
