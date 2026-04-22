"""
Step 1: Extract ACS DP03 variables for benchmark region counties.

Reads the ACS 5-Year DP03 data (ACSDP5Y2024), filters to the 18 target
counties defined in GEOID_reference.xlsx, selects the variables listed in
the Benchmark Regions document, and saves a county-level extract CSV.

The 5-Year ACS is used instead of the 1-Year ACS because many of the
target counties have populations under 65,000 and are suppressed in the
1-Year estimates. The 5-Year estimates cover all counties regardless of
population size.

Input:
  - GEOID_reference.xlsx  (county names, GEOIDs, and cluster assignments)
  - ACSDP5Y2024.DP03-Data.csv  (ACS 5-Year Selected Economic Characteristics)

Output:
  - 01_data/staging/census_acs/county_acs_extract.csv
"""

import sys
from pathlib import Path

import pandas as pd

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[1]  # transform -> python
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

ACS_CSV = DATA / "raw" / "census_acs" / "ACSDP5Y2024.DP03_2026-02-18T223058" / "ACSDP5Y2024.DP03-Data.csv"
REF_XLSX = DATA / "reference" / "GEOID_reference.xlsx"
OUT_DIR = DATA / "staging" / "census_acs"

# ── Target variables ──────────────────────────────────────────────────────────
# These variable codes come from the "Variables to summarize for 02/20/2026
# client meeting" section of the Benchmark Regions document.
#
# Variables are split into four groups based on how they will be used in
# the regional summary step:
#   - COUNT_VARS:   raw counts, summed across counties in a cluster.
#                   Also serve as weights for the weighted averages.
#   - PERCENT_VARS: percentages from the ACS "Percent" columns (PE suffix).
#   - RATE_VARS:    the unemployment rate, which is also a percentage.
#   - DOLLAR_VARS:  dollar amounts (mean earnings).

COUNT_VARS = [
    "DP03_0002E",   # Population 16 years and over in labor force
    "DP03_0026E",   # Civilian employed population 16 years and over
    "DP03_0051E",   # Total households
    "DP03_0064E",   # Total number of households with earnings
]

# Occupation percentages (DP03_0027PE through DP03_0030PE),
# armed forces percentage, household income bracket percentages
# (DP03_0052PE through DP03_0061PE), and retirement income percentage.
#
# Note on DP03_0068PE: The Benchmark Regions document references
# DP03_0069PE for "% of total households with retirement income,"
# but DP03_0069PE is actually the percent column for mean retirement
# income (which is not meaningful as a percentage and shows (X) in the
# 5-Year data). DP03_0068PE is the correct column for the share of
# households receiving retirement income.
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

# Note on DP03_0009PE: The Benchmark Regions document references
# DP03_0009E for the unemployment rate, but in the 5-Year ACS the
# "E" (estimate) column contains (X). The "PE" (percent) column
# holds the actual unemployment rate value.
RATE_VARS = [
    "DP03_0009PE",  # Civilian labor force unemployment rate
]

DOLLAR_VARS = [
    "DP03_0065E",   # Mean earnings for households with earnings
]

# Combined list of all variable codes to extract from the ACS CSV.
ALL_VARS = COUNT_VARS + PERCENT_VARS + RATE_VARS + DOLLAR_VARS


def load_reference() -> pd.DataFrame:
    """Load the GEOID reference file and normalize GEOIDs to 5-digit strings.

    The reference file stores some GEOIDs as integers (e.g., 46099) and
    others as zero-padded strings (e.g., '04005'). This function converts
    all GEOIDs to 5-character zero-padded strings so they can be matched
    against the ACS GEO_ID field.
    """
    ref = pd.read_excel(REF_XLSX)
    ref["GEOID"] = ref["GEOID"].astype(str).str.zfill(5)
    return ref


def load_acs(geoids: list[str]) -> pd.DataFrame:
    """Load the ACS CSV, filter to target counties, and convert to numeric.

    The ACS CSV has two header rows: the first contains variable codes
    (e.g., DP03_0009PE) and the second contains human-readable labels.
    Row index 1 (the label row) is dropped before processing.

    The GEO_ID column in the ACS data uses the format '0500000US{FIPS}'
    where the last 5 characters are the county FIPS code. This function
    extracts those 5 digits into a FIPS column for matching.

    Values like '(X)' and 'N' (suppressed or unavailable data) are
    converted to NaN during the numeric conversion step.
    """
    df = pd.read_csv(ACS_CSV, dtype=str, low_memory=False)
    df = df.iloc[1:]  # Drop the human-readable label row (row index 1)

    # Extract the 5-digit county FIPS code from the full GEO_ID
    df["FIPS"] = df["GEO_ID"].str[-5:]

    # Keep only the counties listed in the reference file
    df = df[df["FIPS"].isin(geoids)].copy()

    # Convert target variable columns from strings to numeric.
    # Non-numeric values such as '(X)' or 'N' become NaN.
    for col in ALL_VARS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Load the reference file containing county names, GEOIDs, and
    # cluster assignments (e.g., Cluster 0 = Black Hills, Cluster 1 = Sioux Falls).
    print("Loading reference file...")
    ref = load_reference()
    geoids = ref["GEOID"].tolist()
    print(f"  {len(geoids)} counties in reference file")

    # Load and filter the ACS data to only the target counties.
    print("Loading ACS 5-Year data...")
    acs = load_acs(geoids)
    print(f"  {len(acs)} county rows matched")

    # Warn if any counties from the reference file were not found in the ACS data.
    if len(acs) != len(geoids):
        matched = set(acs["FIPS"])
        missing = [g for g in geoids if g not in matched]
        print(f"  WARNING: missing counties: {missing}")

    # Select only the identifier columns and the target variables,
    # then merge with the reference file to attach cluster metadata
    # (County name, State, Cluster_ID, Cluster_name) to each row.
    extract = acs[["GEO_ID", "NAME", "FIPS"] + ALL_VARS].copy()
    extract = extract.merge(
        ref[["GEOID", "County", "State", "Cluster_ID", "Cluster_name"]],
        left_on="FIPS", right_on="GEOID", how="left",
    )

    # Save the county-level extract. This file is the input to
    # compute_regional_summary.py (Step 2).
    extract_path = OUT_DIR / "county_acs_extract.csv"
    extract.to_csv(extract_path, index=False)
    print(f"\nCounty-level extract saved to:\n  {extract_path}")


if __name__ == "__main__":
    main()
