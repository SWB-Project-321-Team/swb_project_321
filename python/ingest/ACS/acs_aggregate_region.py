import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import io
import boto3

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

REF_GEOID_CSV = DATA / "reference" / "GEOID_reference.csv"
GEOID_ZIP_CSV = DATA / "reference" / "geoid_zip_codes.csv"

ref_geoid = pd.read_csv(REF_GEOID_CSV, dtype={"GEOID": str})

# Define the S3 bucket and key for the input file
bucket_name='swb-321-irs990-teos'
input_key='silver/acs/acs_merged_2024.csv'
output_key='silver/acs/acs_region_2024.csv'

df=pd.read_csv(f"s3://{bucket_name}/{input_key}", dtype={"fips": str})

# Variables that should be summed by regions. 
var_sum=[
    "B01003_001E",  # total population
    "B23025_003E",  # civilian labor force
    "B23025_004E",  # civilian employed
    "B23025_005E",  # unemployed
    "B02001_004E",  # American Indian and Alaska Native alone

    "S2402_C01_001E", # total employed pop 16+
    "S2402_C01_002E", # mangement, business, science, and arts occupations
    "S2402_C01_026E", # sales and office occupations
    "S2402_C01_029E", # natural resources, construction, and maintenance occupations
    "S2402_C01_033E", # production, transportation, and material moving occupations

    "DP03_0002E", # total 16 and above population in labor force
    "DP03_0006E", # Armed forces
    "DP03_0064E", # households with earnings
    "DP03_0052E", # Income < $10K 
    "DP03_0053E", # Income $10 - $15k
    "DP03_0054E", # Income $15 - 25k 
    "DP03_0055E", # Income $25K - $35k
    "DP03_0056E", # Income $35k - $50k
    "DP03_0057E", # Income $50 - $75K
    "DP03_0058E", # Income $75 - $100k 
    "DP03_0059E", # Income $100 - $150k 
    "DP03_0060E", # Income $150 - $200k 
    "DP03_0061E", # Income $200k
    "DP03_0068E" # Household with retirement income    
]

# A set of rules to normalize the regional counts and calculate percentages
rules=pd.DataFrame({
    'numerator':[
        "B23025_004E",  # civilian employed
        "B23025_005E",  # unemployed
        "B02001_004E",  # American Indian and Alaska Native alone
        "S2402_C01_002E", # mangement, business, science, and arts occupations
        "S2402_C01_026E", # sales and office occupations
        "S2402_C01_029E", # natural resources, construction, and maintenance occupations
        "S2402_C01_033E", # production, transportation, and material moving occupations
        "DP03_0006E", # Armed forces
        "DP03_0068E", # Household with retirement income   
        "DP03_0052E", # Income < $10K 
        "DP03_0053E", # Income $10 - $15k
        "DP03_0054E", # Income $15 - 25k 
        "DP03_0055E", # Income $25K - $35k
        "DP03_0056E", # Income $35k - $50k
        "DP03_0057E", # Income $50 - $75K
        "DP03_0058E", # Income $75 - $100k 
        "DP03_0059E", # Income $100 - $150k 
        "DP03_0060E", # Income $150 - $200k 
        "DP03_0061E", # Income $200k
    ],
    'base':[
        "B23025_003E",  # civilian labor force
        "B23025_003E",  # civilian labor force
        "B01003_001E",  # total population
        "S2402_C01_001E", # total employed pop 16+
        "S2402_C01_001E", # total employed pop 16+
        "S2402_C01_001E", # total employed pop 16+
        "S2402_C01_001E", # total employed pop 16+
        "DP03_0002E", # total 16 and above population in labor force
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
        "DP03_0064E", # households with earnings
    ],   
    'label':[
        "per_employed", 
        "per_unemployed", 
        "per_AIAN", 
        "per_management",
        "per_sales", 
        "per_natural", 
        "per_production",
        "per_arm_forces",
        "per_retirement_household",
        "per_10k", # Income < $10K 
        "per_10_15k", # Income $10 - $15k
        "per_15_25k", # Income $15 - 25k 
        "per_25_35k", # Income $25K - $35k
        "per_35_50k", # Income $35k - $50k
        "per_50_75k", # Income $50 - $75K
        "per_75_100k", # Income $75 - $100k 
        "per_100_150k", # Income $100 - $150k 
        "per_150_200k", # Income $150 - $200k 
        "per_200k", # Income $200k
    ]
})


# a dataframe with only the variables that need to be summed, fips, and regions
df_sum=df[['fips']+var_sum].merge(
    ref_geoid[['Cluster_name', 'GEOID']], 
    left_on='fips',
    right_on='GEOID',
    how='left'
).drop(columns=['fips'])

df_region=df_sum.groupby('Cluster_name')[var_sum].sum()

for _, row in rules.iterrows():
    num_col=row['numerator']
    base_col=row['base']
    new_col=row['label']
    
    df_region[new_col]= np.where(
        df_region[base_col] !=0,
        df_region[num_col]/df_region[base_col]*100,
        np.nan
    )


# Caculate regional mean household income as a weighted mean by total household with earnings
df_mean=df[['fips', 'S1902_C01_001E', 'DP03_0064E']].merge(
    ref_geoid[['Cluster_name', 'GEOID']], 
    left_on='fips',
    right_on='GEOID',
    how='left'   
).drop(columns=['fips'])

df_region_mean=(
    df_mean.groupby('Cluster_name')[['S1902_C01_001E', 'DP03_0064E']]
            .apply(lambda x: (x['S1902_C01_001E'] * x['DP03_0064E']).sum()
                            / x['DP03_0064E'].sum())
            .reset_index(name='mean_region_household_income')
)

df_out=df_region_mean.merge(
    df_region,
    on='Cluster_name',
    suffixes=('', '_drop')
).loc[:, lambda x: ~x.columns.str.endswith('_drop')]

# write the merged dataframe to S3
df_out.to_csv(f"s3://{bucket_name}/{output_key}", index=False)


