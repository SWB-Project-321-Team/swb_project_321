import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests
import io
import boto3

# Ensure python/ is on path so we can import utils
_SCRIPT_DIR = Path(__file__).resolve()
_PYTHON_DIR = _SCRIPT_DIR.parents[2]
if str(_PYTHON_DIR) not in sys.path:
    sys.path.insert(0, str(_PYTHON_DIR))
from utils.paths import DATA

# Load the reference GEOID data to filter the BLS data for the specified regions.
REF_GEOID_CSV = DATA / "reference" / "GEOID_reference.csv"
ref_geoid = pd.read_csv(REF_GEOID_CSV, dtype={"GEOID": str})

# define the economic level for the BLS data in naisc code. 
# default is 10 for private total, but can be changed to other codes for different levels of economic activity.
naics_code = '10'

# Initialize the S3 client
s3 = boto3.client('s3')

# Define the S3 bucket and key for the input file
bucket_name='swb-321-irs990-teos'
key='bronze/bls/qcew-nonprofits-2022.xlsx'

# Download the file from S3
response = s3.get_object(Bucket=bucket_name, Key=key)
excel_data = io.BytesIO(response['Body'].read())
df = pd.read_excel(excel_data, sheet_name='2022_nonprofit_data', skiprows=3)
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(r"\s+", "_", regex=True)
    .str.replace(r"\.1", "_np", regex=True)
)

df['fips'] = df['fips'].astype(str).str.zfill(5)
ref_geoid['GEOID'] = ref_geoid['GEOID'].astype(str).str.zfill(5)

# Filter the DataFrame for the specified regions and NAICS code (private total)
df_filtered = df[(df['fips'].isin(ref_geoid['GEOID'])) & (df['naics']==naics_code)]

# return a list of regions that did not have data in the BLS dataset. 
missing_regions = set(ref_geoid['GEOID']) - set(df_filtered['fips'])
missing_regions_df = ref_geoid.loc[ref_geoid['GEOID'].isin(missing_regions),
                                  ['County', 'State', 'Cluster_name', 'GEOID']].reset_index(drop=True)

# add MSAs to the filtered dataframe. 
fips_msa = ['04362', # Sioux Falls 
            '01374', # Billings
            '02238', #Flagstaff
            '03354', # Missoula
            '03966' # Rapid city
            ]
df_msa = df[(df['fips'].isin(fips_msa)) & (df['naics']==naics_code)]

df_filtered = pd.concat([df_filtered, df_msa], ignore_index=True) 

# Save the filtered DataFrame back to S3 as a CSV file
df_filtered.to_csv(f"s3://{bucket_name}/silver/bls/qcew_nonprofits_2022.csv", index=False)
missing_regions_df.to_csv(f"s3://{bucket_name}/silver/bls/metadata/missing_regions_2022.csv", index=False)

