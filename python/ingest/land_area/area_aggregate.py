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

ref_geoid = pd.read_csv(REF_GEOID_CSV, dtype={"GEOID": str})

# Define the S3 bucket and key for the output file
bucket_name='swb-321-irs990-teos'
input_key='bronze/land_area/land_area.csv'
output_key='silver/land_area/land_area_region.csv'

df=pd.read_csv(f"s3://{bucket_name}/{input_key}",
               dtype={'state': str, 'county':str})

df["fips"]=df["state"]+df["county"]
df["fips"]=df["fips"].astype(str)

df_merged=df.merge(
    ref_geoid[['Cluster_name', 'GEOID']],
    left_on='fips',
    right_on='GEOID',
    how='right'
).drop(['fips', 'state', 'county'], axis=1)

df_sum=df_merged.groupby('Cluster_name')[['AREALAND']]\
                .sum()
df_sum=df_sum.assign(
    AREALAND_SQMILE= lambda x: x['AREALAND'] * 3.86102e-7
)

df_sum.to_csv(f"s3://{bucket_name}/{output_key}", index=False)
