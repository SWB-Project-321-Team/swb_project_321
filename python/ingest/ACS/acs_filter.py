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

REF_GEOID_CSV = DATA / "reference" / "GEOID_reference.csv"
GEOID_ZIP_CSV = DATA / "reference" / "geoid_zip_codes.csv"

ref_geoid = pd.read_csv(REF_GEOID_CSV, dtype={"GEOID": str})


# Define the S3 bucket and key for the input file
bucket_name='swb-321-irs990-teos'
key_full = 'bronze/acs/acs5_full_2024.csv'
key='bronze/acs/acs5_subject_2024.csv'
output_key = '/silver/acs/'

df_full = pd.read_csv(f"s3://{bucket_name}/{key_full}")
df_subject = pd.read_csv(f"s3://{bucket_name}/{key}")

df_dict_full = pd.DataFrame({
    "variable":df_full.columns, 
    "description":df_full.iloc[0].values
})
df_dict_subject = pd.DataFrame({
    "variable":df_subject.columns, 
    "description":df_subject.iloc[0].values
})

df_dict = (pd.concat([df_dict_full, df_dict_subject], ignore_index=True).
           drop_duplicates())

df_dict.to_csv(f"s3://{bucket_name}{output_key}metadata/acs_variable_dictionary.csv", index=False)

df_full = df_full.drop(index=0)
df_subject = df_subject.drop(index=0)
df_full["fips"] = df_full["state"] + df_full["county"]
df_subject["fips"] = df_subject["state"] + df_subject["county"]

df_full["fips"] = df_full["fips"].astype(str)
df_subject["fips"] = df_subject["fips"].astype(str)
df_full_filtered = df_full[df_full["fips"].isin(ref_geoid["GEOID"])]
df_subject_filtered = df_subject[df_subject["fips"].isin(ref_geoid["GEOID"])]   

df_merged = df_full_filtered.merge(df_subject_filtered, 
                                   on=["fips", "NAME"], 
                                   suffixes=("", "_drop")
                                   ).loc[:, lambda x: ~x.columns.str.endswith("_drop")]

# reorder state, county, fips columns to the front
cols = df_merged.columns.tolist()
cols = (["NAME","state", "county", "fips"] +
        [col for col in cols if col not in ["NAME","state", "county", "fips"]])
df_merged = df_merged[cols]

# write the merged dataframe to S3
df_merged.to_csv(f"s3://{bucket_name}{output_key}acs_merged_2024.csv", index=False)