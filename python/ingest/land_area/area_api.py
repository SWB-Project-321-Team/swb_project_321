import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import numpy as np
import requests
import io
import boto3

# Define the S3 bucket and key for the output file
bucket_name='swb-321-irs990-teos'
output_key='bronze/land_area/land_area.csv'

url_land="https://api.census.gov/data/2024/geoinfo?get=NAME,AREALAND&for=county:*"

# Retrieve data from the census API
def fetch_census_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None
    
data_land=fetch_census_data(url_land)
df_land=pd.DataFrame(data_land[1:], columns=data_land[0])

df_land.to_csv(f"s3://{bucket_name}/{output_key}", index=False)