import requests
import pandas as pd
import boto3
from io import StringIO

# List of variables to fetch from the ACS API
def build_census_url(vars_list, subject=1, geography="county:*", year=2024):
    
    if subject == 1:
        base = f"https://api.census.gov/data/{year}/acs/acs5/subject"
    else:
        base = f"https://api.census.gov/data/{year}/acs/acs5"
    vars_str = ",".join(vars_list)
    return f"{base}?get={vars_str}&for={geography}&descriptive=true"

vars_fulltable = [
    "NAME", # Name of the geographic area
    "B01003_001E",  # total population
    "B19013_001E",  # median household income
    "B23025_003E",  # civilian labor force
    "B23025_004E",  # civilian employed
    "B23025_005E",  # unemployed
    "B02001_004E",   # American Indian and Alaska Native alone
]
vars_subjecttable = [
    "NAME", # Name of the geographic area
    "S1902_C01_001E", # Mean household income in the past 12 months (in 2024 inflation-adjusted dollars)
    "S2402_C01_001E", # total employed pop 16+
    "S2402_C01_002E", # maangement, business, science, and arts occupations
    "S2402_C01_026E", # sales and office occupations
    "S2402_C01_029E", # natural resources, construction, and maintenance occupations
    "S2402_C01_033E" # production, transportation, and material moving occupations
]

url_subject = build_census_url(vars_subjecttable, subject=1)
url_full = build_census_url(vars_fulltable, subject=0)

# Retrieve data from the ACS API
def fetch_census_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

data_subject = fetch_census_data(url_subject)
data_full = fetch_census_data(url_full)

df_subject = pd.DataFrame(data_subject[1:], columns=data_subject[0])
df_full = pd.DataFrame(data_full[1:], columns=data_full[0]) 

# Upload the file to S3
csv_buffer = StringIO()
df_subject.to_csv(csv_buffer, index=False)

# Create S3 client
s3 = boto3.client('s3')
bucket_name='swb-321-irs990-teos'
key='bronze/acs/acs5_subject_2024.csv'
s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=csv_buffer.getvalue()
)

# Clear the buffer for the next upload
csv_buffer.truncate(0)
csv_buffer.seek(0)

# update key for full table
key_full = 'bronze/acs/acs5_full_2024.csv'
df_full.to_csv(csv_buffer, index=False)
s3.put_object(
    Bucket=bucket_name,
    Key=key_full,
    Body=csv_buffer.getvalue()
)


