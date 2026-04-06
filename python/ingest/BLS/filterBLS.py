import boto3
import pandas as pd
import io


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
    .str.replace(".1", "_np", regex=True)
)

# Filter the DataFrame for the specified regions and NAICS code (private total)
regions = ["Sioux Falls", "Billings", "Flagstaff", "Missoula"]
regions_FIPS =[4362, 1374, 2238, 3354]
df_filtered = df[(df['fips'].isin(regions_FIPS)) & (df['naics']=='10')]

# Filter the DataFrame for BlackHills region only.

# Aggregate the BlackHills regions by summing the relevant columns.

# Combine the benchmark regions with the aggregated BlackHills region. 

