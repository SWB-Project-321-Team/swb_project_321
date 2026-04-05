import pandas as pd
from bs4 import BeautifulSoup
from urllib import request
from urllib.request import urlopen
import requests
import boto3

url = "https://www.bls.gov/bdm/nonprofits/tables/qcew-nonprofits-2022.xlsx"
out_file = "qcew-nonprofits-2022.xlsx"
headers =     {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',}
# df = pd.read_excel(url, sheet_name='2022_nonprofit_data', skiprows=3)
# print(df.head())
# download the file using requests
r = requests.get(url, headers=headers, timeout=30)
r.raise_for_status()  # Check if the request was successful

# Create S3 client
s3 = boto3.client('s3')

# Upload the file to S3
bucket_name='swb-321-irs990-teos'
key='bronze/bls/qcew-nonprofits-2022.xlsx'

s3.put_object(
    Bucket=bucket_name,
    Key=key,
    Body=r.content
)