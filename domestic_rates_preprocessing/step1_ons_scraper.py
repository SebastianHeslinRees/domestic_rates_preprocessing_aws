import requests
from bs4 import BeautifulSoup
import boto3
import os

s3 = boto3.client('s3')
bucket_name = 'dpa-population-projection-data'  # No s3:// prefix

def lambda_handler(event, context):
    print("Starting ONS data scraping Lambda...")

    base_url = "https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/internalmigrationinenglandandwales/"
    print(f"Fetching base page: {base_url}")
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    print("Parsing download links...")
    links = [a['href'] for a in soup.find_all('a', href=True) if 'detailedinternalmigrationestimates' in a['href']]
    print(f"Found {len(links)} relevant links.")

    download_links = []

    for link in links:
        full_url = f"https://www.ons.gov.uk{link}"
        filename = full_url.split("/")[-1]
        local_path = f"/tmp/{filename}"

        print(f"Downloading file: {full_url}")
        r = requests.get(full_url)
        with open(local_path, "wb") as f:
            f.write(r.content)
        print(f"Saved to local path: {local_path}")

        s3_key = f"population_mid_year_estimates/ons_data/raw/{filename}"
        print(f"Uploading to S3: s3://{bucket_name}/{s3_key}")
        s3.upload_file(local_path, bucket_name, s3_key)

        s3_uri = f"s3://{bucket_name}/{s3_key}"
        download_links.append(s3_uri)
        print(f"✅ Upload complete: {s3_uri}")

    print("All downloads and uploads complete.")

    return {
        "statusCode": 200,
        "download_links": download_links
    }



