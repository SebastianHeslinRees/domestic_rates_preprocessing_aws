import requests
from bs4 import BeautifulSoup
#import boto3  # Commented out for local testing
import os

s3 = boto3.client('s3')  # Commented out for local testing
bucket_name = 'dpa-population-projection-data'  # Commented out for local testing

def lambda_handler(event=None, context=None):
    print("Starting ONS data scraping...")

    base_url = "https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/internalmigrationinenglandandwales/"
    print(f"Fetching base page: {base_url}")
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    print("Parsing download links...")
    links = [a['href'] for a in soup.find_all('a', href=True) if 'detailedinternalmigrationestimates' in a['href']]
    print(f"Found {len(links)} relevant links.")

    download_links = []

    # Create test_data directory if it doesn't exist
    os.makedirs("test_data", exist_ok=True)

    for link in links:
        full_url = f"https://www.ons.gov.uk{link}"
        filename = full_url.split("/")[-1]
        local_path = f"test_data/{filename}"

        print(f"Downloading file: {full_url}")
        r = requests.get(full_url)
        with open(local_path, "wb") as f:
            f.write(r.content)
        print(f"Saved to local path: {local_path}")

        s3_key = f"population_mid_year_estimates/ons_data/1_raw/{filename}"
        print(f"Uploading to S3: s3://{bucket_name}/{s3_key}")
        s3.upload_file(local_path, bucket_name, s3_key)
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        download_links.append(s3_uri)

        # Instead, use local file path
        download_links.append(local_path)

    print("All downloads saved successfully.")

    return {
        "statusCode": 200,
        "download_links": download_links
    }

# Run locally
if __name__ == "__main__":
    result = lambda_handler()
    print(result)



