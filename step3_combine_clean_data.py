import boto3
import pandas as pd
import os

s3 = boto3.client('s3')

def handler(event, context):
    print("Starting combine_cleaned_data Lambda function...")

    input_path = event.get("input_path")  # e.g., s3://bucket/population_mid_year_estimates/ons_data/2_cleaned_data/
    output_path = event.get("output_path")  # e.g., s3://bucket/population_mid_year_estimates/ons_data/cleaned_data_combined/3_cleaned_data_combined.parquet

    if not input_path or not output_path:
        print("Missing 'input_path' or 'output_path' in event.")
        return {"status": "error", "message": "input_path and output_path are required"}

    # Parse S3 paths
    in_bucket, in_prefix = parse_s3_path(input_path)
    out_bucket, out_key = parse_s3_path(output_path)

    print(f"Input Bucket: {in_bucket}, Prefix: {in_prefix}")
    print(f"Output Bucket: {out_bucket}, Key: {out_key}")

    # List files
    all_files = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=in_bucket, Prefix=in_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(".csv"):
                all_files.append(key)

    if not all_files:
        print("No .csv files found.")
        return {"status": "no files"}

    print(f"Found {len(all_files)} files to combine.")

    combined_df = pd.DataFrame()

    for key in all_files:
        print(f"Processing file: {key}")
        local_path = f"/tmp/{os.path.basename(key)}"
        s3.download_file(in_bucket, key, local_path)
        df = pd.read_csv(local_path)
        df.columns = df.columns.str.lower()  # column headers to lowercase
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Save combined file
    output_local_path = "/tmp/new_series_lad.csv"
    combined_df.to_parquet(output_local_path, index=False)

    print(f"Uploading combined file to S3: {out_bucket}/{out_key}")
    s3.upload_file(output_local_path, out_bucket, out_key)

    print("✅ Combined file successfully uploaded.")
    return {
        "status": "done",
        "files_combined": len(all_files),
        "output_file": f"s3://{out_bucket}/{out_key}"
    }

def parse_s3_path(s3_path):
    """Parse an S3 path into bucket and key/prefix"""
    if not s3_path.startswith("s3://"):
        raise ValueError("S3 path must start with s3://")
    parts = s3_path[5:].split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key
