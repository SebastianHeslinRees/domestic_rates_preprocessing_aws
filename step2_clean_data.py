import pandas as pd
import boto3
import os

s3 = boto3.client('s3')

def handler(event, context):
    print("Starting clean_data Lambda function...")

    input_path = event.get("input_path")
    output_path = event.get("output_path")
    if not input_path or not output_path:
        print("Missing 'input_path' or 'output_path' in event.")
        return {"status": "error", "message": "input_path and output_path are required"}

    # Parse bucket and prefix from input_path
    if not input_path.startswith("s3://"):
        raise ValueError("input_path must start with s3://")
    path_parts = input_path[5:].split("/", 1)
    bucket = path_parts[0]
    prefix = path_parts[1] if len(path_parts) > 1 else ""

    # Parse output bucket and prefix similarly
    if not output_path.startswith("s3://"):
        raise ValueError("output_path must start with s3://")
    out_parts = output_path[5:].split("/", 1)
    out_bucket = out_parts[0]
    out_prefix = out_parts[1] if len(out_parts) > 1 else ""

    print(f"Input Bucket: {bucket}, Prefix: {prefix}")
    print(f"Output Bucket: {out_bucket}, Prefix: {out_prefix}")

    # List all files under prefix
    print(f"Listing objects in bucket '{bucket}' with prefix '{prefix}'...")
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith(".xlsx") or key.endswith(".xls"):
                files.append(key)

    if not files:
        print("No Excel files found to process.")
        return {"status": "no files"}

    print(f"Found {len(files)} files to process.")

    for key in files:
        filename = key.split("/")[-1]
        tmp_path = f"/tmp/{filename}"

        print(f"\nDownloading {key} from bucket {bucket}...")
        s3.download_file(bucket, key, tmp_path)

        print(f"Reading Excel file {tmp_path}...")
        df = pd.read_excel(tmp_path, sheet_name=4)

        print("Cleaning data (dropping NA rows)...")
        df_clean = df.dropna()

        # Rename file if it includes "2021and2023"
        clean_filename = filename.replace('2021and2023', '2023').replace('.xlsx', '.csv').replace('.xls', '.csv')
        clean_key = f"{out_prefix}{clean_filename}"
        clean_tmp_path = f"/tmp/clean_{clean_filename}"
        df_clean.to_csv(clean_tmp_path, index=False)

        print(f"Uploading cleaned file to {out_bucket}/{clean_key} ...")
        s3.upload_file(clean_tmp_path, out_bucket, clean_key)

        print(f"✅ Finished processing {filename}")

    print("All files processed.")
    return {"status": "done", "files_processed": len(files)}
