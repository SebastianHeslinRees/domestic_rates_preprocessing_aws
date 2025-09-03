import boto3
import pandas as pd
import os
import zipfile
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq
import shutil
from gsscoder_python import recode_gss
from smart_open import open as smart_open  # Avoid conflict with built-in open
import io

# ----------------------------- Configuration -----------------------------
s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

# Constants
S3_PARQUET_ZIP_S3_URI = "s3://dpa-population-projection-data/population_mid_year_estimates/modelled-population-backseries/origin_destination_2002_to_2020.parquet.zip"
S3_NEW_SERIES_PATH = "population_mid_year_estimates/ons_data/cleaned_data_combined/cleaned_data_combined.parquet"
OUTPUT_PARQUET_PREFIX = "population_mid_year_estimates/ons_data/4_clean_old_and_new_combined_series.parquet"
TMP_DIR = "/tmp"

START_YR_NEW_SERIES = 2012
GSS_OLD_YEAR = 2021
GSS_NEW_YEAR = 2023
BUCKET_NAME = "dpa-population-projection-data"

def handler(event, context):
    print("🚀 Starting combine_series Lambda...")

    # 1. Stream and unzip old series parquet from S3
    print("📥 Streaming ZIP from S3 using smart_open...")
    with smart_open(S3_PARQUET_ZIP_S3_URI, 'rb') as s3_stream:
        zip_bytes = io.BytesIO(s3_stream.read())

    with zipfile.ZipFile(zip_bytes) as z:
        # Assume first file inside is the Parquet file
        parquet_name = z.namelist()[0]
        print(f"📦 Unzipping and reading {parquet_name}")
        with z.open(parquet_name) as f:
            df_old = pd.read_parquet(f)

    df_old = df_old[df_old['year'] < START_YR_NEW_SERIES]
    print(f"✅ Loaded old series shape: {df_old.shape}")

    # 2. Recode gss_in
    df_in_gss = df_old[df_old['gss_in'].str.contains("E0|W0")]
    df_not_in_gss = df_old[~df_old['gss_in'].str.contains("E0|W0")]
    
    #df_in_gss = recode_gss(df_in_gss, "gss_in", "value", GSS_OLD_YEAR, GSS_NEW_YEAR)
    #view full function recode_gss
    print(f"🔄 Recoding gss_in from year {GSS_OLD_YEAR} to {GSS_NEW_YEAR}...")
    df_in_gss = recode_gss(
        df_in=df_in_gss,
        col_code='gss_in',
        col_data='value',
        fun='sum',
        recode_from_year=GSS_OLD_YEAR,
        recode_to_year=GSS_NEW_YEAR,
        )

    df_old_recoded_in = pd.concat([df_in_gss, df_not_in_gss], ignore_index=True)

    # 3. Recode gss_out
    df_out_gss = df_old_recoded_in[df_old_recoded_in['gss_out'].str.contains("E0|W0")]
    df_not_out_gss = df_old_recoded_in[~df_old_recoded_in['gss_out'].str.contains("E0|W0")]
    
    #df_out_gss = recode_gss(df_out_gss, "gss_out", "value", GSS_OLD_YEAR, GSS_NEW_YEAR)
    #view full function recode_gss
    print(f"🔄 Recoding gss_out from year {GSS_OLD_YEAR} to {GSS_NEW_YEAR}...")
    df_out_gss = recode_gss(
        df_in=df_out_gss,
        col_code='gss_out',
        col_data='value',
        fun='sum',
        recode_from_year=GSS_OLD_YEAR,
        recode_to_year=GSS_NEW_YEAR,
    )
    
    
    
    df_old_recoded = pd.concat([df_out_gss, df_not_out_gss], ignore_index=True)

    print(f"✅ Recoded old series shape: {df_old_recoded.shape}")

    # 4. Load new series from S3
    tmp_csv_path = os.path.join(TMP_DIR, "new_series.csv")
    print(f"📥 Downloading new series from S3: {S3_NEW_SERIES_PATH}")
    s3.download_file(BUCKET_NAME, S3_NEW_SERIES_PATH, tmp_csv_path)
    #new_df = pd.read_csv(tmp_csv_path)
    new_df = pd.read_parquet(tmp_csv_path)
    print(f"✅ Loaded new series shape: {new_df.shape}")

    # 5. Combine and filter
    df_combined = pd.concat([df_old_recoded, new_df], ignore_index=True)
    df_combined = df_combined[df_combined['gss_in'] != df_combined['gss_out']]
    print(f"🧩 Combined series shape after filtering: {df_combined.shape}")

    # 6. Partition and write Parquet by year
    grouped = df_combined.groupby("year")
    for year, group in grouped:
        table = pa.Table.from_pandas(group)
        output_path = os.path.join(TMP_DIR, f"parquet_year_{year}")
        pq.write_to_dataset(table, root_path=output_path, partition_cols=["year"])

        for root, _, files in os.walk(output_path):
            for file in files:
                if file.endswith(".parquet"):
                    s3_key = os.path.join(
                        OUTPUT_PARQUET_PREFIX,
                        f"year={year}",
                        file
                    )
                    local_file_path = os.path.join(root, file)
                    print(f"⬆️ Uploading {local_file_path} to s3://{BUCKET_NAME}/{s3_key}")
                    s3.upload_file(local_file_path, BUCKET_NAME, s3_key)

    print("✅ All done.")
    return {"status": "done", "years_written": grouped.ngroups}


