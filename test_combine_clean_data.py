from step3_combine_clean_data import handler

event = {
    "input_path": "s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/cleaned_data/",
    "output_path": "s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/cleaned_data_combined/cleaned_data_combined.parquet"
}

handler(event, None)
