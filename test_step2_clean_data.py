# test_clean_data.py
from step2_clean_data import handler

mock_event = {
    "input_path": "s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/raw/",
    "output_path": "s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/cleaned_data/"
}
result = handler(mock_event, None)
print(result)
