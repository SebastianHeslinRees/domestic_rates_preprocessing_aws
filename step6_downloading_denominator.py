import os
import urllib.request

# Define the local path and remote URL
POPULATION_PATH = "data/processed/population_coc.rds"
URL_POPULATION = (
    "https://data.london.gov.uk/download/modelled-population-backseries/"
    "2b07a39b-ba63-403a-a3fc-5456518ca785/full_modelled_estimates_series_EW%282023_geog%29.rds"
)

# Create the folder if it doesn't exist
os.makedirs(os.path.dirname(POPULATION_PATH), exist_ok=True)

# Download the file if it doesn't already exist
if not os.path.exists(POPULATION_PATH):
    print("Downloading modelled population estimates...")
    urllib.request.urlretrieve(URL_POPULATION, POPULATION_PATH)
    print(f"Saved to {POPULATION_PATH}")
else:
    print("Population file already exists. Skipping download.")