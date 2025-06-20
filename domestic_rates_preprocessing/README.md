# Domestic rate preprocessing pipeline

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Status](https://img.shields.io/badge/status-active-brightgreen.svg)]()

> *A modular AWS Lambda pipeline for scraping, cleaning, combining, and standardising internal migration data from the UK Office for National Statistics (ONS).*

Built to integrate historical modelled migration estimates with released ONS Excel files, using consistent GSS geographies across time.

---

## 🔎  Overview

-  Scrapes new `.xlsx` releases from ONS
-  Cleans each year into usable `.csv` files
-  Combines into a single Parquet file
-  Harmonises legacy datasets using `gsscoder_python`
-  Outputs partitioned, queryable data for use in models and dashboards

---

## ➡️ Pipeline Steps

### 🔹 Step 1: Scrape & Upload ONS Raw Files

Scrapes the ONS website for **Detailed Internal Migration Estimates** from 2012 to 2023.

- Downloads raw `.xlsx` files
- Uploads them to S3 at:  
  `s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/raw/`

💡 These files are **raw and unprocessed**, directly as provided by ONS.

---

### 🔹 Step 2: Clean Excel Files

Reads each raw `.xlsx` file and extracts the relevant sheet (e.g., 2023 Local Authority geography), cleans up the structure.

- Removes empty rows and formats
- Outputs cleaned `.csv` files to:  
  `s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/cleaned_data/`  

---

### 🔹 Step 3: Combine Cleaned Files

Concatenates all cleaned `.csv` files from Step 2 into a single DataFrame.

- Converts into `.parquet` format
- Saves the combined dataset to:  
  `s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/cleaned_data_combined/`

---

### 🔹 Step 4: Merge With Historical Modelled Data

Merges the cleaned ONS data with modelled historical migration data located at:  
`s3://dpa-population-projection-data/population_mid_year_estimates/modelled-population-backseries/origin_destination_2002_to_2020.parquet.zip`

- Recodes `gss_in` and `gss_out` geographies from **2021 to 2023** for older data, using [`gsscoder_python`](https://github.com/Greater-London-Authority/gsscoder_python).

- Filters out self-to-self flows (`gss_in == gss_out`), which are usually excluded from migration datasets.

- Final output is written to:  
`s3://dpa-population-projection-data/population_mid_year_estimates/ons_data/clean_old_and_new_combined_series.parquet/`  
Partitioned by year for Athena/Glue compatibility.

---

## 🏗️ Project Structure
├── step1_ons_scraper.py # Scrape and upload raw Excel files
├── step2_clean_data.py # Clean raw Excel files into CSV
├── step3_combine_clean_data.py # Combine CSVs into single Parquet
├── step4_combine_series.py # Recode and merge with historical data
├── requirements.txt # All dependencies
├── lookups/ # Lookup tables and GSS mapping
├── test_*.py # Unit tests for each Lambda
├── origin_destination_2002_to_2020.parquet.zip # Historical modelled data


##  Quick Start

### Installation

Clone the repository and install locally:

bash
git clone 
cd gsscoder_python
pip install -r requirements.txt

---

## 🤝 Contributing

We welcome contributions! Please feel free to:
- 🐛 Report bugs
- 💡 Suggest features  
- 📝 Improve documentation
- 🔧 Submit pull requests

---


## 📫 Contact

For questions or feedback, please reach out to [sebastian.heslin-rees@london.gov.uk].

---

## 📄 License
Shield: [![CC BY-NC 4.0][cc-by-nc-shield]][cc-by-nc]

This work is licensed under a
[Creative Commons Attribution-NonCommercial 4.0 International License][cc-by-nc].

[![CC BY-NC 4.0][cc-by-nc-image]][cc-by-nc]

[cc-by-nc]: https://creativecommons.org/licenses/by-nc/4.0/
[cc-by-nc-image]: https://licensebuttons.net/l/by-nc/4.0/88x31.png
[cc-by-nc-shield]: https://img.shields.io/badge/License-CC%20BY--NC%204.0-lightgrey.svg

please email [sebastian.heslin-rees@london.gov.uk] for license infomation.

