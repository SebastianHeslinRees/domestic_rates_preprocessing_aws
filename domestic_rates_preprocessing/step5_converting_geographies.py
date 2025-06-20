import pandas as pd
import pyarrow.dataset as ds
import os

# File paths (local or S3-mounts)
FLOWS_PATH = "data/processed/domestic_od_flows/"
LOOKUP_REGION = "lookups/lookup_lad_rgn_ctry.csv"
LOOKUP_COUNTRY = "lookups/lookup_lad_ctry.csv"
LOOKUP_INNER_OUTER = "lookups/lookup_lad_inner_outer_london.csv"

# Load Parquet dataset
dataset = ds.dataset(FLOWS_PATH, format="parquet", partitioning="hive")

# Convert to DataFrame
df = dataset.to_table().to_pandas()

# --- 1. Create gross flows ---
def create_gross_flows(df, rounding=1):
    """Returns total inflow and outflow for each area and year."""
    #Sums people moving out and in of each LAD (gss_out) per year.
    inflow = df.groupby(['gss_in', 'year'])['value'].sum().reset_index().rename(columns={"gss_in": "gss_code", "value": "inflow"})
    outflow = df.groupby(['gss_out', 'year'])['value'].sum().reset_index().rename(columns={"gss_out": "gss_code", "value": "outflow"})
    merged = pd.merge(inflow, outflow, on=['gss_code', 'year'], how='outer').fillna(0)
    merged["inflow"] = merged["inflow"].round(rounding)
    merged["outflow"] = merged["outflow"].round(rounding)
    return merged

lad_gross_flows = create_gross_flows(df)

# --- 2. Region Aggregation ---
def aggregate_to_region(df, lookup_path, name="region"):
    lookup = pd.read_csv(lookup_path)
    df = df.merge(lookup, left_on='gss_in', right_on='lad_code')
    df['region_in'] = df[name]
    df = df.merge(lookup, left_on='gss_out', right_on='lad_code')
    df['region_out'] = df[name + '_y']

    # Summarise
    agg = df.groupby(['region_in', 'region_out', 'year'])['value'].sum().reset_index()
    agg.columns = ['gss_in', 'gss_out', 'year', 'value']
    return agg

region_od_data = aggregate_to_region(df, LOOKUP_REGION, name="region_code")
country_od_data = aggregate_to_region(df, LOOKUP_COUNTRY, name="country_code")
inner_outer_od_data = aggregate_to_region(df, LOOKUP_INNER_OUTER, name="io_london")

# --- 3. Gross flows by aggregation ---
region_gross_flows = create_gross_flows(region_od_data)
country_gross_flows = create_gross_flows(country_od_data)
inner_outer_gross_flows = create_gross_flows(inner_outer_od_data)

# --- 4. Combine country + region + Inner/Outer flows ---
final_gross = pd.concat([
    country_gross_flows[country_gross_flows['gss_code'] == "E92000001"],  # England only
    region_gross_flows,
    inner_outer_gross_flows[inner_outer_gross_flows['gss_code'] != "other"]
])

# Save (optional)
lad_gross_flows.to_parquet("data/processed/lad_gross_flows.parquet", index=False)
region_od_data.to_parquet("data/processed/region_od_series.parquet", index=False)
country_od_data.to_parquet("data/processed/ctry_od_series.parquet", index=False)
inner_outer_od_data.to_parquet("data/processed/inner_outer_london_od_data.parquet", index=False)
final_gross.to_parquet("data/processed/ctry_region_gross_flows.parquet", index=False)
