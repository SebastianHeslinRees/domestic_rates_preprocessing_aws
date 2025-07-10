import pandas as pd
import pyarrow.dataset as ds
import pyreadr

# Step 6 output: population models saved to disk
POP_PATH = "data/processed/population_coc.rds"

# Step 5 output: partitioned OD flows
FLOWS_PATH = "data/processed/domestic_od_flows/"

# Lookups from Step 4
LOOKUP_REGION = "lookups/lookup_lad_rgn_ctry.csv"
LOOKUP_INNER_OUTER = "lookups/lookup_lad_inner_outer_london.csv"

# Step 7 output
OUT_NET_FLOWS_RDS = "data/processed/in_out_net_flows.rds"
OUT_NET_FLOWS_CSV = "data/processed/domestic_flows_children.csv"

# 1. Load partitioned OD flows (result of Step 5)
ds_lad = ds.dataset(FLOWS_PATH, format="parquet", partitioning="hive")
df_lad_od = ds_lad.to_table().to_pandas()

# 2. Load population data (downloaded in Step 6)
pop = pyreadr.read_r(POP_PATH)[None]
# Example use: You could merge population counts into df_lad_od here if needed

# 3. Load lookup tables for region and inner/outer London
lookup_reg = pd.read_csv(LOOKUP_REGION)
lookup_io = pd.read_csv(LOOKUP_INNER_OUTER)

# 4. Compute flows by age <= 15 between LAD and regions
years = df_lad_od['year'].unique()
ldn_region = []
for year in years:
    sub = df_lad_od[df_lad_od['year']==year].query('age <= 15')
    merged = sub.merge(lookup_reg, left_on='gss_in', right_on='lad_code') \
                .merge(lookup_reg, left_on='gss_out', right_on='lad_code', suffixes=('_in','_out'))
    agg = merged.groupby(['RGNCD_in','RGNCD_out','age','year'])['value'].sum().reset_index()
    ldn_region.append(agg.rename(columns={'RGNCD_in':'gss_in','RGNCD_out':'gss_out'}))
ldn_region = pd.concat(ldn_region, ignore_index=True)

# 5. Compute total flows (in+out) across all OD pairs
total_flows = []
for df in [ldn_region]:
    df_in = df.copy()
    df_in['gss_out'], df_in['value'] = 'total', df_in['value']
    tot_in = df_in.groupby(['gss_in','age','year'])['value'].sum().reset_index()
    tot_in['gss_out'] = 'total'
    total_flows.append(tot_in)

    df_out = df.copy()
    df_out['gss_in'], df_out['value'] = 'total', df_out['value']
    tot_out = df_out.groupby(['gss_out','age','year'])['value'].sum().reset_index()
    tot_out['gss_in'] = 'total'
    total_flows.append(tot_out)

total_flows = pd.concat(total_flows, ignore_index=True)

# 6. Inner/Outer London flows for age <=15, filtering by relevant codes
ldn_io = []
for year in years:
    sub = df_lad_od[df_lad_od['year']==year].query('age <= 15')
    sub = sub[sub['gss_in'].isin(['E13000001','E13000002']) & 
              sub['gss_out'].isin(['E13000001','E13000002'])]
    md = sub.merge(lookup_io, left_on='gss_in', right_on='lad_code') \
            .merge(lookup_io, left_on='gss_out', right_on='lad_code', suffixes=('_in','_out'))
    agg = md.groupby(['RGNCD_in','RGNCD_out','age','year'])['value'].sum().reset_index()
    ldn_io.append(agg.rename(columns={'RGNCD_in':'gss_in','RGNCD_out':'gss_out'}))
inner_outer = pd.concat(ldn_io, ignore_index=True)

# 7. London-region and inner/outer flows combined
london_region = ldn_region.copy()
london_region = london_region[ldn_region['gss_in'].str.startswith('E09')].groupby(['year','age','gss_out'])['value'].sum().reset_index()
london_region[['gss_in']] = 'E12000007'

# 8. Consolidate all flows
all_od = pd.concat([ldn_region, inner_outer, london_region, total_flows], ignore_index=True)

# 9. Pivot to in/out/net and wide format
# net = inflow - outflow
flows = all_od.copy()
in_ = flows.copy()
in_['inflow'] = in_['value']; in_ = in_.drop(columns='value')
out = flows.copy(); out.columns = ['gss_out','gss_in','age','year','outflow']
net = in_.merge(out, on=['gss_in','gss_out','age','year'], how='outer').fillna(0)
net['netflow'] = net['inflow'] - net['outflow']

# Pivot age groups
wide = net.melt(id_vars=['gss_code','origin_destination_code','age','year'], 
                value_vars=['inflow','outflow','netflow'],
                var_name='direction', value_name='value') \
          .pivot_table(index=['gss_code','origin_destination_code','direction','year'], 
                       columns='age', values='value', fill_value=0).reset_index()

# 10. Save results
import pyreadr
pyreadr.write_rds(net, OUT_NET_FLOWS_RDS)
wide.to_csv(OUT_NET_FLOWS_CSV, index=False)