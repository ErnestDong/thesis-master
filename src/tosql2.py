# %%
import pandas as pd
from clickhouse_driver import Client

gdp = pd.read_excel("../data/GDP.xlsx", skipfooter=2, index_col=0)
gdp.rename(columns=lambda x: x.replace(":GDP", ""), inplace=True)
gdp.index = gdp.index.map(lambda x: x.year)
gdp = gdp.pct_change().stack().reset_index()
gdp.columns = ["year", "province", "gdp"]

# %%
density = pd.read_excel(
    "../data/保险密度.xlsx", skiprows=[i for i in range(29) if i != 2], index_col=0
).drop(columns=["厦门", "大连"])

density.index = density.index.map(lambda x: x.year)
density = density.stack().reset_index()
density.columns = ["year", "province", "保险密度"]
# %%
depth = pd.read_excel(
    "../data/保险深度.xlsx", skiprows=[i for i in range(29) if i != 2], index_col=0
).drop(columns=["厦门", "大连"])
depth.index = depth.index.map(lambda x: x.year)
depth = depth.stack().reset_index()
depth.columns = ["year", "province", "保险深度"]

# %%
df = gdp.merge(density, on=["year", "province"]).merge(depth, on=["year", "province"])

# %%
client = Client("localhost", settings={"use_numpy": True})
client.execute(
    "create or replace table thesis.gdp (`year` Int32, `province` String, `gdp` Float64, `保险密度` Float64, `保险深度` Float64) engine = MergeTree order by (year, province)"
)

client.insert_dataframe("insert into thesis.gdp values", df)
# %%
