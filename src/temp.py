# %%
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import statsmodels.formula.api as smf
from pyecharts.charts import Geo
from pyecharts import options as opts
from pyecharts.globals import ChartType
from pyecharts.render import make_snapshot
from snapshot_phantomjs import snapshot

# %load_ext sql
sns.set_style("white", {"font.sans-serif": ["STHeiti"], "figsize": (8, 6)})
engine_str = "clickhouse+native://localhost/thesis"
# engine_str = "clickhouse+native://default:Z4cfvnADI5MM.@simfv776a0.ap-south-1.aws.clickhouse.cloud/thesis"
# %sql $engine_str

distance = pd.read_sql("res", con=engine_str)
dist = distance["distance"]
dist.name = "气象站到保单位置距离(km)"
p = sns.histplot(dist, fill=True, kde=True)
p.set_ylabel("计数")
plt.savefig("../lib/img/distance.png")

# %%
locations = pd.read_sql(
    """with asdf as(
    select
        *
    from
        locations hr,
        locations hr2
)
select
    *,
    geoDistance(
        asdf.`经度`,
        asdf.`纬度`,
        asdf.`hr2.经度`,
        asdf.`hr2.纬度`
    )/1000 as dist
from
    asdf
order by dist""",
    con=engine_str,
)
locations = locations[~(locations["区站号"] == locations["hr2.区站号"])]
locations = locations.sort_values(
    by=["区站号", "dist"], ascending=[True, True]
).drop_duplicates(subset=["区站号"], keep="first")
locations = locations["dist"]
locations.name = "气象站间距离(km)"
p = sns.histplot(locations, kde=True)
p.set_ylabel("计数")
plt.savefig("../lib/img/locations_distance.png")

# %%
