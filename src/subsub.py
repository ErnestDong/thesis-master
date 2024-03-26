# %%
import json

import pandas as pd
import seaborn as sns
from pyecharts import options as opts
from pyecharts.charts import Geo
from pyecharts.globals import ChartType
from pyecharts.render import make_snapshot
from snapshot_phantomjs import snapshot

# %load_ext sql
sns.set_theme(style="white")
engine_str = "clickhouse+native://localhost/thesis"

locations = pd.read_parquet("../data/ols.parquet")
locations["far"] = 1 - locations["treated"] - locations["middle"]

var = "treated"
condition = 1
name = "near"


def draw(var, condition, name):
    print(f"Drawing {var} {condition} {name}")
    InitOpts = opts.InitOpts(theme="light")
    c = Geo(InitOpts).add_schema(maptype="china")
    tmp = locations[["区站号", "区站经度", "区站纬度"]]
    # tmp = locations[locations[var] == condition][["区站号", "区站经度", "区站纬度"]]
    tmp_json = tmp.to_dict(orient="records")
    jsons = {}
    for i in tmp_json:
        jsons[str(i["区站号"])] = [i["区站经度"], i["区站纬度"]]
    with open(f"../lib/json/{name}.json", "w") as f:
        data = json.dumps(jsons)
        f.write(data)
    print(f"adding {len(jsons)} points")
    c = c.add_coordinate_json(f"../lib/json/{name}.json")
    c = c.set_series_opts(
        label_opts=opts.LabelOpts(is_show=True, formatter="{b}", font_size=0),
        effect_opts=opts.EffectOpts(symbol="circle", color="white"),
    ).add(
        "",
        [(str(i), 100) for i in jsons],
        type_=ChartType.EFFECT_SCATTER,
        color="white",
        symbol_size=1,
    )
    print("rendering")
    make_snapshot(
        snapshot,
        c.render(),
        f"../lib/img/{name}.png",
        is_remove_html=True,
        pixel_ratio=1,
    )


# draw("treated", 1, "near")
# draw("middle", 1, "middle")
# draw("far", 1, "far")
draw("treated", 0, "locations")

# %%
