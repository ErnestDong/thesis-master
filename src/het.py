# %%
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import statsmodels.formula.api as smf
from numpy import log
from sqlalchemy import create_engine
from stargazer.stargazer import Stargazer

engine = create_engine("clickhouse+native://localhost:9000/thesis")

sns.set_theme(style="white")
east = {
    i: "East"
    for i in "北京、天津、河北、上海、江苏、浙江、福建、山东、广东、辽宁、海南".split(
        "、"
    )
}
middle = {
    i: "Middle" for i in "山西、吉林、黑龙江、安徽、江西、河南、湖北、湖南".split("、")
}
west = {
    i: "West"
    for i in "重庆、四川、内蒙古、广西、贵州、云南、西藏、陕西、甘肃、青海、宁夏、新疆".split(
        "、"
    )
}
northeast = {i: "NorthEast" for i in "".split("、")}
provinces = {**east, **middle, **west, **northeast}
east = {
    i: "East"
    for i in "北京、天津、河北、上海、江苏、浙江、福建、山东、广东、辽宁、海南".split(
        "、"
    )
}
middle = {
    i: "Middle" for i in "山西、吉林、黑龙江、安徽、江西、河南、湖北、湖南".split("、")
}
west = {
    i: "West"
    for i in "重庆、四川、内蒙古、广西、贵州、云南、西藏、陕西、甘肃、青海、宁夏、新疆".split(
        "、"
    )
}
provinces = {**east, **middle, **west}
# %%
rainings = pd.read_sql(
    "select * from history_raining hr left join location l on hr.`区站号`=l.`区站号`",
    engine,
)

rainings["region"] = rainings["省份"].map(provinces)

x = 0.05
rainings["raining(mm)"] = rainings["20-20时累计降水量"] / 5
low, high = rainings["raining(mm)"].quantile([x, 1 - x])
sns.boxplot(
    x="region",
    y="raining(mm)",
    data=rainings[rainings["raining(mm)"].between(low, high)],
)
plt.savefig("../lib/img/rainings.png")
# %%
df = pd.read_parquet("../data/olsups.parquet")
df["region"] = df["省份"].map(provinces)
df["raining"] = df["累计降水量"]
low, high = df["raining"].quantile([x, 1 - x])
sns.boxplot(x="region", y="raining", data=df[df["raining"].between(low, high)])
# %%
# %%
df["Post"] = df["after"]
nonwest = df[df["region"] != "West"]
west = df[df["region"] == "West"]

nonwest_before = nonwest[nonwest["Post"] == 0]["保险金额"].mean()
nonwest_after = nonwest[nonwest["Post"] == 1]["保险金额"].mean()
west_before = west[west["Post"] == 0]["保险金额"].mean()
west_after = west[west["Post"] == 1]["保险金额"].mean()
nonwest_before, nonwest_after, west_before, west_after
# %%
res = pd.DataFrame(
    {
        "region": ["ex. West", "West"],
        "before": [nonwest_before, west_before],
        "after": [nonwest_after, west_after],
    }
)
res.set_index("region", inplace=True)
res = res.T
# %%
cmap = sns.cubehelix_palette(2)
sns.set_palette(cmap)

sns.set_style("white", {"font.sans-serif": ["STHeiti"], "figsize": (8, 6)})
plt.clf()
fig, ax = plt.subplots()
p=sns.barplot(data=res.stack().reset_index(), ax=ax, x="level_0", y=0, hue="region")
p.set_yscale("log")
p.set_ylabel("平均保额(元)")
p.set_xlabel(None)

plt.savefig("../lib/img/covbyregion.png")
# %%
