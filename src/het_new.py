# %%
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.api as sm
import statsmodels.formula.api as smf
from sqlalchemy import create_engine
from stargazer.stargazer import Stargazer


def tablelize(star):
    string = (
        star.render_latex()
        .replace("\\begin{table}[!htbp] \\centering", "")
        .replace("\\end{table}", "")
        .replace("_", "\\_")
    )
    return re.sub(r"\(df=[\d; ]*\)", "", string)


engine = create_engine("clickhouse+native://localhost/thesis")
log = np.log
sns.set_theme(style="white")
# original_df = pd.read_sql("ols_ups_far", engine)
original_df = pd.read_parquet("../data/olsups.parquet")
sns.histplot(original_df["distance"], kde=True)
df = original_df[original_df["保险金额"] > 0].copy()
df["历史投保"] = df["上年保单号"].map(lambda x: 1 if x else 0)
df = df[(df["t"] > 1999) & (df["t"] < 2014)]
df["ti"] = df["t"].astype(str)
df["是否理赔"] = df["total_claim"].map(lambda x: 1 if x > 0 else 0)
df.rename(
    columns={
        "保费合计": "Premium",
        "保险金额": "Coverage",
        "middle": "Neighbor",
        "treated": "Disaster",
        "after": "Post",
        "历史投保": "Prem_before",
        "保险财产购置价": "Price",
        "建筑面积": "Area",
        "是否理赔": "Claim",
    },
    inplace=True,
)
df["Price"] = df["Price"] / 1000000
df.head()
# %%
groups = "Coverage"
hdf = df[df[groups] > 0]
quantiles = hdf[groups].quantile([0.05, 0.35, 0.65, 0.95]).values.tolist()
hdf = hdf[hdf[groups].between(quantiles[0], quantiles[-1])]
stars = []

for category in ["Disaster", "Neighbor"]:
    for i in range(len(quantiles) - 1):
        noncat = "Neighbor" if category == "Disaster" else "Disaster"
        model = smf.ols(
            f"log(Coverage) ~ {category}*Post",
            data=hdf[
                (hdf[groups].between(quantiles[i], quantiles[i + 1]))
                & (hdf[noncat] == 0)
            ],
        ).fit()
        stars.append(model)
stargazer = Stargazer(stars)
stargazer.custom_columns(["Low", "Middle", "High"] * 2)
# with open("../lib/table/het_cov.tex", "w") as f:
#     f.write(tablelize(stargazer))
stargazer

# %%
with open("../data/city.txt") as f:
    cities = f.read().split("\n")
cities = [i.split(" ") for i in cities]
cities = {i[0]: i[1] for i in cities if len(i) == 2}
parsing_cities = {}
for i in cities:
    if i[:2] not in parsing_cities:
        parsing_cities[i[:2]] = {"province": cities[i[:2] + "0000"][:2]}
    if i[:4] not in parsing_cities[i[:2]]:
        parsing_cities[i[:2]][i[:4]] = {"city": cities[i[:4] + "00"][:2]}
    parsing_cities[i[:2]][i[:4]][i] = cities[i][:2]

tmp = df[["省份", "站名"]].dropna().drop_duplicates().to_dict(orient="records")
tmp

result = {}
missing = {}
for items in tmp:
    province = [
        i for i in parsing_cities.values() if i["province"] == items["省份"][:2]
    ][0]
    city = [
        i
        for i in province
        if i != "province" and province[i]["city"] == items["站名"][:2]
    ]
    if (city and "县" not in items["站名"]) or province["province"] in [
        "北京",
        "上海",
        "天津",
        "重庆",
    ]:
        result[items["站名"]] = {
            "type": "城市",
            "province": items["省份"],
            "city": items["站名"],
            "rural": None,
        }
        continue
    else:
        for city in province:
            if city != "province":
                for rural in province[city]:
                    if province[city][rural] == items["站名"][:2]:
                        result[items["站名"]] = {
                            "type": "农村",
                            "province": items["省份"],
                            "city": province[city]["city"],
                            "rural": items["站名"],
                        }
                        continue
                else:
                    continue
        else:
            if items["站名"] not in result:
                result[items["站名"]] = {
                    "type": "农村",
                    "province": items["省份"],
                    "city": None,
                    "rural": items["站名"],
                }
                missing[items["站名"]] = items["省份"]

["https://www.google.com/search?q=" + missing[i] + i for i in missing]

# %%
missing_mod = [
    "洪家",
    "耀县",
    "陵县",
    "万县",
    "达县",
    "儋县",
    "贺县",
    "滁县",
    "宿县",
    "达板城",
    "尚丘",
]
for i in missing_mod:
    result[i].update({"type": "城市", "city": i, "rural": None})

# %%
df = df.dropna(subset=["站名"])
df["rural"] = df["站名"].map(lambda x: result[x]["type"])
stars = []
labels = []
for rural, group in df.groupby("rural"):
    for category in ["Disaster", "Neighbor"]:
        labels.append(rural)
        print(rural, category)
        noncat = "Neighbor" if category == "Disaster" else "Disaster"
        model = smf.ols(
            f"log(Coverage) ~ {category}*Post",
            data=group[group[noncat] == 0],
        ).fit()
        stars.append(model)
stargazer = Stargazer(stars)
stargazer.custom_columns(labels)
with open("../lib/table/het_rur.tex", "w") as f:
    f.write(tablelize(stargazer))
stargazer

# %%
