# %%
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine

sns.set_theme(style="white")
# %%
original_df = pd.read_parquet("../data/olsups.parquet")
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
descol = ["Coverage", "Disaster", "Neighbor", "Post", "Prem_before", "Price", "Area"]
todesc = df[descol].astype(float).describe()
# todesc = todesc.astype(int).astype(str).T.rename(columns={"50%": "median"})

# %%
desc = df.describe().T.rename(
    columns={
        "count": "观测值",
        "mean": "均值",
        "std": "标准差",
        "min": "最小值",
        "50%": "中位数",
        "max": "最大值",
    }
)
desc.drop(columns=["25%", "75%"], inplace=True)
desc.to_latex("../lib/table/desc.tex", float_format="%.2f")
# %%

tmp = df[["Coverage", "Premium"]]
tmp1 = tmp["Coverage"].quantile([0.05, 0.95])
tmp2 = tmp["Premium"].quantile([0.05, 0.95])
tmps = tmp[
    (tmp["Coverage"].between(tmp1[0.05], tmp1[0.95]))
    & (tmp["Premium"].between(tmp2[0.05], tmp2[0.95]))
]
tmps.corr()

# %%

engine = create_engine("clickhouse+native://localhost/thesis")
df2 = pd.read_sql("select `主险保费`,`主险费率`, `保险金额` from bases", engine)

# %%
tmp11 = df2["主险保费"].quantile([0.05, 0.95])
tmp22 = df2["保险金额"].quantile([0.05, 0.95])
tmpss = df2[
    (df2["主险保费"].between(1, tmp11[0.95]))
    & (df2["保险金额"].between(1, tmp22[0.95]))
]
tmpss.corr()
# %%
pg = create_engine("clickhouse+native://localhost/thesis")
df22 = pd.read_sql("select `主险保费`, `保险金额` from base", pg)

tmp11 = df2["主险保费"].quantile([0.05, 0.95])
tmp22 = df2["保险金额"].quantile([0.05, 0.95])
tmpss = df2[
    (df2["主险保费"].between(1, tmp11[0.95]))
    & (df2["保险金额"].between(1, tmp22[0.95]))
]
tmpss.corr()
# %%
categories = ["Disaster", "Neighbor"]
res = []
for cat in categories:
    non_cat = "Neighbor" if cat == "Disaster" else "Disaster"
    robust = pd.read_parquet(f"../data/robust_{cat}.parquet")
    robust = robust[robust[non_cat] == 0]
    tmp = robust.groupby([cat, "Quarter"]).mean()["Coverage"].reset_index(cat)
    if cat == "Disaster":
        res.append(tmp[tmp["Disaster"] == 1])
    else:
        res.append(tmp)
tmp = pd.concat(res)
lmd = lambda x: "Neighbor" if x["Neighbor"] == 1 else "Control"
tmp["Category"] = tmp.apply(
    lambda x: "Disaster" if x["Disaster"] == 1 else lmd(x), axis=1
)
tmp = tmp[(tmp.index > -5) & (tmp.index < 4)]
plt.figure(figsize=(8, 4))
sns.lineplot(data=tmp, x="Quarter", y="Coverage", hue="Category")
plt.savefig("../lib/img/robust.png")

# %%
