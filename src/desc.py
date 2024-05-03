# %%
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sqlalchemy import create_engine

# sns.set_theme(style="white")
sns.set_style("white", {"font.sans-serif": ["STHeiti"]})
# %%

df = pd.read_parquet("../data/df.parquet")
df["历史投保"] = df["上年保单号"].map(lambda x: 1 if x else 0)
df = df[(df["t"] > 1999) & (df["t"] < 2014)]
df["ti"] = df["t"].astype(str)
df.head()

# %%
descol = [
    "Coverage",
    "Disaster",
    "Neighbor",
    "Post",
    "Price",
    "GDP",
    "Density",
    "Penetration",
    "Prem_before",
    "Claim",
    "Premium",
    "total_claim",
    "保费",
    "累计降水量",
]

todesc = df[descol].astype(float).describe()
todesc = todesc.rename(
    columns={"total_claim": "累计赔付额", "Prem_before": "Prem\\_before"}
)
# todesc = todesc.astype(int).astype(str).T.rename(columns={"50%": "median"})
todesc
# %%
desc = todesc.T.rename(
    columns={
        "count": "观测数",
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

sns.displot(np.log(df[df["Disaster"] == 1]["累计降水量"]), kde=True)
plt.savefig("../lib/img/precip.png")
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
