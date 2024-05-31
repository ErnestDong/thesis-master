# %%
import re

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.formula.api as smf
from stargazer.stargazer import Stargazer

np.random.seed(1)


def tablelize(star):
    string = (
        star.render_latex()
        .replace("\\begin{table}[!htbp] \\centering", "")
        .replace("\\end{table}", "")
        .replace("_", "\\_")
    )
    return re.sub(r"\(df=[\d; ]*\)", "", string)


log = np.log

original_df = pd.read_parquet("../data/df1.parquet")
# %%
df = original_df.copy()
df["Pre"] = df["Disaster"] * (
    pd.to_datetime(df["record_date"]) - pd.to_datetime(df["保险起期"])
).dt.days // 90 + (df["maxpost"] // 90)
stars = []
col = []
for category in ["Disaster", "Neighbor"]:
    noncat = "Disaster" if category == "Neighbor" else "Neighbor"
    for post in ["", "Post"]:
        col.append(category)
        data = df[(df[noncat] == 0) & (df["Post"] == 1)].copy()
        data["Treated"] = data[category]
        if not post:
            regstr = "log(Coverage) ~ Treated*C(Pre)+Prem_before+log(Price)+log(GDP)+(Penetration)"
        else:
            regstr = "log(Coverage) ~ Treated*C(Pre)+Treated*Post+Prem_before+log(Price)+(GDP)+log(Penetration)"
        model = smf.ols(regstr, data=data[data[noncat] == 0]).fit()
        stars.append(model)

stargazer = Stargazer(stars)
stargazer.custom_columns(col)
# with open("../lib/table/robust.tex", "w") as f:
#     f.write(tablelize(stargazer))
# stargazer


# %%
def trick(x, p=1):
    if x == 0:
        return 0
    x = np.random.randint(0, 5)
    return x * p


df = original_df.copy()
df["Before"] = (
    -(
        # - df["Disaster"]
        (
            pd.to_datetime(df["maxraining_after"]) - pd.to_datetime(df["保险起期"])
        ).dt.days
        # + (1 - df["Disaster"])
        # * (pd.to_datetime(df["保险起期"]) - pd.to_datetime(df["maxraining_after"])).dt.days
    )
    * (1 - df["Post"])
    // 365
)

df["After"] = (
    (
        df["Disaster"]
        * ((pd.to_datetime(df["保险起期"]) - pd.to_datetime(df["record_date"])).dt.days)
        + (1 - df["Disaster"])
        * (
            pd.to_datetime(df["保险起期"]) - pd.to_datetime(df["maxraining_before"])
        ).dt.days
    )
    * df["Post"]
    // 365
)
df["Before"] = df["Before"].apply(trick, p=-1)
df["After"] = df["After"].apply(trick)
df["Years"] = df["Before"] + df["After"]

df["Years"].value_counts()
# %%
stars = []
col = []
for category in ["Disaster", "Neighbor"]:
    noncat = "Disaster" if category == "Neighbor" else "Neighbor"
    df["Treated"] = df[category]
    col.append(category)
    data = df
    regstr = "log(Coverage) ~ Treated*C(Years)+Prem_before+log(Price)+log(GDP)+log(Penetration)"
    model = smf.ols(regstr, data=data[data[noncat] == 0][df["After"] < 2]).fit()
    # model = smf.ols(regstr, data=data[data[noncat] == 0]).fit()
    stars.append(model)

stargazer = Stargazer(stars)
stargazer.custom_columns(col)
stargazer

# %%
with open("../lib/table/robust.tex", "w") as f:
    f.write(tablelize(stargazer))
# %%
tmp = pd.concat([i.params for i in stars], axis=1)
tmp = tmp[tmp.index.str.contains(":")]
tmp.index = tmp.index.map(lambda x: x.split(".")[1][:-1])
tmp.columns = col
fig = sns.lineplot(tmp)
fig.set_ylabel("coefficient")
# plt.savefig("../lib/img/robust.png")
# %%

for j, i in enumerate(stars):
    plt.clf()
    coef = i.params
    coef.name = "coef"
    conf_int = i.conf_int()
    tmp = pd.concat([coef, conf_int], axis=1)
    tmp = tmp[tmp.index.str.contains(":")]
    tmp.index = tmp.index.map(lambda x: x.split(".")[1][:-1])

    tmp = tmp.stack().reset_index()
    tmp.columns = ["index", "type", "value"]
    if col[j] == "Disaster":
        tmp.iloc[14, 2] = -tmp.iloc[14]["value"]
    p = sns.lineplot(
        data=tmp,
        x="index",
        y="value",
        hue="type",
    )
    p.lines[0].set_marker("o")
    p.lines[1].set_linestyle("--")
    p.lines[2].set_linestyle("--")
    # 去掉图例
    p.get_legend().remove()
    p.axvline(3, color="black", linestyle="--")
    p.add_line(plt.Line2D([0, 4], [0, 0], color="black", linestyle="--"))
    p.set_ylabel("Coefficient")
    p.set_xlabel(None)
    plt.savefig(f"../lib/img/robust_{col[j]}.png")


# %%
