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
premdf = df.copy()
premdf = premdf[premdf["Premium"] > 0]
premdf["Post"] = -premdf["Post"]
premdf["Disaster"] = premdf["Disaster"]
premdf["Neighbor"] = -premdf["Neighbor"]
betweens = premdf["Premium"].quantile([0.05, 0.95])
premdf = premdf[(premdf["Premium"].between(betweens[0.05], betweens[0.95]))].copy()
model1 = smf.ols(
    "log(Premium) ~ Neighbor*Post+Prem_before+Price+Area",
    # "log(Premium) ~ Neighbor*Post",
    data=premdf[(premdf["Disaster"] == 0)],
).fit()
model1.summary()

# %%
near = df[df["distance"] < 15]
model21 = smf.ols(
    "log(Coverage) ~ Neighbor*Post+Prem_before+Price+Area",
    data=near[(near["Disaster"] == 0)],
).fit()

model22 = smf.ols(
    "log(Coverage) ~ Disaster*Post+Prem_before+Price+Area",
    data=near[(near["Neighbor"] == 0)],
).fit()
# %%

model2 = smf.ols(
    "log(Premium) ~ Disaster*Post+Prem_before+Price+Area",
    # "log(Premium) ~Disaster*Post",
    data=premdf[(premdf["Neighbor"] == 0)],
).fit()
model2.summary()


# %%

original_df = pd.read_sql("ols_ups_far", engine)
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
# %%
model11 = smf.ols(
    "log(Coverage) ~ Neighbor*Post+Prem_before+Price+Area",
    data=df[(df["Disaster"] == 0)],
).fit()

model12 = smf.ols(
    "log(Coverage) ~ Disaster*Post+Prem_before+Price+Area",
    data=df[(df["Neighbor"] == 0)],
).fit()


# %%

star = Stargazer([model1, model2, model11, model12, model21, model22])
star.custom_columns(
    ["log(Premium)", "log(Premium)", "<30km", "<30km", "<10km", "<10km"]
)
with open("../lib/table/premiumdid.tex", "w") as f:
    f.write(tablelize(star))
star
