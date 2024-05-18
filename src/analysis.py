# %%
import pandas as pd
import seaborn as sns
from numpy import log
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.formula.api as smf
from stargazer.stargazer import Stargazer
import matplotlib.pyplot as plt

import re


def tablelize(star):
    string = (
        star.render_latex()
        .replace("\\begin{table}[!htbp] \\centering", "")
        .replace("\\end{table}", "")
        .replace("_", "\\_")
    )
    return re.sub(r"\(df=[\d; ]*\)", "", string)


sns.set_theme(style="whitegrid")
df = pd.read_parquet("../data/df1.parquet")
df.head()
# %%
df["claimed"] = df["total_claim"].map(lambda x: 1 if x > 0 else 0)
claims = smf.logit(
    "claimed ~ Disaster*Post+Prem_before+log(Price)+log(Penetration)+log(GDP)+C(ti)",
    data=df[(df["Neighbor"] == 0)],
).fit()

claims.summary()
# %%
treated = df[df["Neighbor"] == 0].copy()
treated["Post"] = -treated["Post"]
treated["renew"] = treated["下年保单号"].map(lambda x: 1 if x else 0)
renew = smf.logit(
    "renew ~ Disaster*Post+Prem_before+log(Price)+log(Penetration)+log(GDP)+C(ti)",
    data=treated,
).fit()
renew.summary()


# %%
# firstinsured = df[df["Disaster"] == 1].copy()
firstinsured = df.copy()
firstinsured["First"] = firstinsured["上年保单号"].map(lambda x: 0 if x else 1)
first = smf.ols(
    "log(Coverage) ~ First*Post+log(Price)+log(Penetration)+log(GDP)+C(ti)",
    data=firstinsured[firstinsured["Disaster"] == 1],
).fit()
# first = smf.logit(
#     "First ~ Disaster*Post+log(Price)+log(Penetration)+log(GDP)+C(ti)",
#     data=firstinsured[firstinsured["Neighbor"] == 0],
# ).fit(maxiter=1000)
first.summary()
# %%
stars = Stargazer([claims, renew, first])
stars
# %%
with open("../lib/table/renew.tex", "w") as f:
    f.write(tablelize(stars))

# %%
