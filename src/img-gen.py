# %%
import pathlib

import pandas as pd
import seaborn as sns

path = pathlib.Path("/Users/dcy/Desktop/thesis/")
sns.set_style("white", {"font.sans-serif": ["STHeiti"]})

df = pd.DataFrame(
    {
        "": list(range(2013, 2023)),
        "家庭财产保险保费(亿元)": [38, 34, 42, 52, 63, 77, 91, 91, 98, 164],
    }
)
fig = sns.barplot(x="", y="家庭财产保险保费(亿元)", data=df).get_figure()
fig.savefig(path / "img" / "家庭财产保险保费.png")
# %%
df = pd.read_parquet("../data/olsups.parquet")

# %%
df["保险起期"] = pd.to_datetime(df["保险起期"])
fig = sns.histplot(df["保险起期"], kde=True)
fig.set_ylabel("保单数量")
fig
# %%
