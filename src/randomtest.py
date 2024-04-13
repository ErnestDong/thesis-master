# %%
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import statsmodels.formula.api as smf

seed = np.random.Generator(np.random.PCG64(42))
log = np.log
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


def random_test(data=df, treated="Neighbor"):
    data = data.copy().reset_index(drop=True)
    nontreat = "Disaster" if treated == "Neighbor" else "Neighbor"
    data[treated] = (
        data[treated].sample(frac=1, random_state=seed).reset_index(drop=True)
    )
    data = data[data[nontreat] == 0]
    model = smf.ols(f"log(Coverage) ~ {treated}*Post", data=data)
    result = model.fit()
    return result


res = random_test(df, "Neighbor")
res.summary()


# %%
def tests(n=100, treated="Neighbor"):
    results = []
    for i in range(n):
        print(f"Test {treated} for {i}")
        res = random_test(df, treated)
        results.append(res.params)
    return pd.concat(results, axis=1).T


results = {}
for treat in ["Neighbor", "Disaster"]:
    data = tests(500, treat)
    results[treat] = data[[f"{treat}:Post", f"{treat}"]]

sns.kdeplot(pd.concat(results.values()))
plt.savefig("../lib/img/randomtest.png")
# %%
