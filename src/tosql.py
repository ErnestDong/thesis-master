# %%
import glob
import logging

import pandas as pd
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
engine = create_engine("postgresql+psycopg://dcy@localhost:5432/thesis")


def rainings(engine):
    rainings = glob.glob("/Users/dcy/Downloads/毕业论文/降水/降水/*.xlsx")
    for i, raining in enumerate(rainings):
        df = pd.read_excel(raining)
        logging.info(f"{i}/{len(rainings)} read {raining}")
        df.to_sql("raining", engine, if_exists="append", index=False)
        logging.info(f"{i}/{len(rainings)}write {raining} to sql")


# rainings(engine)


def location(engine):
    df = pd.read_excel(
        "/Users/dcy/Downloads/毕业论文/降水/SURF_CLI_CHN_MUL_DAY_STATION（v3.0新）.xls"
    )
    df.to_sql("location", engine, if_exists="append", index=False)


# location(engine)


def sh(engine):
    f = glob.glob("/Users/dcy/Downloads/毕业论文/台风巨灾_所有数据原版/sh/*.csv")
    for i in f:
        if "base" not in i:
            continue
        print(i)
        df = pd.read_csv(
            i,
            encoding="GBK",
            encoding_errors="ignore",
            dtype={
                "上年保单号": str,
                "是否涉农业务": str,
                "标的邮编": str,
                "建筑类型": str,
                "建筑用途": str,
                "房屋结构": str,
                "保险费率分解": str,
            },
        )
        # 8,13,14,16,17,25
        df.to_sql(
            i.split("_")[-1].split(".")[0], engine, if_exists="replace", index=False
        )


# sh(engine)


def base(engine):
    file = "/Users/dcy/Downloads/毕业论文/台风巨灾_所有数据原版/sh/output_gis_cyclone_sh_base.csv"
    with open(file, "r", encoding="gb2312") as f:
        content = f.readline()
        while True:
            try:
                content = f.readline()
                print(content)
            except UnicodeDecodeError:
                pass
            if not content:
                break


# %%
df = pd.read_csv(
    "/Users/dcy/Desktop/thesis/data/台风巨灾_所有数据原版/claim/个财险公共数据.csv",
    encoding="GBK",
)
# %%
from clickhouse_driver import Client

client = Client("localhost")
client.insert_dataframe(
    "INSERT INTO thesis.claim VALUES",
    df,
    settings=dict(use_numpy=True),
)
# df.to_sql("claim", engine, if_exists="replace", index=False)
# %%
df = pd.read_csv(
    "/Users/dcy/Desktop/thesis/data/台风巨灾_所有数据原版/claim/企财个财险历史物损.csv",
    encoding="GBK",
)

# %%
df.to_sql("history_claim", engine, if_exists="replace", index=False)

# %%

df.to
