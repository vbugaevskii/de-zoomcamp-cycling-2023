import re
import requests

import numpy as np
import pandas as pd

from pathlib import Path
from urllib.parse import urlparse

from tqdm.auto import tqdm

from prefect import flow, task
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket
from prefect_sqlalchemy import SqlAlchemyConnector

from typing import List, Tuple, Optional


@task()
def prepare_env(workdir: str) -> Path:
    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


@task(log_prints=True)
def fetch_partitions(partition_num: int, workdir: Path) -> pd.DataFrame:
    AwsCredentials.load("yandex-cloud-s3-credentials")
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")

    df_joined = []

    for metric in ['tasmin', 'tasmax', 'rainfall']:
        (workdir / f"weather/{metric}").mkdir(parents=True, exist_ok=True)

        partition_s3 = f"weather/{metric}/part_{partition_num}.parquet"
        partition_local = workdir / partition_s3
    
        print(f"Processing partition:", partition_s3)
        s3_block.download_object_to_path(
            from_path=partition_s3,
            to_path=partition_local,
        )

        df = pd.read_parquet(partition_local)
        df.set_index(["station_id", "date"], inplace=True)
        df_joined.append(df)
    
    df_joined = pd.concat(df_joined, axis=1)
    df_joined.reset_index(inplace=True)

    print("df_joined.shape =", df_joined.shape)
    print("df_joined.head():\n", df_joined.head())

    return df_joined


def create_table(table: str) -> str:
    return f'''
    CREATE TABLE IF NOT EXISTS default.{table}
    (
        station_id              Int64,
        date                    DateTime,
        tasmin                  Float32,
        tasmax                  Float32,
        rainfall                Float32
    )
    ENGINE = MergeTree()
    PARTITION BY toYYYYMM(date)
    ORDER BY (date, station_id)
    SETTINGS index_granularity = 8192
    '''


def drop_partition_table(table: str, partition_num: int) -> str:
    return f'ALTER TABLE default.{table} DROP PARTITION {partition_num}'


@task(retries=2)
def upload_ch(df: pd.DataFrame, table: str, partition_num: int) -> None:
    with SqlAlchemyConnector.load("yandex-cloud-clickhouse-connector") as con:
        print("Connection:", con)
        print("Engine:", con.get_engine())
        
        sql_query = create_table(table)
        con.execute(sql_query)

        sql_query = drop_partition_table(table, partition_num=partition_num)
        con.execute(sql_query)

        df.to_sql(
            name=table,
            con=con.get_engine(),
            chunksize=100_000,
            if_exists="append",
            index=False,
        )


@flow(log_prints=True)
def etl_weather_to_ch(partition_num: int) -> None:
    workdir = prepare_env("workdir")
    upload_ch(
        fetch_partitions(partition_num, workdir=workdir),
        table="weather",
        partition_num=partition_num,
    )


@flow(log_prints=True)
def etl_weather_to_ch_multiple(partitions_num: List[int]) -> None:
    for partition_num in partitions_num:
        etl_weather_to_ch(partition_num)


if __name__ == "__main__":
    etl_weather_to_ch_multiple(
        partitions_num=list(range(202001, 202013)) 
    )