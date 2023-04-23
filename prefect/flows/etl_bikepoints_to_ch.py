import re
import json
import requests

import pandas as pd

from pathlib import Path

from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket

from typing import Dict


@task()
def prepare_env(workdir: str) -> Path:
    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


@task()
def fetch(workdir: Path) -> pd.DataFrame:
    AwsCredentials.load("yandex-cloud-s3-credentials")

    s3_path = "metainfo_bike_point.parquet"
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.download_object_to_path(from_path=s3_path, to_path=workdir / s3_path)

    return pd.read_parquet(workdir / s3_path)


def create_table(table: str) -> str:
    return f'''
    CREATE TABLE IF NOT EXISTS default.{table}
    (
        Id                Int64,
        Name              String,
        TerminalName      Int64,
        Lat               Float32,
        Lon               Float32,
        Installed         Bool,
        Locked            Bool,
        Temporary         Bool,
        NbBikes           Int8,
        NbEmptyDocks      Int8,
        NbDocks           Int8,
        NbStandardBikes   Int8,
        NbEBikes          Int8
    )
    ENGINE = MergeTree()
    ORDER BY Id
    PRIMARY KEY Id
    '''


def drop_table(table: str) -> str:
    return f'DROP TABLE IF EXISTS default.{table}'


@task()
def upload_ch(df: pd.DataFrame, table: str) -> None:
    with SqlAlchemyConnector.load("yandex-cloud-clickhouse-connector") as con:
        print("Connection:", con)
        print("Engine:", con.get_engine())
        
        sql_query = drop_table(table)
        con.execute(sql_query)

        sql_query = create_table(table)
        con.execute(sql_query)

        df.to_sql(
            name=table,
            con=con.get_engine(),
            chunksize=100_000,
            if_exists="append",
            index=False,
        )


@flow(log_prints=True)
def etl_bikepoints_to_ch():
    workdir = prepare_env("workdir")
    upload_ch(fetch(workdir), table="bike_point")


if __name__ == "__main__":
    etl_bikepoints_to_ch()
