import re

import pandas as pd

from io import StringIO 
from pathlib import Path

from prefect import flow, task
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket
from prefect_sqlalchemy import SqlAlchemyConnector

from typing import List, Optional


@task()
def prepare_env(workdir: str) -> Path:
    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


@task(log_prints=True)
def fetch_partition(partition_num: int, workdir: Path) -> pd.DataFrame:
    partition_local = str(workdir / f"part_{partition_num:05d}.parquet")
    partition_s3 = f"usage-stats/part_{partition_num:05d}.parquet"
    
    print(f"Processing partition:", partition_s3)

    AwsCredentials.load("yandex-cloud-s3-credentials")

    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.download_object_to_path(
        from_path=partition_s3,
        to_path=partition_local,
    )

    df = pd.read_parquet(partition_local)
    df["dwh_partition"] = partition_num

    return df


def create_table(table: str) -> str:
    return f'''
    CREATE TABLE IF NOT EXISTS default.{table}
    (
        rental_id               Int64,
        bike_id                 Int64,
        start_datetime          DateTime,
        start_station_id        Int64,
        start_station_name      String,
        end_datetime            DateTime,
        end_station_id          Int64,
        end_station_name        String,
        dwh_partition           Int64
    )
    ENGINE = MergeTree()
    PARTITION BY dwh_partition
    ORDER BY start_datetime
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
def etl_usagestats_to_ch(partition_num: int) -> None:
    workdir = prepare_env("workdir")
    upload_ch(
        fetch_partition(partition_num, workdir=workdir),
        table="usage_stats",
        partition_num=partition_num,
    )


@task(log_prints=True)
def list_partitions(latest: Optional[int] = None) -> List[int]:
    AwsCredentials.load("yandex-cloud-s3-credentials")
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    partitions = s3_block.list_objects("usage-stats")

    print("Partitions List:", partitions)

    partitions = [int(re.search(r"part_(\d+).parquet", p["Key"]).group(1)) for p in partitions]
    partitions = sorted(partitions)

    if latest is not None:
        partitions = partitions[-latest:]
    return partitions


@flow(log_prints=True)
def etl_usagestats_to_ch_multiple(partitions_num: Optional[List[int]] = None, latest: int = 50) -> None:
    if partitions_num is None:
        partitions_num = list_partitions(latest)

    for partition_num in partitions_num:
        etl_usagestats_to_ch(partition_num)


if __name__ == "__main__":
    etl_usagestats_to_ch_multiple(partitions_num=list(range(195, 314)))
