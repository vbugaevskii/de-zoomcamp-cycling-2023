import re
import requests

import numpy as np
import pandas as pd
import lxml.html as lhtml

from io import StringIO
from pathlib import Path
from functools import reduce

from prefect import flow, task
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket

from typing import List, Optional


@task()
def prepare_env(workdir: str) -> Path:
    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


def get_partition_num(path: str) -> int:
    try:
        return int(re.search(r'^usage-stats/(\d+)', path).group(1))
    except AttributeError as e:
        print("Failed extract partition number for path:", path)
        return -1


@task(retries=2, log_prints=True)
def find_available_partitions(latest: Optional[int] = None) -> List[str]:
    page = requests.get("https://s3-eu-west-1.amazonaws.com/cycling.data.tfl.gov.uk/?list-type=2")
    tree = lhtml.fromstring(page.content)

    partitions = tree.xpath('//key[starts-with(text(), "usage-stats")]/text()')
    partitions = [p for p in partitions if p.endswith('.csv') and 'JourneyDataExtract' in p]
    partitions = [p for p in partitions if re.search(r'\d+JourneyDataExtract(?:\d{2}\w+\d{4})-(?:\d{2}\w+\d{4})', p)]
    partitions = sorted(partitions, key=get_partition_num)

    if latest is not None:
        partitions = partitions[-latest:]

    return partitions


@task(retries=2, log_prints=True)
def fetch(partition_url: str) -> pd.DataFrame:
    print(f"partition_url={partition_url}")

    headers = (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)'
        'Chrome/110.0.0.0 YaBrowser/23.3.0.2318 Yowser/2.5 Safari/537.36'
    )
    headers = {'user-agent': headers}

    content = requests.get(partition_url, headers=headers).text
    content = StringIO(content)

    df = pd.read_csv(content)
    print("columns_raw = ", df.columns)

    df.columns = df.columns.str.lower().str.replace(' ', '')

    df.rename(columns={
        "number":             "rental_id",
        "rentalid":           "rental_id",
        "bikenumber":         "bike_id",
        "bikeid":             "bike_id",
        "enddate":            "end_datetime",
        "endstationid":       "end_station_id",
        "endstationnumber":   "end_station_id",
        "endstationname":     "end_station_name",
        "endstation":         "end_station_name",
        "startdate":          "start_datetime",
        "startstationnumber": "start_station_id",
        "startstationid":     "start_station_id",
        "startstationname":   "start_station_name",
        "startstation":       "start_station_name",
    }, inplace=True)

    # HOTFIX: because column names and number are incosistent
    # if "end_station_id" not in df.columns:
    #     df["end_station_id"] = -1

    columns = [
        "rental_id",
        "bike_id",
        "start_datetime",
        "start_station_id",
        "start_station_name",
        "end_datetime",
        "end_station_id",
        "end_station_name",
    ]
    df = df[columns]

    for col in ["start_datetime", "end_datetime"]:
        df[col] = pd.to_datetime(df[col])

    for col in ["start_station_name", "end_station_name"]:
        df[col] = df[col].map(lambda s: re.sub(r"\s*,\s*", ", ", s))

    # NOTE: Drop some incosistent IDs

    mask = []
    for col in ["rental_id", "bike_id", "start_station_id", "end_station_id"]:
        mask_ = pd.to_numeric(df[col], errors="coerce").notnull()
        mask.append(mask_)
    mask = reduce(np.logical_and, mask)

    df = df[mask]

    for col in ["rental_id", "bike_id", "start_station_id", "end_station_id"]:
        df[col] = df[col].astype(int)

    print("Drop rows with bad IDs:", (~mask).sum())

    print("Partition info:")
    print(df.head(2))
    print(f"cols:\n{df.dtypes}")
    print(f"rows: {df.shape[0]}")

    return df


@task()
def save_partition(df: pd.DataFrame, path: Path) -> Path:
    df.to_parquet(path, index=False, compression="gzip")
    return path


@task()
def upload_s3(path_local: str, path_remote: str) -> None:
    AwsCredentials.load("yandex-cloud-s3-credentials")
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.upload_from_path(from_path=path_local, to_path=path_remote)


@flow(log_prints=True)
def process_partition(partition_path: str, workdir: Path) -> None:
    partition_num = get_partition_num(partition_path)
    partition_url = "https://cycling.data.tfl.gov.uk/" + partition_path

    df_partition = fetch(partition_url)
    partition_path = Path(f"part_{partition_num:05d}.parquet")
    partition_local = save_partition(df_partition, workdir / partition_path)
    partition_path = Path("usage-stats") / partition_path

    upload_s3(partition_local, partition_path)


@flow(log_prints=True)
def etl_usagestats_to_s3(partition_num: Optional[int] = None) -> None:
    workdir = prepare_env("workdir")

    partitions = find_available_partitions()
    
    if partition_num is not None:
        partitions_dict = {get_partition_num(p): p for p in partitions}
        partition = partitions_dict.get(partition_num)
    else:
        partition = partitions[-1]

    if partition is None:
        raise KeyError("Partition is not available", partition_num)
    
    process_partition(partition, workdir=workdir)


@flow(log_prints=True)
def etl_usagestats_to_s3_multiple(partitions_num: Optional[List[int]] = None, latest: int = 50) -> None:
    workdir = prepare_env("workdir")
    
    if partitions_num is not None:
        for partition_num in partitions_num:
            etl_usagestats_to_s3(partition_num)
        return

    partitions = find_available_partitions(latest)
    for partition in partitions:
        process_partition(partition, workdir=workdir)


if __name__ == "__main__":
    etl_usagestats_to_s3_multiple(latest=50)
