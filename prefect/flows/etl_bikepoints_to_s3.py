import re
import json
import requests
import pandas as pd

from pathlib import Path

from prefect import flow, task
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket

from typing import Dict


@task()
def prepare_env(workdir: str) -> Path:
    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


def create_dataframe(path: Path) -> pd.DataFrame:
    def make_record(record: Dict) -> Dict:
        row = {
            "Id":   int(record["id"].split("_")[-1]),
            "Name": re.sub(r"\s*,\s*", ", ", record["commonName"]),
            "Lat":  float(record["lat"]),
            "Lon":  float(record["lon"]),
        }

        properties = {item["key"]: item["value"] for item in record["additionalProperties"]}
        row.update(properties)

        return row

    content = json.load(path.open())
    df = pd.DataFrame([make_record(record) for record in content])

    df["Lat"] = df["Lat"].astype("float32")
    df["Lon"] = df["Lon"].astype("float32")

    df["TerminalName"] = df["TerminalName"].astype(int)

    df["Installed"] = df["Installed"] == "true"
    df["Locked"] = df["Locked"] == "true"
    df["Temporary"] = df["Temporary"] == "true"
    df["NbBikes"] = df["NbBikes"].astype("int8")
    df["NbEmptyDocks"] = df["NbEmptyDocks"].astype("int8")
    df["NbDocks"] = df["NbDocks"].astype("int8")
    df["NbStandardBikes"] = df["NbStandardBikes"].astype("int8")
    df["NbEBikes"] = df["NbEBikes"].astype("int8")

    df.drop(columns=["InstallDate", "RemovalDate"], inplace=True)

    print("Partition info:")
    print(df.head(5))
    print(f"cols:\n{df.dtypes}")
    print(f"rows: {df.shape[0]}")

    return df


@task(retries=3)
def fetch(path: str) -> Path:
    page = requests.get("https://api.tfl.gov.uk/BikePoint/")
    path = Path(path)
    json.dump(page.json(), path.open('w'), indent=4, separators=(', ', ': '), ensure_ascii=False)
    return path


@task()
def upload_s3(path_local: str, path_remote: str) -> None:
    AwsCredentials.load("yandex-cloud-s3-credentials")
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.upload_from_path(from_path=path_local, to_path=path_remote)


@flow(log_prints=True)
def etl_bikepoints_to_s3():
    workdir = prepare_env("workdir")

    path_json = "metainfo_bike_point.json"
    upload_s3(fetch(workdir / path_json), path_json)

    df = create_dataframe(workdir / path_json)
    
    path_parquet = "metainfo_bike_point.parquet"
    df.to_parquet(workdir / path_parquet, index=False, compression="gzip")
    upload_s3(workdir / path_parquet, path_parquet)


if __name__ == "__main__":
    etl_bikepoints_to_s3()
