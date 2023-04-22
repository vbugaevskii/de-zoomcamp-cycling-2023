import json
import requests

from pathlib import Path

from prefect import flow, task
from prefect_aws import AwsCredentials
from prefect_aws.s3 import S3Bucket


@task()
def prepare_env(workdir: str) -> Path:
    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


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
    path = "metainfo_bike_point.json"
    path_local = fetch(workdir / path)
    upload_s3(path_local, path)


if __name__ == "__main__":
    etl_bikepoints_to_s3()
