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
from prefect.blocks.system import Secret

from typing import List, Tuple, Optional


def progress_download(url: str, path: Path, chunk_size: int = 1024, **kwargs) -> Path:
    page = requests.get(url, stream=True, **kwargs)
    size = int(page.headers.get('content-length', 0))

    with path.open('wb') as fd, tqdm(
        desc=str(path),
        total=size,
        unit='iB',
        unit_scale=True,
        unit_divisor=chunk_size,
    ) as pbar:
        for data in page.iter_content(chunk_size=chunk_size):
            size = fd.write(data)
            pbar.update(size)

    return path


@task()
def prepare_env(workdir: str) -> Path:
    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True)
    return workdir


def get_partition_name(path: str) -> Optional[Tuple[str, int]]:
    try:
        match = re.search(r'/(\w+)_hadukgrid_uk_(?:\d+km)_day_(\d{8})-(\d{8}).nc', path)
        return (match.group(1), int(match.group(2)[:-2]), )
    except AttributeError as e:
        print("Failed extract partition number for path:", path)
        return -1


@task(retries=2, log_prints=True)
def find_available_partitions(metric: str) -> List[str]:
    secret_block = Secret.load("ceda-archive-secret")

    page = requests.get(
        f'https://data.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.1.0.0/5km/{metric}/day/v20220310?json',
        cookies={"ceda.session.1": secret_block.get()},
    )

    return [p['download'] for p in page.json()['items']]


@task(retries=2, log_prints=True)
def fetch(partition_url: str, workdir: Path) -> pd.DataFrame:
    import netCDF4 as nc

    print(f"partition_url={partition_url}")

    _, partition_name = urlparse(partition_url).path.rsplit('/', 1)
    metric_name, metric_date = get_partition_name(partition_url)

    secret_block = Secret.load("ceda-archive-secret")
    partition_local = progress_download(
        partition_url,
        workdir / metric_name / partition_name,
        cookies={"ceda.session.1": secret_block.get()},
    )

    try:
        nc_data = nc.Dataset(partition_local)
        nc_data_mask = ~nc_data[metric_name][:].mask.all(axis=0)
        
        df = pd.DataFrame({
            'Lat': nc_data['latitude'][:][nc_data_mask],
            'Lon': nc_data['longitude'][:][nc_data_mask],
            metric_name: nc_data[metric_name][:][:, nc_data_mask].T.tolist(),
        })
    finally:
        nc_data.close()

    return df


@task()
def save_partition(df: pd.DataFrame, path: Path) -> Path:
    df.to_parquet(path, index=False, compression="gzip")
    return path


@task()
def fetch_bikepoints(workdir: Path) -> pd.DataFrame:
    AwsCredentials.load("yandex-cloud-s3-credentials")

    s3_path = "metainfo_bike_point.parquet"
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.download_object_to_path(from_path=s3_path, to_path=workdir / s3_path)

    return pd.read_parquet(workdir / s3_path)


@task(log_prints=True)
def match_weather_to_bikepoints(df_weather, df_bikepoints, partition_name):
    from sklearn.neighbors import NearestNeighbors

    metric_name, metric_date = partition_name
    
    print("Running kNN algroithm")
    nn = NearestNeighbors(n_neighbors=1, metric='haversine')
    nn.fit(df_weather[['Lat', 'Lon']])
    _, indices = nn.kneighbors(df_bikepoints[['Lat', 'Lon']], return_distance=True)

    print("Make joined algorithm")
    df_joined = pd.DataFrame({
        'station_id': df_bikepoints['TerminalName'].values,
        metric_name: df_weather[metric_name].iloc[indices.ravel()],
    })

    num_days = len(df_joined[metric_name].iloc[0])
    num_points = df_joined.shape[0]
    print("num_days =", num_days)
    print("num_points =", num_points)
    
    df_joined = df_joined.explode(metric_name)
    df_joined['date'] = metric_date * 100 + np.tile(1 + np.arange(num_days), num_points)
    df_joined['date'] = pd.to_datetime(df_joined['date'].map(str), format='%Y%m%d')

    print("df_joined.shape =", df_joined.shape)

    return df_joined


@task()
def upload_s3(path_local: str, path_remote: str) -> None:
    AwsCredentials.load("yandex-cloud-s3-credentials")
    s3_block = S3Bucket.load("yandex-cloud-s3-bucket")
    s3_block.upload_from_path(from_path=path_local, to_path=path_remote)


@flow(log_prints=True)
def process_partition(partition_url, df_bikepoints, workdir) -> None:
    metric_name, metric_date = get_partition_name(partition_url)
    
    df_partition = fetch(partition_url, workdir=workdir)
    df_partition = match_weather_to_bikepoints(
        df_partition,
        df_bikepoints,
        partition_name=(metric_name, metric_date),
    )

    partition_path = Path(f"{metric_name}/part_{metric_date}.parquet")
    partition_local = save_partition(df_partition, workdir / partition_path)
    upload_s3(partition_local, "weather" / partition_path)


@flow(log_prints=True)
def etl_weather_to_s3(partition_num: int, metric: str) -> None:
    workdir = prepare_env("workdir")

    partitions = find_available_partitions(metric)
    partitions = {get_partition_name(p): p for p in partitions}

    partition_url = partitions.get((metric, partition_num))
    if partition_url is None:
        print(f"Partition `{partition_num}` for `{metric}` not found")
        return

    (workdir / metric).mkdir(parents=True, exist_ok=True)

    df_bikepoints = fetch_bikepoints(workdir)
    process_partition(partition_url, df_bikepoints, workdir)


@flow(log_prints=True)
def etl_weather_to_s3_multiple(
    partitions_num: List[int] = None,
    metrics: List[str] = ['tasmin', 'tasmax', 'rainfall'],
) -> None:
    workdir = prepare_env("workdir")

    df_bikepoints = fetch_bikepoints(workdir)

    for metric in metrics:
        (workdir / metric).mkdir(parents=True, exist_ok=True)

        partitions = find_available_partitions(metric)
        partitions = {get_partition_name(p): p for p in partitions}

        if not partitions_num:
            partitions_num = [max(partitions.keys())[1], ]
        
        for partition_num in partitions_num:
            partition_url = partitions.get((metric, partition_num))
            process_partition(partition_url, df_bikepoints, workdir)


if __name__ == "__main__":
    etl_weather_to_s3_multiple(
        partitions_num=list(range(202101, 202113)) 
    )