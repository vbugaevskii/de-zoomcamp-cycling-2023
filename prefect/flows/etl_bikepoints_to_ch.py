import re
import requests

import pandas as pd

from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector


def make_record(record: dict) -> dict:
    row = {
        "Id":   int(record["id"].split("_")[-1]),
        "Name": re.sub(r"\s*,\s*", ", ", record["commonName"]),
        "Lat":  float(record["lat"]),
        "Lon":  float(record["lon"]),
    }

    properties = {item["key"]: item["value"] for item in record["additionalProperties"]}
    row.update(properties)

    return row


@task(retries=3)
def fetch() -> pd.DataFrame:
    page = requests.get("https://api.tfl.gov.uk/BikePoint/")

    df = pd.DataFrame([make_record(record) for record in page.json()])

    df["Lat"] = df["Lat"].astype("float32")
    df["Lon"] = df["Lon"].astype("float32")

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


def create_table(table):
    return f'''
    CREATE TABLE IF NOT EXISTS default.{table}
    (
        Id                Int64,
        Name              String,
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
    ENGINE = Dictionary({table})
    '''


@task()
def upload_ch(df: pd.DataFrame, table: str) -> None:
    with SqlAlchemyConnector.load("yandex-cloud-clickhouse-connector") as con:
        print("Connection:", con)
        print("Engine:", con.get_engine())
        
        sql_query = create_table(table)
        con.execute(sql_query)

        df.to_sql(
            name=table,
            con=con.get_engine(),
            chunksize=100_000,
            if_exists="replace",
            index=False,
        )


@flow(log_prints=True)
def etl_bikepoints_to_ch():
    upload_ch(fetch(), table="bike_point")


if __name__ == "__main__":
    etl_bikepoints_to_ch()
