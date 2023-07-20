#!/usr/bin/env python
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket

@task()
fetch(url: str) -> pd.DataFrame:
    """Read taxi data from a web source and pass dataFrame"""
    return pd.read_csv(url)
    
@flow()
def etl_web_to_gcs() -> None:
    """The main function for ETL"""
    color = "yellow"
    year = 2020
    month = 2
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)


if __name__ == '__main__':
    etl_web_to_gcs()