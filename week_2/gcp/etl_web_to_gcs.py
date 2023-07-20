#!/usr/bin/env python
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    return pd.read_csv(url)
 
@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Doing some transformations on passed data"""
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)       
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    
    return df
    
@flow(log_prints=False)
def etl_web_to_gcs() -> None:
    """The main function for ETL"""
    
    color = "yellow"
    year = 2020
    month = 2
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    tr_df = transform(df)


if __name__ == '__main__':
    etl_web_to_gcs()