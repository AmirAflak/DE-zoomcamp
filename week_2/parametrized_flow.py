#!/usr/bin/env python
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    
    return pd.read_csv(url)
 
@task(log_prints=True)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Doing some transformations on passed data"""
    
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)       
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    
    return df
    
@task(log_prints=True)
def to_parquet_and_save(df: pd.DataFrame, dataset_file: str) -> Path:
    """Save dataframe as parquet format and return it's path"""
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    
    return path

@task(log_prints=True)
def load_to_gcs(path: Path) -> None:
    """Upload dataset into gcs"""
    
    gcs_bucket = GcsBucket.load("rides-gcs")
    gcs_bucket.upload_from_path(from_path=path,
                                to_path=path)
    return

@flow(log_prints=False)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main function for ETL"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        
    df = fetch(dataset_url)
    tr_df = transform(df)
    df_parquet_path = to_parquet_and_save(tr_df, dataset_file)
    load_to_gcs(df_parquet_path)
    
@flow(log_prints=True)
def etl_parent_flow(months: list[int] = [2, 3],
                    year: int = 2021,
                    color: str = 'yellow'):
    for month in months:
        etl_web_to_gcs(year, month, color)
    

if __name__ == '__main__':
    etl_parent_flow(months=[3, 6, 7], year=2020, color='green')