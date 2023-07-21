#!/usr/bin/env python
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse
import os



@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download file from gcs and save it locally"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_bucket = GcsBucket.load("rides-gcs")
    gcs_bucket.download_object_to_path(from_path=gcs_path)
    
    return Path(f"{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Convert to dataFrame and data cleaning"""
    return pd.read_parquet(path)

@task()
def to_bq(df: pd.DataFrame) -> None:
    """Write dataFrame to big query"""
    load_dotenv()
    gcp_credentials_block = GcpCredentials.load("rides-gcs-creds")
    
    dataframe.to_gbq(
                destination_table=os.getenv('BQ_TABLE_NAME'), 
                project_id=os.getenv('BQ_PROJECT_ID'),
                credentials=gcp_credentials_block.get_credentials_from_service_account(),
                chunksize=500_000, 
                if_exists='append') 
    return
    

    

@flow(log_prints=True)
def etl_gcs_to_bq():
    """ETL flow to get data from gcs and insert that into BigQuery""" 
    color = "yellow"
    year = 2020
    month = 2 
    
    path = extract_from_gcs(color, year,  month)
    # transformed data
    tr_data = transform(path)
    # write to big query
    to_bq(tr_data)
    
if __name__ == '__main__':
    etl_gcs_to_bq()