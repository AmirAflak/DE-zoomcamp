#!/usr/bin/env python
import pandas as pd
from pathlib import Path
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download file from gcs and save it locally"""
    gcs_path = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_bucket = GcsBucket.load("rides-gcs")
    gcs_bucket.get_directory(from_path=gcs_path)
    
    return Path(f"{gcs_path}")





@flow()
def etl_gcs_to_bq():
    """ETL flow to get data from gcs and insert that into BigQuery""" 
    color = "yellow"
    year = 2021
    month = 1
    
    path = extract_from_gcs(color, year,  month)