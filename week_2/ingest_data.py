#!/usr/bin/env python
import os
import argparse
import pandas as pd
from time import time
from datetime import timedelta
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    return df
    

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
    database_block = SqlAlchemyConnector.load("postgres")
    with database_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')


@task(log_prints=True)
def transform_data(df):
    print(f"passengers count before: {df['passenger_count'].isnull().sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"passengers count after: {df['passenger_count'].isnull().sum()}")
    return df

@flow(name="Sub Flow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Sub Flow Logging for {table_name}")
    
    
@flow(name="Ingest Flow")
def main(table_name: str):
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    tr_data = transform_data(raw_data)
    ingest_data(table_name, tr_data)
    
if __name__ == '__main__':
    main("yellow_taxi_trips")