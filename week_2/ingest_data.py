#!/usr/bin/env python
import os
import argparse
from time import time
from datetime import timedelta
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash

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
def ingest_data(user, password, host, port, db, table_name, df):
    
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(postgres_url)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


@task(log_prints=True)
def transform_data(df):
    print(f"passengers count before: {df['passenger_count'].isnull().sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"passengers count after: {df['passenger_count'].isnull().sum()}")
    return df
    
    
    
@flow(name="Ingest Flow")
def main():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    
    raw_data = extract_data(csv_url)
    tr_data = transform_data(raw_data)
    ingest_data(user, password, host, port, db, table_name, tr_data)
    
if __name__ == '__main__':
    main()