# This file performs inserting a custom csv file into database

import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table = params.table_name



    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    engine.connect()


    df = pd.read_csv(csv_name, nrows=10)
    df

    df_iter = pd.read_csv(csv_name,  iterator=True, chunksize=100000)
    df_iter


    # create table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')


    # insert records to table
    while True:
        t_start = time()
        
        df = next(df_iter)
        
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        t_end = time()
        
        print(f'inserted another chunk.... which took {t_end - t_start}')
        


if __name__ == '__main__':


    parser = argparse.ArgumentParser(description='Ingest csv data to postgresql')
    parser.add_argument('--user', help='postgres username')
    parser.add_argument('--password', help='postgres password')
    parser.add_argument('--host', help='postgres hostname')
    parser.add_argument('--db', help='postgres database name')
    parser.add_argument('--port', help='postgres port')
    parser.add_argument('--table_name', help='specifies which table to write the results')
    parser.add_argument('--url', help='csv url')

    args = parser.parse_args()

    main(args)



