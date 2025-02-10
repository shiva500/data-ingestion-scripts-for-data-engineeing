import os
import argparse
import pandas as pd
from sqlalchemy import create_engine
import pyarrow.parquet as pq
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    if url.endswith('.parquet.gz'):
        input_parquet = 'output.parquet.gz'
    else:
        input_parquet = 'output.parquet'
    
    #download the file
    os.system(f"wget {url} -O {input_parquet}")
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_parquet(input_parquet, engine='pyarrow')
    
    #test the connection    
    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    chunk_size = 100000
    #len(df_iter) // chunk_size + (1 if len(df_iter) % chunk_size != 0 else 0)
    num_of_chunks = len(df) // chunk_size + ( 1 if len(df) % chunk_size != 0 else 0)

    for i in range(num_of_chunks):
        t_start = time()
        df_chunk = df.iloc[i * chunk_size: (i + 1) * chunk_size]
        df_chunk.to_sql(name = table_name, con = engine, if_exists = 'append', index = False)
        t_end = time()
        print(f"Inserted Chunk {i+1}/{num_of_chunks} into {table_name}, took {t_end - t_start:.3f} seconds")

    print("Finished ingesting Parquet data into the PostgreSQL database")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest Parquet data into PostgreSQL')
    parser.add_argument('--url', required=True, help='URL of the Parquet file')
    parser.add_argument('--user', required=True, help='PostgreSQL username')
    parser.add_argument('--password', required=True, help='PostgreSQL password')
    parser.add_argument('--host', required=True, help='PostgreSQL host')
    parser.add_argument('--port', required=True, help='PostgreSQL port')
    parser.add_argument('--db', required=True, help='PostgreSQL database name')
    parser.add_argument('--table_name', required=True, help='PostgreSQL table name')
    
    args = parser.parse_args()
    main(args)



