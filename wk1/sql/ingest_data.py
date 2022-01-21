import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    # Call parameters 
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    # The name of output data
    csv_name = 'output.csv'

    # Download the data
    os.system(f"wget {url} -O {csv_name}")

    # Download taxi zone data (csv) and rename it to taxi_zone.csv
    os.system(f"wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv -O taxi_zone.csv")

    # set up SQLAlchemy engine 
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    zone_data = pd.read_csv('taxi_zone.csv')
    zone_data.to_sql(name='zone_data', con=engine, if_exists='replace', index=False)
    engine.execute("ALTER TABLE zone_data ADD PRIMARY KEY (\"LocationID\")")

    taxi_data_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    count = 1
    try:
        while True:
            # Record the starting time
            t_start = time()
            # Read 100,000 records from the dataset 
            taxi_data = next(taxi_data_iter)
            # If the table 'yellow_taxi_data' exists, replace the table with a new table
            if count == 1:
                taxi_data.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
            # Change the datatype of the pickup and dropoff datetime columns from string to datetime 
            taxi_data.tpep_pickup_datetime = pd.to_datetime(taxi_data.tpep_pickup_datetime)
            taxi_data.tpep_dropoff_datetime = pd.to_datetime(taxi_data.tpep_dropoff_datetime)
            # Insert the data to the database
            taxi_data.to_sql(name=table_name, con=engine, if_exists='append')
            # Record the ending time 
            t_end = time()
            count += 1
            # Calculate and print the runtime
            print(f'inserted another {100_000} chunk..., took {(t_end-t_start):.3f} seconds.')
    except StopIteration: # When the iterator reaches the end... 
        record_num = engine.execute(f'SELECT COUNT(*) FROM {table_name}').fetchall()[0][0]
        print(f"Finished inserting. Total {record_num} records.")


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where the data will be written to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)