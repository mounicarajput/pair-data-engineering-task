import logging
import ast
import math
from os import environ
from time import sleep
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_postgresql():
    """Connect to PostgreSQL and return the engine"""
    while True:
        try:
            psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
            return psql_engine
        except OperationalError:
            sleep(0.1)

# Write the solution here

def transform_data(psql_engine):
    """Transform the data from PostgreSQL"""
    pg_query = "SELECT * FROM devices"
    df = pd.read_sql_query(pg_query, psql_engine)

    # Convert the 'location' column from string to dictionary
    df['location'] = df['location'].apply(ast.literal_eval)

    # Convert the 'time' column to datetime
    df['time'] = pd.to_datetime(df['time'], unit='s')

    # Group the data by device_id and hour
    grouped = df.groupby([df['device_id'], df['time'].dt.hour])

    # Calculate the maximum temperatures per device per hour
    max_temperatures = grouped['temperature'].max()

    # Calculate the amount of data points aggregated per device per hour
    data_points_count = grouped.size()

    # Calculate the total distance of device movement per device per hour
    def calculate_distance(group):
        latitudes = group['location'].apply(lambda x: math.radians(float(x['latitude'])))
        longitudes = group['location'].apply(lambda x: math.radians(float(x['longitude'])))
        lat_diff = latitudes - latitudes.shift()
        lon_diff = longitudes - longitudes.shift()
        a = (lat_diff.apply(math.sin) * lat_diff.shift().apply(math.sin)) + \
            (lat_diff.apply(math.cos) * lat_diff.shift().apply(math.cos) * lon_diff.apply(math.cos))
        distance = a.apply(math.acos) * 6371  # Radius of the Earth in kilometers
        return distance.sum()

    total_distance = grouped.apply(calculate_distance)
    return pd.DataFrame({'max_temperatures': max_temperatures, 'data_points_count': data_points_count, 'total_distance': total_distance})

def connect_to_mysql():
    """Connect to MySQL and return the engine"""
    while True:
        try:
            mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
            return mysql_engine
        except OperationalError:
            sleep(0.1)
            print('Connection to MYSQL  successful.')

def write_to_mysql(mysql_engine, transformed_data):
    """Write the transformed data to MySQL"""
    transformed_data.to_sql(con=mysql_engine, name='transformed_data', if_exists='append', index=False)

def main():
    logger.info('Waiting for the data generator...')
    sleep(20)
    logger.info('ETL Starting...')

    # Connect to PostgreSQL
    psql_engine = connect_to_postgresql()
    logger.info('Connection to PostgreSQL successful.')

    # Transform the data
    transformed_data = transform_data(psql_engine)

    # Connect to MySQL
    mysql_engine = connect_to_mysql()
    logger.info('Connection to MySQL successful.')

    # Write the transformed data to MySQL
    write_to_mysql(mysql_engine,transformed_data)

    logger.info("ETL Successful")

if __name__ == '__main__':
    main()