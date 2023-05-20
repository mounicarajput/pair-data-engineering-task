import logging
import ast
import math
from os import environ
from time import sleep
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import numpy as np
from math import radians, cos, sin, acos


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

    # Extract latitude and longitude from the location column
    df['latitude'] = df['location'].apply(lambda loc: loc['latitude'])
    df['longitude'] = df['location'].apply(lambda loc: loc['longitude'])

    # Group the data by device_id and hour
    grouped = df.groupby([df['device_id'], df['time'].dt.hour])

    # Calculate the maximum temperatures per device per hour
    max_temperatures = grouped['temperature'].max()


    # Calculate the amount of data points aggregated per device per hour
    data_points_count = grouped.size()

    # Calculate the total distance of device movement per device per hour
    def calculate_distance(lat1, lon1, lat2, lon2):
        lat1_rad = radians(lat1)
        lon1_rad = radians(lon1)
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)

        distance = acos(sin(lat1_rad) * sin(lat2_rad) + cos(lat1_rad) * cos(lat2_rad) * cos(lon2_rad - lon1_rad)) * 6371
        return distance

    def calculate_hourly_distance(df):
        df['distance'] = np.nan
        df['latitude'] = df['latitude'].astype(float)  # Convert latitude to float
        df['longitude'] = df['longitude'].astype(float) # Convert longitude to float
        df['latitude_rad'] = np.radians(df['latitude'])
        df['longitude_rad'] = np.radians(df['longitude'])

        for device_id in df['device_id'].unique():
            device_df = df[df['device_id'] == device_id]
            device_df = device_df.sort_values('time')

            for i in range(1, len(device_df)):
                lat1 = device_df.iloc[i - 1]['latitude_rad']
                lon1 = device_df.iloc[i - 1]['longitude_rad']
                lat2 = device_df.iloc[i]['latitude_rad']
                lon2 = device_df.iloc[i]['longitude_rad']

                distance = calculate_distance(lat1, lon1, lat2, lon2)
                df.loc[device_df.index[i], 'distance'] = distance

        hourly_distance = df.groupby(['device_id', df['time'].dt.hour])['distance'].sum()
        return hourly_distance

    hourly_distance = calculate_hourly_distance(df)
    return pd.DataFrame({'max_temperatures': max_temperatures, 'data_points_count': data_points_count, 'total_distance': hourly_distance})

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