from os import environ
from time import sleep
import pandas as pd
import ast
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import math


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL  successful.')

# Write the solution here

# Read data from PostgreSQL into a pandas DataFrame
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
# Print the results
print("Maximum temperatures:")
print(max_temperatures)
print("\nData points count:")
print(data_points_count)
print("\nTotal distance of device movement:")
print(total_distance)



#my_conn=create_engine("mysql+pymysql://root:password@localhost/analytics")

while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MYSQL  successful.')

max_temperatures.to_sql(con=mysql_engine,name='max_temperatures',if_exists='append',index=False)
data_points_count.to_sql(con=mysql_engine,name='data_points_count',if_exists='append',index=False)
total_distance.to_sql(con=mysql_engine,name='total_distance',if_exists='append',index=False)

print("ETL Successful")
