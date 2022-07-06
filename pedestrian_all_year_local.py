#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 18:18:24 2022

@author: hemantsingh
"""
import pandas as pd
#from sodapy import Socrata
import boto3
from botocore.exceptions import ClientError
from io import StringIO, BytesIO
import pyarrow
import fastparquet
import json
import logging
logger = logging.getLogger(name=None)
logger.setLevel(logging.INFO)


logger.info("Getting configuration data for S3 bucket and local source file path/name")

try:
    file = open('config.json', 'r')
    config_data = json.loads(file.read())
except IOError as error:
    logger.error("I/O Error({0}) occurred while opening {1} file : {2} ".format(error.errno, error.filename, error.strerror))
else:
    file.close()

logger.info("Generating complete hourly count source file path/name")

src_hourly_cnt_file = config_data['local_path'] + config_data['hourly_sensor']

logger.info("Loading DataFrame from the hourly count source file")

hourly_df = pd.read_csv(src_hourly_cnt_file)
hourly_df.columns = hourly_df.columns.str.upper()
hourly_df["HOURLY_COUNTS"] = pd.to_numeric(hourly_df["HOURLY_COUNTS"])
#print(hourly_df.head(5))

#DAY stats over all years
logger.info("Building a new DataFrame for Day-wise aggregation")
logger.info("Aggregating Data to get the total of hourly pedestrian counts day-wise at each location/sensor")

day_df = hourly_df.groupby(['SENSOR_ID','DAY'])['HOURLY_COUNTS'].agg('sum').reset_index().rename(columns={'HOURLY_COUNTS':'TOTAL_DAY_COUNT'})

day_df["TOTAL_DAY_COUNT"] = pd.to_numeric(day_df["TOTAL_DAY_COUNT"])
#day_df["SENSOR_ID"] = pd.to_numeric(day_df["SENSOR_ID"])
logger.info("Adding new field in the DataFrame 'Rank' to rank the location/sensor based on total pedestrian counts")

day_df['RANK'] = day_df.groupby(['DAY'])['TOTAL_DAY_COUNT'].rank(method="first", ascending=False)

logger.info("Filter data in the DataFrame to keep only TOP 10 location/sensor for each day from beginning of records")

day_df = day_df.loc[day_df['RANK'] <= 10]

logger.info("Building a new DataFrame for Month-wise aggregation")
logger.info("Aggregating Data to get the total of hourly pedestrian counts Month-wise at each location/sensor")

monthly_df = hourly_df.groupby(['SENSOR_ID','MONTH'])['HOURLY_COUNTS'].agg('sum').reset_index().rename(columns={'HOURLY_COUNTS':'MONTHLY_CNT'})
#print(monthly_df.dtypes)

logger.info("Adding new field in the DataFrame 'Rank' to rank the location/sensor based on total monthly pedestrian counts")

monthly_df['RANK'] = monthly_df.groupby(['MONTH'])['MONTHLY_CNT'].rank(method="first", ascending=False)

logger.info("Filter data in the DataFrame to keep only TOP 10 location/sensor for each Month from beginning of records")

monthly_df = monthly_df.loc[monthly_df['RANK']<=10]
#print(monthly_df.head(50))

logger.info("Generating complete sensors location source file path/name")

src_sensors_loc_file = config_data['local_path'] + config_data['sensors_loc']

logger.info("Building a new DataFrame for Sensor location ref file to get location for each sensor")

src_sensors_df = pd.read_csv(src_sensors_loc_file)
sensors_df = src_sensors_df[['sensor_id', 'sensor_description', 'location']]
sensors_df.columns = sensors_df.columns.str.upper()
#print(sensors_df.head(5))

logger.info("Joining Sensor location and Day-wise dataframe to generate extract")

day_sensor_df = pd.merge(day_df, sensors_df, on='SENSOR_ID', how='inner')
day_sensor_df = day_sensor_df.sort_values(by=['DAY','RANK'], ascending=['True','True'])
#print(day_sensor_df.head(5))

logger.info("Joining Sensor location and Month-wise dataframe to generate extract")

month_sensor_df = pd.merge(monthly_df, sensors_df, on='SENSOR_ID', how='inner')
month_sensor_df = month_sensor_df.sort_values(by=['MONTH','RANK'], ascending=['True','True'])
#print(month_sensor_df.head(5))

#month_sensor_df.to_csv('/Users/hemantsingh/Desktop/monthly_extract.csv', index=False)
#file_name = '/Users/hemantsingh/Desktop/monthly_extract.csv'

logger.info("Preparing to upload extracts to S3 bucket")

monthly_csv_io = StringIO()
month_sensor_df.to_csv(monthly_csv_io, index=False)

#we will need fastparquet and pyarrow - engines to be install for parquet
month_pq_io = BytesIO()
month_sensor_df.to_parquet(month_pq_io, engine='auto', compression='snappy')

day_csv_io = StringIO()
day_sensor_df.to_csv(day_csv_io, index=False)

day_pq_io = BytesIO()
day_sensor_df.to_parquet(day_pq_io, engine='auto', compression='snappy')

logger.info("Reading config to get S3 bucket")

bucket = config_data['s3_bucket']

logger.info("finalizing object keys to be uploaded")

m_obj_key = 'monthly_extract.csv'
d_obj_key = 'day_extract.csv'
m_pq_obj_key = 'monthly_extract.snappy.parquet'
d_pq_obj_key = 'day_extract.snappy.parquet'

logger.info("Getting an S3 client object")

s3_client = boto3.client('s3')
try:
    #response = s3_client.upload_file(file_name, bucket, object_key)
    logger.info("Uploading {0} to S3 bucket {1}".format(m_obj_key,bucket))
    response = s3_client.put_object(
        ACL = 'private',
        Body = monthly_csv_io.getvalue(),
        Bucket = bucket,
        Key = m_obj_key)

    logger.info("Upload complete")

    logger.info("Uploading {0} to S3 bucket {1}".format(d_obj_key,bucket))

    response = s3_client.put_object(
        ACL = 'private',
        Body = day_csv_io.getvalue(),
        Bucket = bucket,
        Key = d_obj_key)

    logger.info("Upload complete")

    logger.info("Uploading {0} to S3 bucket {1}".format(d_pq_obj_key,bucket))

    response = s3_client.put_object(
        ACL = 'private',
        Body = day_pq_io.getvalue(),
        Bucket = bucket,
        Key = d_pq_obj_key)

    logger.info("Upload complete")

    logger.info("Uploading {0} to S3 bucket {1}".format(m_pq_obj_key,bucket))

    response = s3_client.put_object(
        ACL = 'private',
        Body = month_pq_io.getvalue(),
        Bucket = bucket,
        Key = m_pq_obj_key)

    logger.info("Upload complete")

except ClientError as e:
    logging.error(e)

