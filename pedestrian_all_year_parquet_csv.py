#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 18:18:24 2022

@author: hemantsingh
"""
import pandas as pd
from sodapy import Socrata
import boto3
from botocore.exceptions import ClientError
from io import StringIO, BytesIO
import pyarrow
import fastparquet
import json
import logging
logger = logging.getLogger(name=None)
logger.setLevel(logging.INFO)


logger.info("Getting configuration data for S3 bucket")

try:
    file = open('config.json', 'r')
    config_data = json.loads(file.read())
except IOError as error:
    logger.error("I/O Error({0}) occurred while opening {1} file : {2} ".format(error.errno, error.filename, error.strerror))
else:
    file.close()

logger.info("Preparing to make a request to SODA API")

#https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json
client = Socrata("data.melbourne.vic.gov.au", None)

#DAY stats over all years
logger.info("Request to get day-wise total of pedestrian counts at each sensor/location")
#Added SOQL to get aggregated data back as response
results = client.get("b2ak-trbp", query="SELECT SENSOR_ID,  DAY,                          \
                                                SUM(HOURLY_COUNTS) AS TOTAL_DAY_COUNT     \
                                                GROUP BY SENSOR_ID, DAY                   \
                                                LIMIT 100000")

# Convert to pandas DataFrame
logger.info("Building pandas datafraame from the resultset")
#day_df 
day_df = pd.DataFrame.from_records(results)
day_df["TOTAL_DAY_COUNT"] = pd.to_numeric(day_df["TOTAL_DAY_COUNT"])
#day_df["SENSOR_ID"] = pd.to_numeric(day_df["SENSOR_ID"])

logger.info("Adding new field in the DataFrame 'Rank' to rank the location/sensor based on total pedestrian counts")

day_df['RANK'] = day_df.groupby(['DAY'])['TOTAL_DAY_COUNT'].rank(method="first", ascending=False)

logger.info("Filter data in the DataFrame to keep only TOP 10 location/sensor for each day from beginning of records")

day_df = day_df.loc[day_df['RANK'] <= 10]

#Monthly stats over all years
logger.info("Request to get Month-wise total of pedestrian counts at each sensor/location")

results = client.get("b2ak-trbp", query="SELECT SENSOR_ID, MONTH ,                      \
                                                date_extract_m(DATE_TIME) AS MM,        \
                                                SUM(HOURLY_COUNTS) AS MONTHLY_CNT       \
                                                GROUP BY SENSOR_ID, MONTH, MM           \
                                                LIMIT 100000")

monthly_df = pd.DataFrame.from_records(results)
monthly_df[['MM','MONTHLY_CNT']] = monthly_df[['MM','MONTHLY_CNT']].apply(pd.to_numeric)
#print(monthly_df.dtypes)
monthly_df['RANK'] = monthly_df.groupby(['MONTH', 'MM'])['MONTHLY_CNT'].rank(method="first", ascending=False)
monthly_df = monthly_df.loc[monthly_df['RANK']<=10]

logger.info("Building a new DataFrame for Sensor location ref file to get location for each sensor")

#https://data.melbourne.vic.gov.au/resource/h57g-5234.json
results = client.get("h57g-5234", query="SELECT SENSOR_ID, SENSOR_DESCRIPTION, LOCATION\
                                         WHERE STATUS = 'A'")

sensors_df = pd.DataFrame.from_records(results)

logger.info("Joining Sensor location and Day-wise dataframe to generate extract")

day_sensor_df = pd.merge(day_df, sensors_df, on='SENSOR_ID', how='inner')
day_sensor_df = day_sensor_df.sort_values(by=['DAY','RANK'], ascending=['True','True'])
#day_sensor_df.to_csv('/Users/hemantsingh/Desktop/day_extract.csv', index=False)

logger.info("Joining Sensor location and Month-wise dataframe to generate extract")

month_sensor_df = pd.merge(monthly_df, sensors_df, on='SENSOR_ID', how='inner')
month_sensor_df = month_sensor_df.sort_values(by=['MM','RANK'], ascending=['True','True'])
#month_sensor_df.to_csv('/Users/hemantsingh/Desktop/monthly_extract.csv', index=False)

logger.info("Preparing to upload extracts to S3 bucket")

monthly_csv_io = StringIO()
month_sensor_df.to_csv(monthly_csv_io, index=False)

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

