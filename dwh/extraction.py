from datetime import datetime
import boto3
import logging
import json
from io import BytesIO
import pandas as pd

'''
   * Extracts Raw Data from S3 Buckets.
   *
   * @param access_key_id:      Keys for programmatic access to AWS S3
   * @param secret_access_key:  Keys for programmatic access to AWS S3
   * @param bucket_name:        Data Storage location in AWS S3
   * @param input_files:        Files to extract from S3 Bucket
   * @return input_df:          Dataframe of Raw Data
'''


def extracted_data(access_key_id, secret_access_key, bucket_name, input_files):

    # Creates S3 Connection
    s3_client = create_aws_connection(access_key_id, secret_access_key)

    # Creating empty dataframe to append S3 files
    input_df = pd.DataFrame()

    # Reading and appending each S3 file to pandas dataframe
    for file in input_files:
        input_stg = read_data(s3_client, bucket_name, file, date_column='TIMESTAMP')
        input_stg.insert(len(input_stg.columns), 'inserted_at', str(datetime.now()))
        input_df = input_df.append(input_stg, ignore_index=True)
    logging.info('Data from S3 buckets appended to DataFrame')

    # Exploding Attributes column and concatenating it dataframe
    input_df['ATTRIBUTES'] = input_df['ATTRIBUTES'].str.lower().fillna('{}')
    input_df = explode_column(input_df, 'ATTRIBUTES')

    # Cleaning column names in dataframe
    input_df = input_df.rename(lambda col_name: col_name.lower().replace('(', '_')
                               .replace(')', '').replace(' ', '_'), axis='columns') \
                               .rename(columns={'timestamp': 'timestamp_'})
    logging.info('Columns Names are {columns}'.format(columns=input_df.columns))
    return input_df


'''
   * Creates Service Client for S3.
   *
   * @param access_key_id:      Keys for programmatic access to AWS S3
   * @param secret_access_key:  Keys for programmatic access to AWS S3
   * @return client:            Service Client for S3
'''


def create_aws_connection(access_key_id, secret_access_key):
    get_session = boto3.Session(aws_access_key_id=access_key_id,
                                aws_secret_access_key=secret_access_key)
    logging.info("Session Created")

    client = get_session.client('s3')
    logging.info("Service Client for S3 Created")
    return client


'''
   * Reads TSV file from S3 Bucket and creates pandas Dataframe.
   *
   * @param client:        Service Client for S3
   * @param date_column:   Timestamp
   * @param bucket_name:   Data Storage location in AWS S3
   * @param file_name:     Individual File to extract from S3 Bucket
   * @return input_df:     Dataframe from Individual S3 File
'''


def read_data(client, bucket_name, file_name, date_column):
    get_data = client.get_object(Bucket=bucket_name, Key=file_name)

    input_df = pd.read_csv(BytesIO(get_data['Body'].read()), parse_dates=[date_column],
                           date_parser=pd.to_datetime,
                           sep='\t',
                           header='infer',
                           encoding='utf-8')

    input_df['bucketname'] = bucket_name
    input_df['itemname'] = file_name
    return input_df


'''
   * To Flatten Attribute Column of Raw Data.
   *
   * @param input_df:      input Dataframe
   * @param column:        Column to explode
   * @return exploded_df:  Exploded dataframe
'''


def explode_column(input_df, column):
    json_data = input_df[column].apply(json.loads).apply(pd.Series) \
                                .rename(lambda col_name: 'attributes_' + col_name, axis='columns')
    exploded_df = pd.concat([input_df, json_data], axis=1).replace(r'\n', ' ', regex=True)
    logging.info('Exploded {column} column'.format(column=column))
    return exploded_df
