import os
import psycopg2
import logging
from dwh.extraction import extracted_data
from dwh.loading import load_sql
from configparser import ConfigParser

# Logging to File and Console
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s %(funcName)s %(message)s',
                    handlers=[logging.FileHandler("assignment.log", mode='w'),
                              stream_handler])

# Reading properties from jobfeed.ini file
config_object = ConfigParser()
config_object.optionxform = lambda option: option
config_object.read('jobfeed.ini')

bucket_name = config_object["DATAINFO"]["BUCKET_NAME"]
input_files = config_object["DATAINFO"]["INPUT_FILES"].split(',')

access_key_id = os.environ['AWS_ACCESS_KEY_ID']
secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

try:
    connection = psycopg2.connect(user='user',
                                  password='password',
                                  host='postgres',
                                  database='database')
    logging.info("Connected to database")

    # Extraction
    df = extracted_data(access_key_id, secret_access_key, bucket_name, input_files)
    # Loading
    load_sql(df, connection, './sql/')

except (Exception, psycopg2.Error) as error:
    logging.error('Error while connecting to PostgreSQL:{error}', error=error)

finally:
    connection.close()
    logging.info('EL Pipeline Completed')