import psycopg2
import yaml

from time import sleep
from lib.download_files import download_s3_folder
from sql_ddl.create_tables import create_tables
from lib.transform_data import Transformator
from lib.load import Loader


BUCKET_NAME = "upday-data-assignment"
FOLDER_NAME = "lake"
CONFIG_PATH =  "config.yaml"

with open(CONFIG_PATH) as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

params = {
    'user': config['DB']['user'],
    'password': config['DB']['password'],
    'host': config['DB']['host'],
    'database': config['DB']['database']
}


if __name__ == '__main__':
    sleep(10)
    # extract data from s3 buckry
    download_s3_folder(BUCKET_NAME, FOLDER_NAME)

    # execute DDL's in postresSQL
    create_tables(params)

    # transform data from local data folder
    tr = Transformator()
    processed_df = tr.tranform_all_folder_files()

    # load data into postgresSQL
    ld = Loader()
    ld.populate_with_data(params, processed_df)