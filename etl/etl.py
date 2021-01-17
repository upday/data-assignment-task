import json
import os

import boto3
import psycopg2
from botocore import UNSIGNED
from botocore.config import Config
import pandas as pd

RELEVANT_EVENTS = ['article_viewed', 'top_news_card_viewed', 'my_news_card_viewed']
HOST = os.getenv('DB_HOST', 'postgres')
DATABASE = os.getenv('database', 'database')
USER = os.getenv('user', 'user')
PASSWORD = os.getenv('password', 'password')

s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
upday_bucket = s3.Bucket('upday-data-assignment')


class ETL:
    """
    This class has functionality to run all ETL steps from start to the end
    """

    COLUMN_NAMES_ARTICLE = ['article_id', 'event_name', 'timestamp']
    COLUMN_NAMES_USER = ['user_id', 'event_name', 'session_id', 'timestamp']

    def __init__(self):
        """
        Get list of file names to run ETL on and initialize tables in database
        """
        self.df = pd.DataFrame()
        self.files_to_extract = []

    def run(self):
        """Run all etl steps"""
        # Get list of files
        self.files_to_extract = self.get_list_of_files_from_s3()
        # Initialize database tables
        self.initialize()
        # Loop through file and run etl steps on them one-by-one
        for file in self.files_to_extract:
            local_path = self.extract(file)
            self.transform()
            self.load()
            self.clean_up(local_path)

    @staticmethod
    def get_list_of_files_from_s3():
        """Get available object name from S3 bucket for further processing"""
        list_of_files = []
        for my_bucket_object in upday_bucket.objects.filter(Prefix='lake/'):
            if str(my_bucket_object.key).endswith('.tsv'):
                list_of_files.append(my_bucket_object.key)

        return list_of_files

    def initialize(self):
        """Create database tables in postgres db

        Tables are:
            * article_performance
            * user_performance
        """
        self.execute_on_db(file='etl/create_article_performance.sql')
        self.execute_on_db(file='etl/create_user_performance.sql')

    @staticmethod
    def execute_on_db(query: str = None, file: str = None):
        """
        Execute passed query on database. Query can be passed as string or file(path)
        At least one of them should be passed to make it work.

        Args:
            query: Query to execute
            file: SQL file to execute

        Returns:
            None
        """
        if query is None and file is None:
            print("Query or SQL file should be provided")
            raise Exception

        print("Starting table init")
        with psycopg2.connect(host=HOST,
                              database=DATABASE,
                              user=USER,
                              password=PASSWORD) as connection:
            try:
                cursor = connection.cursor()
                sql = query if query else open(file, "r").read()
                cursor.execute(sql)
                connection.commit()
                print("SUCCESS")
            except Exception as error:
                connection.rollback()
                print(error)

    def extract(self, file_path: str):
        """
        Download data from specified file_path and reads into pandas dataframe

        Args:
            file_path: url to download data

        Returns:
            local_path(str): path where file is stored locally
        """
        local_path = self.download_file(file_path)
        self.df = pd.read_csv(local_path,
                              sep='\t',
                              converters={
                                  'ATTRIBUTES': self.parse_column
                              })

        return local_path

    @staticmethod
    def download_file(file_path):
        """
        Download object from S3 and store locally

        Args:
            file_path: S3 object path/name

        Returns:
            local_path(str): path where file is stored locally
        """
        local_path = file_path.split('lake/')[1]
        if local_path not in os.listdir():
            upday_bucket.download_file(file_path, local_path)

        return local_path

    @staticmethod
    def parse_column(data, key: str = 'id'):
        """
        This (hacky) helper function gets string representation of json,
        transforms into dict and parses only specified column

        If it fails to convert string into dict, function will return None

        Args:
            data: string representation of json
            key: key to parse from dictionary
        Returns:
            dict with only value of key
        """
        try:
            return json.loads(data)[key]
        except Exception as e:
            print(e)

    def transform(self):
        """Run basic transformations on dataframe"""

        self.df.rename(columns={'ATTRIBUTES': "ARTICLE_ID",
                                'MD5(USER_ID)': 'USER_ID',
                                'MD5(SESSION_ID)': 'SESSION_ID'},
                       inplace=True)

        self.df = self.df.loc[self.df['EVENT_NAME'].isin(RELEVANT_EVENTS)]
        self.df.columns = self.df.columns.str.lower()

        # Not the most optimal way to store them, but should work for now TODO: store just reference
        self.article_performance_df = self.df[self.COLUMN_NAMES_ARTICLE]
        self.user_performance_df = self.df[self.COLUMN_NAMES_USER]
