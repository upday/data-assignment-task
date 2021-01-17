import os

import boto3
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
