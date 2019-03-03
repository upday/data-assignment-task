import boto3
import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config


class Extractor:
    """
        Class encapsulating extraction logic objects in S3 and converting them to pandas.DataFrame
    """

    def __init__(self, bucket, prefix='/') -> None:
        self.bucket = bucket
        self.prefix = prefix

    def load_s3_oject_as_df(self):
        return self.__df_from_s3()

    def __filelist_from_s3(self):
        """
            Given bucket name and prefix, get all the objects in s3://{bucket}/{prefix}/ location
            signature_version=UNSIGNED to be able to run from docker with no Credential chain.
            Can only access public bucket objects

            :return:
                file_list list of string of object names in s3://{bucket}/{prefix}/
        """
        file_list = []
        s3client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
        s3_objects = s3client.list_objects_v2(Bucket=self.bucket, StartAfter=self.prefix)
        for object in s3_objects['Contents']:
            file_list.append(object['Key'])
        return file_list

    def __df_from_s3(self):
        """
            Fetches S3 objects and concat and return a single pandas.DataFrame

            :return:
                df  pandas.DataFrame
        """
        df = pd.DataFrame()
        for file in self.__filelist_from_s3():
            temp_df = pd.read_csv('s3://{}/{}'.format(self.bucket, file), sep='\t', header=0)
            if df.empty:
                df = temp_df
            else:
                df = pd.concat((df, temp_df))
        return df
