#!/usr/bin/python
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os


# Since it's a public folder - signing without aws credentials (UNSIGNED)
s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))


def ensure_data_dir(directory_name):
    """
       Make sure local directory where you want to place the files from the s3 bucket's folder exists
       Args:
           directory_name: the name of the local dir where you want to place the files
       """
    parent_dir = os.getcwd()
    data_dir = os.path.join(parent_dir, directory_name)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    return data_dir


def download_s3_folder(bucket_name, s3_folder):
    """
    Download the contents of a folder directory
    Args:
        bucket_name: the name of the s3 bucket
        s3_folder: the folder path in the s3 bucket
    """
    data_dir = ensure_data_dir("data")
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = os.path.join(data_dir, os.path.relpath(obj.key, s3_folder))
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)