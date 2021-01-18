# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module is downloading data from s3
"""

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import os


class Extract(object):
    def __init__(self, config):
        """
            Data Extraction init function which takes config as argument
            :params
                config - contains gloabl configuration
        """        
        self._config = config
        self._s3 = boto3.resource('s3', config=Config(signature_version=UNSIGNED))
    
    def _check_data_dir(self):
        """
            Check whether data_directory exists or not if not make it
            return:
                files_dir - Complete path for data directory
        """        
        working_dir = os.getcwd()
        files_dir = os.path.join(working_dir, self._config['data_directory'])
        if not os.path.exists(files_dir):
            os.makedirs(files_dir)
        return files_dir        
    
    def download_files(self):
        """
            Downloading s3 data files using global parameters 
            return:
                files_list - list of downloaded files
        """        
        files_list = []
        files_dir = self._check_data_dir()
        bucket = self._s3.Bucket(self._config["bucket"])
        for obj in bucket.objects.filter(Prefix=self._config["drive"]):
            location = os.path.join(files_dir, os.path.relpath(obj.key, self._config["drive"]))
            if obj.key[-1] != '/':
                #print(obj.key)
                bucket.download_file(obj.key, location)
                files_list.append(location)
        return files_list
    
    def unlink(self, file_name):
        """
            Deletes file
            return:
                file_name - file name to delete
            return:
                None
        """        
        os.unlink(file_name)