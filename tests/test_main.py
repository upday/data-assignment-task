import os
from lib.transform_data import Transformator
from lib.download_files import download_s3_folder
from os.path import isfile, join


class TestStart(object):
    def test_get_data_columns(self):
        tr = Transformator()
        result_df = tr.get_data('2019-02-15.tsv')
        df_columns = list(result_df)
        assert df_columns == ['TIMESTAMP', 'USER_ID', 'EVENT_NAME', 'SESSION_ID', 'ARTICLE_ID', 'DATE', 'UPDATE_TIMESTAMP']

    def test_download_files(self):
        BUCKET_NAME = "upday-data-assignment"
        FOLDER_NAME = "lake"
        download_s3_folder(BUCKET_NAME, FOLDER_NAME)
        parent_dir = os.getcwd()
        data_dir = os.path.join(parent_dir, "data")
        file_names = [f for f in os.listdir(data_dir) if isfile(join(data_dir, f))]
        assert file_names == ['2019-02-17.tsv', '2019-02-15.tsv', '2019-02-16.tsv']