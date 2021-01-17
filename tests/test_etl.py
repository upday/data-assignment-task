import os
import unittest

from etl.etl import ETL


class TestETL(unittest.TestCase):

    def setUp(self) -> None:
        self.processor = ETL()
        self.test_file_name = 'lake/2019-02-15.tsv'

    def test_get_list_of_files_from_s3(self):
        list_of_files = self.processor.get_list_of_files_from_s3()
        print(list_of_files)
        assert isinstance(list_of_files, list)
        assert len(list_of_files) > 0
        assert len([i for i in list_of_files if not str(i).endswith('tsv')]) == 0

    def test_extract(self):
        local_path = self.processor.extract('lake/2019-02-15.tsv')

        assert local_path == self.test_file_name.split('/')[1]

    def test_transform(self):
        if len(self.processor.df) == 0:
            self.processor.extract(self.test_file_name)
        self.processor.transform()

        assert hasattr(self.processor, 'article_performance_df')
        assert hasattr(self.processor, 'user_performance_df')
        assert len(self.processor.article_performance_df) > 0
        assert len(self.processor.article_performance_df) == len(self.processor.article_performance_df)

    def test_download_file(self):
        local_path = self.processor.download_file(self.test_file_name)

        assert local_path in os.listdir()

    def tearDown(self) -> None:
        for file in [i for i in os.listdir() if i.endswith('.tsv')]:
            os.remove(file)


if __name__ == '__main__':
    unittest.main()
