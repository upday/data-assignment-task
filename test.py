# -*- coding: utf-8 -*-

import json
import pandas as pd
from pipeline.transformation import Transform



class MainTest(object):
    def __init__(self, config):
        """
            Holds Global configuration
        """
        self._config = config
        
    def test_columns(self):
        """
            Testing whether columns are matching after transformation or not
        """
        data = pd.read_csv("tests/2019-02-15.tsv", delimiter="\t")
        
        transform = Transform(self._config)
        transform.prepare_data(data)
        columns = transform._data.columns        
        
        assert columns.tolist() == ['TIMESTAMP', 'EVENT_NAME', 'DATE', 'USER_ID', 'ARTICLE_ID', 'SESSION_ID'], "Few Columns are missing."

    def test_article_id_null(self):
        """
            Testing whether Article id is null or not
        """
        data = pd.read_csv("tests/2019-02-15.tsv", delimiter="\t")
        
        transform = Transform(self._config)
        transform.prepare_data(data)

        assert len(transform._data[transform._data['ARTICLE_ID'].isnull()]) == 0, "ARTICLE ID is null"

    def test_date_null(self):
        """
            Testing whether Date is null or not
        """
        data = pd.read_csv("tests/2019-02-15.tsv", delimiter="\t")
        
        transform = Transform(self._config)
        transform.prepare_data(data)

        assert len(transform._data[transform._data['DATE'].isnull()]) == 0, "DATE value is null"

    def test_event_null(self):
        """
            Testing whether Evenyt Name null or not
        """
        data = pd.read_csv("tests/2019-02-15.tsv", delimiter="\t")
        
        transform = Transform(self._config)
        transform.prepare_data(data)

        assert len(transform._data[transform._data['EVENT_NAME'].isnull()]) == 0, "EVENT_NAME is null"
        
    def test_col_length(self):
        """
            Testing whether Column Length shouldn't exceed max allowed chars length
        """
        data = pd.read_csv("tests/2019-02-15.tsv", delimiter="\t")
        
        transform = Transform(self._config)
        transform.prepare_data(data)
        new_data = transform._data
        
        assert max(new_data["EVENT_NAME"].apply(len)) <= 64 , "EVENT_NAME is exceeding column data length"
        assert max(new_data["ARTICLE_ID"].apply(len)) <= 64 , "ARTICLE_ID is exceeding column data length"
        assert max(new_data["USER_ID"].apply(len)) <= 64 , "USER_ID is exceeding column data length"
        assert max(new_data["SESSION_ID"].apply(len)) <= 64 , "SESSION_ID is exceeding column data length"


if __name__ == "__main__":
    
    with open("config.json") as file:
        config = json.load(file)
    
    test_obj = MainTest(config)

    test_obj.test_columns()
    test_obj.test_article_id_null()
    test_obj.test_date_null()
    test_obj.test_event_null()
    test_obj.test_col_length()