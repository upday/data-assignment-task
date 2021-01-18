# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module is utilizes data & transform as per need
"""

import pandas as pd

 
class Transform(object):
    def __init__(self, config):
        """
            Data Transform init function which takes config as argument
            :params
                config - contains gloabl configuration
        """        
        self._config = config
        self._data = None
    
    def _extract_attribute(self, json_in, attrib):
        """
            It extracts attributes/keys from json object
            :params
                json_in - json object
                attrib - Attribute to extract from data
                    Assuming Attribute should be in lower/upper, if not found retun None
            :return
                value - value correspondence to attribute provided
        """        

        json_in = eval(json_in)
        attrib = attrib.lower()

        try:
            value = json_in[attrib] if attrib in json_in else json_in[attrib.upper()]
        except KeyError:
            value = None
            
        return value
    
    def prepare_data(self, data):
        """
            Transforms data, filter, date extraction, attribute extraction
            :params
                data - data from data inform of pandas data frame
            :return
                None
        """        
        data = data[
                data['EVENT_NAME'].isin(self._config['events_filter'])
            ]
        
        #Droping null & empty Attributes Data
        data.dropna(subset=['ATTRIBUTES'], inplace=True)
        data = data[data.ATTRIBUTES != '{}']
        
        #transformting timestamp in date
        data['DATE'] = data['TIMESTAMP'].apply(lambda x:pd.to_datetime(x).date())
        data['USER_ID'] = data['MD5(USER_ID)']
        data['ARTICLE_ID'] = data['ATTRIBUTES'].apply(self._extract_attribute, args=(self._config['attribute'],))
        data['SESSION_ID'] = data['MD5(SESSION_ID)']
        data.drop(['MD5(USER_ID)', 'MD5(SESSION_ID)', 'ATTRIBUTES'], axis=1, inplace=True)

        

        self._data = data
    
    def get_transformations(self, table):
        """
            Returns data specific to table, which is transformed by prepare_data
            :params
                table - table name  which helps to return specified data  
            :return 
                Data for specific table
        """        
        return self._data[self._config["{}_columns".format(table)]]