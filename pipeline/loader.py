# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module is Loads data to database by preparing insert statement
"""
import psycopg2
from psycopg2 import extras

class Load(object):
    def __init__(self, config, tables):
        """
            Loader init function which takes config as argument & data
            :params
                config - contains gloabl configuration
                tables - mapping of tables & relevent data
        """        
        self._config = config
        self._data_tables = tables

    def _populate(self, connection, data, table):
        """
            _populate prepares insert statement & execute query
            :params
                connection - connection tunnel 
                data - data for table
                table - table Name
            :return 
                None
        """        
        columns = ",".join(data.columns)
        values = "VALUES({})".format(",".join(["%s" for _ in data.columns]))
        query = "INSERT INTO {} ({}) {}".format(self._config['{}_table'.format(table)], columns, values)
        extras.execute_batch(connection.cursor(), query, data.values)
        connection.commit()
        
    def populate_with_data(self, connection_params):
        """
            creating connection & populating data via _populate
            :params
                connection_params - connection related details
            :return 
                None
        """        
        try:
            with psycopg2.connect(**connection_params) as connection:
                cursor = connection.cursor()
                
                for key, data in self._data_tables.items():
                    self._populate(connection, data, key)
                    print("{} Table published!!".format(key.title()))
                
                cursor.close()

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)