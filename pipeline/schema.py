# -*- coding: utf-8 -*-
#!/usr/bin/python

"""
    This module defines tables for Database 
"""
import psycopg2

class Registory(object):
    def articles_table():
        """
            Contrains Article  Create Table Statement
            :return: None            
        """        
        return f""" CREATE TABLE IF NOT EXISTS article_performance(
            ARTICLE_ID VARCHAR(64),
            EVENT_NAME VARCHAR(64),
            TIMESTAMP timestamp,
            DATE date
        )"""
    
    
    def users_table():
        """
            Contains User Create Table Statement
            :return: None                    
        """        
        return f""" CREATE TABLE IF NOT EXISTS user_performance(
            USER_ID VARCHAR(64),
            EVENT_NAME VARCHAR(64),
            SESSION_ID VARCHAR(64),
            TIMESTAMP timestamp,
            DATE date
        )"""
    
    
    def create_tables(self, connection_config):
        """
            Creates users and articles performance table in the PostgresDB
            :param connection_config: database details
            :return: None
        """
        try:
            with psycopg2.connect(**connection_config) as connection:
                cursor = connection.cursor()
                cursor.execute(Registory.articles_table())
                print("Success!! Article Table Created")
                cursor.execute(Registory.users_table())
                print("Success!! User Table Created")
                connection.commit()
                cursor.close()
                
        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
