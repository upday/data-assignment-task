import datetime
from dateutil.parser import parse
import json
from etl.panda_utils import *
import os

import pandas as pd


class Transformer:
    """
        Class to perform all the necessary transformation  on the events dataframe
    """

    def __init__(self, df) -> None:
        self.event_df = df

    def transform(self):
        import warnings
        warnings.filterwarnings("ignore")
        filtered_df = self.__filter_unrelevant_events(self.event_df)
        flattened_df = self.__flatten_attributes_to_columns(filtered_df)
        return self.__normalize_columns(flattened_df)

    def __filter_unrelevant_events(self, df):
        """
            Filter events which event EVENT_NAME are not article_viewed, top_news_card_viewed, my_news_card_viewed
            and ATTRIBUTES column that are null
            :param df: events dataframe
            :return: pandas.DataFrame
        """
        return df[((df.EVENT_NAME == 'article_viewed')
                   | (df.EVENT_NAME == 'top_news_card_viewed')
                   | (df.EVENT_NAME == 'my_news_card_viewed')) & df.ATTRIBUTES.apply(
            lambda x: isinstance(x, str))]

    def __flatten_attributes_to_columns(self, df):
        """
            explodes json value in ATTRIBUTES column into DataFrame columns
            :param df: dataframe with ATTRIBUTES columns having json string
            :return: pandas.DataFrame
        """
        attributes = df.ATTRIBUTES.apply(json.loads).apply(pd.Series)

        for col in attributes.columns.values:
            df[normalise_name(col)] = attributes[col]

        return df.drop(['ATTRIBUTES'], axis=1)

    def __normalize_columns(self, df):
        normalized_df = normalize_column_names(df)
        normalized_df['UPDATE_TIMESTAMP'] = datetime.datetime.now()
        return normalized_df

    class PersistHelper:
        """
            Class encapsulating functionality of saving dataframe into Delimited file
        """

        def __init__(self, df, file_loc) -> None:
            self.df = df
            self.file_loc = file_loc

        def save_df_to_filesystem(self):
            os.makedirs(self.file_loc, exist_ok=True)
            self.__save__article_performace()
            self.__save__user_performace()

        def __save__article_performace(self):
            fact_article_access = self.df[
                ['id', 'EVENT_NAME', 'TIMESTAMP', 'category', 'sourceName', 'publishTime', 'url', 'UPDATE_TIMESTAMP']]
            fact_article_access['DATE'] = self.df['TIMESTAMP'].apply(lambda x: parse(x).date())
            fact_article_access.to_csv('{}/fact_article_access.csv'.format(self.file_loc), index=False, header=False,
                                       sep='{')

        def __save__user_performace(self):
            fact_user_activity = self.df[
                ['MD5(USER_ID)', 'TIMESTAMP', 'MD5(SESSION_ID)', 'EVENT_NAME', 'browser', 'UPDATE_TIMESTAMP']]
            fact_user_activity.to_csv('{}/fact_user_activity.csv'.format(self.file_loc), index=False, header=False,
                                      sep=',')
