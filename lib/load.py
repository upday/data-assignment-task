import psycopg2
import psycopg2.extras as extras

class Loader:
    ARTICLE_TABLE = "article_performance"
    USER_TABLE = "user_performance"

    def load_into_article_table(self, df, cursor, connection):
        article_df = df[['ARTICLE_ID', 'EVENT_NAME', 'TIMESTAMP', 'DATE', 'UPDATE_TIMESTAMP']]
        df_columns = list(article_df)
        columns = ",".join(df_columns)
        # create one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        query = "INSERT INTO {} ({}) {}".format(self.ARTICLE_TABLE, columns, values)
        extras.execute_batch(cursor, query, article_df.values)
        connection.commit()


    def load_into_user_table(self, df, cursor, connection):
        user_df = df[['USER_ID', 'EVENT_NAME', 'SESSION_ID', 'TIMESTAMP', 'DATE', 'UPDATE_TIMESTAMP']]
        df_columns = list(user_df)
        columns = ",".join(df_columns)
        # create one '%s' per column
        values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
        query = "INSERT INTO {} ({}) {}".format(self.USER_TABLE, columns, values)
        extras.execute_batch(cursor, query, user_df.values)
        connection.commit()


    def populate_with_data(self, connection_params, data_frame):
        try:
            with psycopg2.connect(**connection_params) as connection:
                cursor = connection.cursor()
                self.load_into_article_table(data_frame, cursor, connection)
                print("article table is now populated!! Great success :D")
                self.load_into_user_table(data_frame, cursor, connection)
                print("user table is now populated!! Great success :D")

        except (Exception, psycopg2.Error) as error:
            print("Error while connecting to PostgreSQL", error)
            connection.rollback()
            cursor.close()

        finally:
            if (connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is now closed")