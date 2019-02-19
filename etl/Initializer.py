from etl.DBUtil import Connectionpool
import logging


def create_load_tables():
    """
        Creates USER_PERFORMANCE, ARTICLE_PERFORMANCE tables in the postgres DB
    """
    try:
        cp = Connectionpool()
        conn = cp.get_connection()
        cursor = conn.cursor()

        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS USER_PERFORMANCE(
                            USER_ID VARCHAR(50),
                            TIMESTAMP timestamp,
                            SESSION_ID VARCHAR(50),
                            EVENT_NAME VARCHAR(30),
                            browser VARCHAR(10),
                            UPDATE_TIMESTAMP timestamp default current_timestamp
                        )
                    """)

        print("CREATE TABLE IF NOT EXISTS USER_PERFORMANCE(..) executed successfully")

        cursor.execute("""
                        CREATE TABLE IF NOT EXISTS ARTICLE_PERFORMANCE(
                            id VARCHAR(50),
                            EVENT_NAME VARCHAR(30),
                            TIMESTAMP timestamp,
                            category VARCHAR(30),
                            sourceName VARCHAR(50),
                            publishTime VARCHAR(20),
                            url text,
                            UPDATE_TIMESTAMP timestamp default current_timestamp,
                            DATE date
                        )
                    """)

        print("CREATE TABLE IF NOT EXISTS ARTICLE_PERFORMANCE(..) executed successfully")

        conn.commit()
        cursor.close()
        cp.close_connection(conn)

    except Exception as error:
        print("Error while creating tables in DB ", error)
        raise error

