import psycopg2


def create_article_table():
    return f""" CREATE TABLE IF NOT EXISTS article_performance(
        ARTICLE_ID VARCHAR(40),
        EVENT_NAME VARCHAR(40),
        TIMESTAMP timestamp,
        UPDATE_TIMESTAMP timestamp default current_timestamp,
        DATE date
    )"""


def create_user_table():
    return f""" CREATE TABLE IF NOT EXISTS user_performance(
        USER_ID VARCHAR(40),
        EVENT_NAME VARCHAR(40),
        SESSION_ID VARCHAR(40),
        TIMESTAMP timestamp,
        UPDATE_TIMESTAMP timestamp default current_timestamp,
        DATE date
    )"""


def create_tables(connection_params):
    """
    :param connection_params: database connection params
    :return: None
    Creates user_table and article_table in the postgresdb
    """
    try:
        with psycopg2.connect(**connection_params) as connection:
            cursor = connection.cursor()
            cursor.execute(create_article_table())
            print("clap and dance article table created!! Great success :D")
            cursor.execute(create_user_table())
            print("clap and dance user table created!! Great success :D")
            connection.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is now closed")




