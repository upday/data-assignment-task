import logging
import sys
from io import StringIO
import pandas as pd


def load_sql(df, connection, SQL_path):
    connection.autocommit = True
    cursor = connection.cursor()
    query = open(SQL_path + 'recreate_schemas.sql', 'r')
    cursor.execute(query.read())
    logging.info('Recreated Schemas')

    create_statement = pd.io.sql.get_schema(df, "staging_db.stg_performance") \
        .replace('"', '').replace('INTEGER', 'BOOLEAN')
    logging.info('Create Statement:{create_statement}'.format(create_statement=create_statement))
    cursor.execute(create_statement)

    load_dataframe(connection=connection, dataframe=df, table='staging_db.stg_performance')

    query = open(SQL_path + 'staging_insert_table.sql', 'r')
    cursor.execute(query.read())
    logging.info('Removed duplicates and inserted in staging_db.staging_insert_table')

    query = open(SQL_path + 'operational_create_tables.sql', 'r')
    cursor.execute(query.read())
    logging.info('Created tables operational_db.article_performance And operational_db.user_performance')

    query = open(SQL_path + 'operational_insert_tables.sql', 'r')
    cursor.execute(query.read())
    logging.info('Data inserted into operational_db.article_performance And operational_db.user_performance tables')


def load_dataframe(connection, dataframe, table):
    str_io = StringIO()
    str_io.write(dataframe.to_csv(index=None, header=None, sep="\t"))
    str_io.seek(0)

    with connection.cursor() as conn:
        conn.copy_from(str_io, table, columns=dataframe.columns, sep="\t", null='')
        connection.commit()

    logging.info('Input Data Inserted to Postgres Table')