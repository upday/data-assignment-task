import psycopg2
from time import sleep
from lib.download_files import download_s3_folder


BUCKET_NAME = "upday-data-assignment"
FOLDER_NAME = "lake"


if __name__ == '__main__':
    sleep(10)
    download_s3_folder(BUCKET_NAME, FOLDER_NAME)


    try:
        # Just some sample code.
        with psycopg2.connect(user='user',
                              password='password',
                              host='postgres',
                              database='database') as connection:
            cursor = connection.cursor()
            # Print PostgreSQL Connection properties
            print(connection.get_dsn_parameters(), '\n')

            # Print PostgreSQL version
            cursor.execute('SELECT version();')
            record = cursor.fetchone()
            print('You are connected to - ', record, '\n')
    except (Exception, psycopg2.Error) as error:
        print('Error while connecting to PostgreSQL', error)