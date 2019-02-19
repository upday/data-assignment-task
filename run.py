import psycopg2
from time import sleep

sleep(10)
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
