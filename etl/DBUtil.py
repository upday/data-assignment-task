import psycopg2
from psycopg2 import pool
import os
import logging

db_host = os.getenv("DB_HOST", "127.0.0.1")
db_port = os.getenv("DB_PORT", 5432)
db_user = os.getenv("DB_USER", "user")
db_password = os.getenv("DB_PASSWORD", "password")
database = os.getenv("DATABASE", "database")


# Class to create and manage pool of connections, implemented as a Singleton instance
class Connectionpool:
    class __Connectionpool:

        def __init__(self, host, port, user, password, database) -> None:
            """
                Private class to create a connection pool, a singleton implementation

                Parameters
                ----------
                    host
                        Postgres host
                    port
                        Postgres port
                    user
                        Postgres user
                    password
                        Postgres password
                    database
                        Database

                Returns
                -------
                    psycopg2.extensions.connection
                        Returns a connection object from the connection pool

            """

            try:
                self.postgreSQL_pool = psycopg2.pool.SimpleConnectionPool(1, 20, user=user,
                                                                          password=password,
                                                                          host=host,
                                                                          port=port,
                                                                          database=database)
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error while connecting to PostgreSQL", error)
                raise error

            if self.postgreSQL_pool:
                print("Connection pool created successfully")

    instance = None

    def __init__(self) -> None:
        if not Connectionpool.instance:
            Connectionpool.instance = Connectionpool.__Connectionpool(db_host, db_port, db_user, db_password, database)

    def get_connection(self):
        """
            Returns a connection object from the connection pool

            Returns
            -------
            psycopg2.extensions.connection
                Returns a connection object from the connection pool

        """
        return Connectionpool.instance.postgreSQL_pool.getconn()

    def close_connection(self, conn):
        """
            Return a connection to the connection pool

            Parameters
            ----------
            conn: psycopg2.extensions.connection

        """
        Connectionpool.instance.postgreSQL_pool.putconn(conn)

    def clean_up(self):
        """
            Cleans up the connectionpool, to be called to close all the connection
        """
        if Connectionpool.instance.postgreSQL_pool:
            Connectionpool.instance.postgreSQL_pool.closeall
            print("PostgreSQL connection pool is closed")
