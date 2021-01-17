from time import sleep

from etl.etl import ETL

if __name__ == '__main__':

    sleep(10)

    processor = ETL()
    processor.run()
