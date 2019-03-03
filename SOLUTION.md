# Solution

The task is implemented as a Python3.x application using Pandas as a dataprocessing tool

Pandas DafaFrame is leveraged to treat the 'tsv' data files as structured dataset 
to perform required transformations in memory

The transformed data shall then be loaded into Postgres using `COPY` with `cursor.copy_from(..)`, a much efficient 
way to load data from delimited files into DB

## Source code
The source code of the application is placed inside `etl` folder .
* The connection to DB are managed by the `Connectionpool` class in the `DBUtil.py` file
* The Extract function is implemented in `Extractor.py`, retrieving objects from s3 and concatenating them into a single `pandas.DataFrame`
* The Transformation logic are encapsulated within the `Transformer.py` file, performing all the required data wrangling on the DataFrame created by Extractor
* The `Loaded.py` contains logic to copy data from Demilited files created by Transformer into Postgres DB

## UnitTest

Unit tests can be run using `python -m unittest discover test` from the root directory of the project with `virtualenv` activated
this shall run all the tests inside the `test/` folder

```
(data-assignment-task) soma@soma-VirtualBox:~/ssa/2/data-assignment-task$ python -m unittest discover  test
....
----------------------------------------------------------------------
Ran 4 tests in 10.386s

OK
```

## Run

The application can be run with  `docker-compose up --build` to enforce building the etl container

This will load the data into the Postgres DB into 2 tables USER_PERFORMANCE and ARTICLE_PERFORMANCE 
* ARTICLE_PERFORMANCE: for performing 
  * daily clicks
  * cumulative sum of click throughout the days
  * click through rate per day (clicks over displays)
* user_performance: for performing
  * daily average session length
  * daily click through rate
 
```
(data-assignment-task) soma@soma-VirtualBox:~/ssa/2/data-assignment-task$ docker-compose up --build
Building etl
.....
.....
Successfully built 37d8eccae117
Successfully tagged data-assignment-task_etl:latest
Creating data-assignment-task_postgres_1 ... done
Creating data-assignment-task_etl_1      ... done
Attaching to data-assignment-task_postgres_1, data-assignment-task_etl_1
postgres_1  | The files belonging to this database system will be owned by user "postgres".
postgres_1  | This user must also own the server process.
.....
.....
postgres_1  | 2019-03-03 15:39:10.084 UTC [1] LOG:  database system is ready to accept connections
etl_1       | Connection pool created successfully
etl_1       | CREATE TABLE IF NOT EXISTS USER_PERFORMANCE(..) executed successfully
etl_1       | CREATE TABLE IF NOT EXISTS ARTICLE_PERFORMANCE(..) executed successfully
etl_1       | Data loaded successfully into ARTICLE_PERFORMANCE table
etl_1       | Data loaded successfully into USER_PERFORMANCE table
data-assignment-task_etl_1 exited with code 0

```
