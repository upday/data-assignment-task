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
 
  
