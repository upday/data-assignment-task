# Assignment task for Data Engineer

## Introduction

Assume you are a Data Engineer working at upday. 

You are in close communication with our Business Intelligence team and you need to provide that data that they need for their daily duty.

At the same time, you have the necessary knowledge to find and fetch the raw data that upday receives from the app and make it ready for the BI team needs.

The scope of this task is build everything that is between raw data and BI tables.

## Assignment
The BI teams requires 2 tables:
* article_performance: the table should allow them to calculate the following statistics for each article (id):
  * daily clicks
  * cumulative sum of click throughout the days
  * click through rate per day (clicks over displays)
* user_performance: the table should allow them to calculate user-based statistics for each user:
  * daily average session length
  * daily click through rate
  
The event triggered when a user reads (clicks) an article is named `article_viewed`. 
The events triggered when a user swipes (displays) an article are named  `top_news_card_viewed` or `my_news_card_viewed`, depending on which section of the app the user was using.
You can find the article ids inside the `attributes` field, under `id`.

The data is available at https://s3.console.aws.amazon.com/s3/buckets/upday-data-assignment/lake/

The bucket contains raw event data from our app.

## Instructions
You should make a pull request to this repository containing the solution. If you need to clarify any point of your solution, please include an explanation in the PR description.

What we expect:
* The code implementing the ETL logic, triggered from the `run.py` script
* Extension of the currently existing `Dockerfile` (only if needed)
* Tests, with instructions on how to run them

Some more details:
* The docker compose file will create a Postgres container already connected to the ETL container. You should use that Postgres instance for storing data models and solution.
* The task reviewer will only run `docker-compose up` and wait for the container to exit, 
* After the command prints `data-assignment-task_etl_1 exited with code 0`, the database is expected to be in its final state, with the solution to the task.

The task will be evaluated based on:
* Correctness of the final result
* Readability of the code
* Architecture of the solution
* Data modeling
* Tests (full coverage is not required, you should mainly test the critical parts of the code)


## Installations
```
pip install boto3  
pip install pandas
```

## Notes
1.) Created FileHandler assignment.log file to store the logging information of the task.    
2.) Created jobfeed.ini configuration file to store AWS S3 bucket path and file names.  
3.) extracted_data() method is used to extract data from AWS S3.  
4.) load_dataframe() method is used to load the extracted data to postgres table.  
5.) sql files are stored in sql folder  
6.) Extraction and Loading operations are stored in dwh folder
6.) Created two databases :- staging_db and operational_db.  
7.) Two tables of operational_db:-   
I.)article_performance II.)user_performance are provided to BI team.
