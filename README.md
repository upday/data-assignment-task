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
The events triggered when a user swipes  an article are named  `top_news_card_viewed` or `my_news_card_viewed`, depending on which section of the app the user was using.
You can find the article ids inside the `attributes` field, under `id`.

The data is available at https://s3.console.aws.amazon.com/s3/buckets/upday-data-assignment/lake/
The bucket contains raw event data from our app.

## Instructions
The candidate should make a pull request to this repository containing his/her solution.
What we expect:
* The code implementing the ETL logic
* A Dockerfile (you should complete the existing one) that we can use to build and run the logic
* The docker compose file will create a Postgres container already connected to the ETL container. Please use that postgres instance for storing your model and solution.
* Once the docker container is run, the reviewer should be able to inspect the Postgres Database to verify the correctness of the aggregated table.
