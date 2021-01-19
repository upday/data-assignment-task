# Overview
Pipeline package is created to complete ETL flow, which contains code files for Extracting, Transforming, loading of data.
It also includes code for creating tables. Main file `run.py` holds flow & integration of pipline steps.

`Run.py` is modified in a way to make it a driving file. It reads data file extracted by extract functionlity & injects these in transcformation for loading purpose..

It is considered that-
- Create table first if not exists
- Downlaods all available files from bucket
- Puiblish these to tables created
- If same date data is available in 2nd run it will create duplicate data

# Pipeline Flow:
1. Creating Table from pipeline/schema.py
1. Downloading file from pipeline/extraction.py
1. Transforming data by pipeline/transformation.py
1. Loading data by pipeline/loader.py
1. Deleting file from local by extraction object


# Populate Database using:
     docker-compose up

# Testing
A python test file is created in parallel to `run.py`, which holds testing functionaly for transformed data. 
It includes checking of columns, null records & length check

# Create a virtual using
     python3 -m venv up_de_test

# Activate venv using:
    . up_de_test/bin/activate

# Install dependencies using:
    pip install -r reauirements.txt

# Run tests:
    python test.py 