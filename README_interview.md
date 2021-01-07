Assumptions and shortcuts made:
- files are always small enough to be loaded and processed in memory
- each file contains complete data for the day (no updates necessary for a given day - once the data is 
processed with pandas it can be loaded into postgres and not expected to change)
- files are downloaded from the s3 burcket in the /data subfolder in the root project folder and 
processing is manually triggered with docker compose up
 (no logic for automatic stuff like trigger lambda once file is placed in the s3 bucket etc.)
- it is assumed cleanup/management of the s3 bucket is done outside of this project, hence code downloads all available 
files and processes them at once and it is assumed collectively they can be processed in memory

# Data flow:
has been built on top of existing logic.

1. Data is downloaded from s3 bucket into local data dir (download_files.py)
1. Data is transformed by Transformer class (lib/transform_data.py) and all files are returned collectively as one data
frame by tranform_all_folder_files function
1. DDL's are created in postgreSQL (sql_ddl/create_tables.py)
1. Data is loaded in batch to postgreSQL using psycopg2 and psycopg2.extras (lib/load.pt)


#To run and populate the database:
docker-compose up

#UNIT TESTS
# create a virtual 
     python3 -m venv venv

# activate venv:
    . venv/bin/activate
    
# make sure all dependencies are installed:
    pip install -r reauirements.txt

# To run unit tests:
    cd tests
    pytest