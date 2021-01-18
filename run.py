import json
from time import sleep

import pandas as pd

from pipeline.schema import Registory 
from pipeline.extraction import Extract 
from pipeline.transformation import Transform 
from pipeline.loader import Load 


#Reading config file for ETL pipelind
print ("++++++++++++++++++++++")
print ("--- Starting Pipeline")

print ("--- Reading config")

with open("config.json") as file:
    config = json.load(file)

#Preparing DB params
print ("--- Preparing DB details config")
db_params = {
    'user': config['DB']['user'],
    'password': config['DB']['password'],
    'host': config['DB']['host'],
    'database': config['DB']['database']
}


sleep(10)

print ("--- Create Tables")
registory = Registory()
registory.create_tables(db_params)

#Extracting data files from Bucket
print ("--- Download Data")
extract = Extract(config)
data_files = extract.download_files()
# extract data from s3 bucket and load into local data dir

print ("--- Transformation Data")
transform = Transform(config)
for dfile in data_files:
    data = pd.read_csv(dfile, delimiter="\t")
    print ("--- Transforming & Loading --- %s"%dfile)
    transform.prepare_data(data)
    tobeload = {
        "article" : transform.get_transformations("article"),
        "user" : transform.get_transformations("user")
    }
    load = Load(config, tobeload)
    load.populate_with_data(db_params)
    extract.unlink(dfile)
    #print(tobeload)
    
print ("--- Finishing Pipeline")
print ("++++++++++++++++++++++")
