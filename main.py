#!/usr/bin/env python
# coding: utf-8

# # Intro
# 
# This sample script loads mock data from fakeapi and inserts that to a table. Originally created to test smartregion deployment pipeline. If you are writing an ingestion script for the first time, this can be useful

# # Get some random data with a GET request
# Sample data:
# ```
#     [
#     {
#     "id": 1,
#     "street": "66336 Norene Islands Apt. 137",
#     "streetName": "Hessel Passage",
#     "buildingNumber": "8972",
#     "city": "Port Adeline",
#     "zipcode": "42103",
#     "country": "Serbia",
#     "county_code": "AX",
#     "latitude": -49.657204,
#     "longitude": 84.888176
#     }
#     ]
# ```

# In[24]:


import requests
import datetime

url = 'https://fakerapi.it/api/v1/addresses?_quantity=10000'

resp = requests.get(url).json()
data = resp['data']
for d in data:
    d['retrievedAt'] = datetime.datetime.now()


# # Create a SparkSession
# You need to create a SparkSession to allocate resources for the next tasks

# In[26]:


from pyspark.sql.session import SparkSession

spark = SparkSession.builder.getOrCreate()


# # Create a spark dataframe
# Create a spark dataframe from obtained data

# In[27]:


from pyspark.sql.types import (
    FloatType,
    StringType,
    DateType,
    StructType,
    StructField,
)
from datetime import datetime

schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("street", StringType(), True),
        StructField("streetName", StringType(), True),
        StructField("buildingNumber", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("country", StringType(), True),
        StructField("county_code", StringType(), True),
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("retrievedAt", DateType(), True),
    ]
)

df = spark.createDataFrame(data, schema)


# # Insert data to spark/iceberg table

# In[48]:


table_name = "randomBooks"

# you need to create a database before
db_name = 'test_db'
spark.sql("CREATE DATABASE IF NOT EXISTS " + db_name)

# create table if not exists and insert data
db_table = f'{db_name}.{table_name}'
if not spark.catalog.tableExists(db_table):
    df.writeTo(db_table).create()
    print("Table was not existing. Created from scratch")
else:
    df.writeTo(db_table).append()
    print("Table already exists")
print("Data added successfully")


# # Read table row number

# In[52]:


# show table
df = spark.table(db_table)

print(f'The table has {df.count()} rows')

