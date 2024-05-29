from pyspark.sql.types import (
    DoubleType,
    FloatType,
    LongType,
    StructType,
    StructField,
    StringType,
)
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext("local")
spark = SparkSession(sc)

table_name = "nyc.icebergtaxi"

# create table if not exists
if spark.catalog.tableExists(table_name):
    print("Table already exists")
else:
    schema = StructType(
        [
            StructField("vendor_id", LongType(), True),
            StructField("trip_id", LongType(), True),
            StructField("trip_distance", FloatType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
        ]
    )

    df = spark.createDataFrame([], schema)
    df.writeTo(table_name).create()  # need to use iceberg ?
    print("Table created")

# add row to the table
schema = spark.table(table_name).schema
now = datetime.now()
data = [
    (1, now.hour, float(now.minute), float(now.second), "Y"),
]
df = spark.createDataFrame(data, schema)
df.writeTo(table_name).append()
print("Row added")

# show table
df = spark.table(table_name)
df.show()
