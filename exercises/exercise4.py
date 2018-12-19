# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("dataframes").getOrCreate()
import pandas as pd
pd.options.display.html.table_schema=True
# Read the raw data from HDFS:

rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)

# (1) Replace the missing values in `rides.service` with the string `Car`.
rides\
   .fillna("Car",['service'])\
    .show(25)
# (2) Rename `rides.cancelled` to `rides.canceled`.
rides\
   .withColumnRenamed('cancelled','canceled').show(25)
# (3) Sort the `rides` DataFrame in descending order with respect to
# `driver_id` and ascending order with respect to `date_time`.
rides_sorted = rides.sort(rides.driver_id.desc(), "date_time")
rides_sorted.show()
  
# (4) Create an approximate 20% random sample of the `rides` DataFrame.
pd.options.display.html.table_schema=True
rides\
   .sample(withReplacement=False, fraction=0.2, seed=12345)\
   .toPandas()

# (5) Remove the driver's name from the `drivers` DataFrame.
drives = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)
pd.options.display.html.table_schema=True
drives\
   .drop('last_name','first_name')\
   .toPandas()
# (6) How many drivers have signed up?  How many female drivers have signed up?
# How many non-white, female drivers have signed up?
pd.options.display.html.table_schema=True
drives.filter(col('sex')=='female').toPandas()
drives.filter(col('ethnicity')!='White').filter(col('sex')=='female').toPandas()

spark.stop()