from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create a SparkSession:
spark = SparkSession.builder.master("local").appName("explore_solutions").getOrCreate()

# Load the enhanced ride data from HDFS:
rides_sdf = spark.read.parquet("/duocar/joined_all")

# (1) Look for variables that might help us predict ride duration.

pd.options.display.html.table_schema=True
rides_sdf\
.sample(withReplacement=False,fraction=0.01,seed=123544)\
.toPandas()

variaveis=rides_sdf.groupBy(dayofweek('date_time').alias('day_of_week'))\
.agg(count('distance'),mean('distance'))\
.orderBy('day_of_week')
variaveis2=variaveis.toPandas()
sns.barplot(x='day_of_week',y='avg(distance)',data=variaveis2)

# (2) Look for variables that might help us predict ride rating.

rides_sdf\
.sample(withReplacement=False,fraction=0.01,seed=1511151)\
.toPandas()

rati=rides_sdf\
.groupBy('vehicle_elite')\
.agg(count('star_rating'),mean('star_rating'))\
.orderBy('vehicle_elite')\
.toPandas()

sns.barplot(x='vehicle_elite',y='avg(star_rating)',data=rati)