# ## Exercises
# Create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("group_solutions").getOrCreate()

# Read the enhanced (joined) ride data from HDFS:
rides = spark.read.parquet("/duocar/joined/")

# Since we will be querying the `rides` DataFrame many times, let us persist
# it in memory to improve performance:
rides.persist()

# (1) Who are DuoCar's top 10 riders in terms of number of rides taken?
rides\
.filter(col('cancelled')==False)\
.groupBy('rider_id')\
.count()\
.orderBy(col('rider_id'))\
.show()
# (2) Who are DuoCar's top 10 drivers in terms of total distance driven?
from pyspark.sql.functions import *
rides\
.groupby('driver_id')\
.agg(sum('distance').alias('sum_distance'))\
.orderBy(col('sum_distance'))\
.show()
# (3) Compute the distribution of cancelled rides.
cancel=rides.groupBy('cancelled').count()
cancel.show()
# (4) Compute the distribution of ride star rating.
# When is the ride star rating missing?
rides\
.groupBy('star_rating')\
.count()\
.orderBy('star_rating')\
.show()

rides.crosstab('star_rating','cancelled')\
.orderBy('star_rating_cancelled')\
.show()
# (5) Compute the average star rating for each level of car service.
# Is the star rating correlated with the level of car service?
rides.groupBy('service')\
.mean('star_rating')\
.show()


rides.unpersist()