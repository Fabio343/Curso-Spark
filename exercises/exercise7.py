# ## Exercises

# (1) Join the `rides` DataFrame with the `reviews` DataFrame.  Keep only those
# rides that have a review.
from pyspark.sql import SparkSession
import pandas as pd
pd.options.display.html.table_schema=True
spark = SparkSession.builder.master("local").appName("combine_solutions").getOrCreate()

# Read the clean data from HDFS:
rides = spark.read.parquet("/duocar/clean/rides/")
drivers = spark.read.parquet("/duocar/clean/drivers/")
reviews = spark.read.parquet("/duocar/clean/ride_reviews/")

rides_reviews=rides.join(reviews,rides.id==reviews.ride_id,'inner')
rides_reviews.toPandas()
rides_reviews=rides.join(reviews,rides.id==reviews.ride_id,'right_outer')
rides_reviews.toPandas()
rides_reviews=rides.join(reviews,rides.id==reviews.ride_id,'left_outer')
rides_reviews.toPandas()
rides_reviews=rides.crossJoin(reviews)
rides_reviews.toPandas()
# (2) How many drivers have not provided a ride?
# Get the driver IDs from `drivers` DataFrame:
id_from_drivers = drivers.select("id")

# Get the driver IDs from `rides` DataFrame:
id_from_rides = rides.select("driver_id").withColumnRenamed("driver_id", "id")

# Find lazy drivers using a left anti join:
lazy_drivers1 = id_from_drivers.join(id_from_rides, "id", "left_anti")
lazy_drivers1.count()
lazy_drivers1.orderBy("id").show(5)
