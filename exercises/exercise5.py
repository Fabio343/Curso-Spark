from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("columns_solutions").getOrCreate()

# Read the raw data from HDFS:
rides = spark.read.csv("/duocar/raw/rides/", header=True, inferSchema=True)
drivers = spark.read.csv("/duocar/raw/drivers/", header=True, inferSchema=True)
riders = spark.read.csv("/duocar/raw/riders/", header=True, inferSchema=True)


# ## Exercises
# (1) Extract the hour of day and day of week from `rides.date_time`.
from pyspark.sql.functions import hour, dayofweek
rides\
    .withColumn('Hora', hour('date_time'))\
    .withColumn('Dia',dayofweek('date_time'))\
    .select("date_time", "Hora", "Dia")\
    .show(10)  
# (2) Convert `rides.duration` from seconds to minutes.
rides\
  .select('duration',round(col('duration')/60,1).alias('dura_minutos'))\
  .show()
# (3) Convert `rides.cancelled` to a Boolean column.
rides \
  .withColumn("cancelled", col("cancelled").cast("boolean")) \
  .select("cancelled") \
  .show(5)
# (4) Create a Boolean column named `five_star_rating` that is 1.0 if the ride
# received a five-star rating and 0.0 otherwise.
rides \
  .withColumn("5 estrelas", (col("star_rating") ==5).cast("double")) \
  .select("star_rating", "5 estrelas") \
  .show()
# (5) Create a new column containing the full name for each driver.
from pyspark.sql.functions import concat_ws
drivers \
  .withColumn("nome comple", concat_ws(" ", "first_name", "last_name")) \
  .select("first_name", "last_name", "nome comple") \
  .show(25)
# (6) Create a new column containing the average star rating for each driver.
drivers\
  .withColumn('estrelas',round(col('stars')/col('rides'),1))\
  .select('rides', 'stars', 'estrelas')\
  .show(25)  
# (7) Find the rider names that are most similar to `Brian`.  **Hint:** Use the
# [Levenshtein](https://en.wikipedia.org/wiki/Levenshtein_distance) function.
from pyspark.sql.functions import lit,levenshtein
riders\
.select('first_name')\
.distinct()\
.withColumn('distance',levenshtein(col('first_name'),lit('Brain')))\
.sort('distance')\
.show(30)

