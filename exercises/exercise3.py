# ## Exercises

# (1) Create a new `SparkSession` and configure Spark to run locally with one
# thread.

# (2) Read the raw driver data from HDFS.

# (3) Examine the schema of the drivers DataFrame.

# (4) Count the number of rows of the drivers DataFrame.

# (5) Examine a few rows of the drivers DataFrame.

# (6) **Bonus:** Repeat exercises (2)-(5) with the raw rider data.

# (7) **Bonus:** Repeat exercises (2)-(5) with the raw ride review data.
# **Hint:** Verify the file format before reading the data.

# (8) Stop the SparkSession.


from pyspark.sql import SparkSession

spark= SparkSession.builder \
    .master("local") \
    .appName("spark") \
    .getOrCreate()
    
ride=spark.read.csv("/duocar/raw/drivers/", sep=",", header=True, inferSchema=True)
ride.schema
ride.count()
ride.printSchema()
ride.show(10)
ride.show(10,truncate=5)
ride.show(10,vertical=True)

riders = spark.read.csv("/duocar/raw/riders/", header=True, inferSchema=True)
riders.printSchema()
riders.count()
riders.head(5)

# (7) **Bonus:** Repeat exercises (2)-(5) with the raw ride review data.
# **Hint:** Verify the file format before reading the data.

reviews = spark.read.csv("/duocar/raw/ride_reviews/", sep="\t", header=False, inferSchema=True)
reviews.printSchema()
reviews.count()
reviews.show(5)


# ## Exercises

# (1) Read the raw driver data from HDFS to a Spark SQL DataFrame called
# `drivers`.

drives=spark.read.csv("/duocar/raw/drivers/", sep=",", header=True, inferSchema=True)

# (2) Examine the inferred schema.  Do the data types seem appropriate?
drives.printSchema()
# (3) Verify the integrity of the putative primary key `drivers.id`.
from pyspark.sql.functions import count, countDistinct
drivers.select(count("*"), count("id"), countDistinct("id")).show()

# (4) Determine the unique values of `drivers.vehicle_make`.
drivers.select("vehicle_make").distinct().count()
drivers.select("vehicle_make").distinct().show(30)

# (5) Compute basic summary statistics for `drivers.rides`.
drives.describe("rides").show()
# (6) Inspect `drivers.birth_date`.  What data type did Spark SQL infer?

drives.select('birth_date').show(10)
# (7) **Bonus:** Inspect additional columns of the `drivers` DataFrame.

# (8) **Bonus:** Inspect the raw rider data.
