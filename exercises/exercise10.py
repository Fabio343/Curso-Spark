# ## Exercises

# In the following exercises we use *isotonic regression* to fit a monotonic
# function to the data.
from __future__ import print_function
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.ml.regression import *

# (1)  Import the `IsotonicRegression` class from the regression module.
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("regress_solucao").getOrCreate()
rides=spark.read.parquet("/duocar/clean/rides/")
# (2)  Create an instance of the `IsotonicRegression` class.  Use the same
# features and label that we used for our linear regression model.
instancia=IsotonicRegression(featuresCol='features',labelCol='duration')
print(instancia.explainParams())

# (3)  Fit the isotonic regression model on the train data.  It may take a
# minute for the fit to complete.  Note that this will produce an instance of
# the `IsotonicRegressionModel` class.
instancia2=instancia.fit(train)
type(instancia2)
# (4)  The model parameters are available in the `boundaries` and `predictions`
# attributes of the isotonic regression model.  Print these attributes.
instancia2.boundaries
instancia2.predictions
# (5) Apply the isotonic regression model to the train data using the `transform` method.
instancia2_transformada=instancia2.transform(train)
instancia2_transformada.show()
# (6) Use the `RegressionEvaluator` to compute the RMSE on the train data.

evaluator.evaluate(instancia2_transformada)
# (7) Repeat (5) and (6) on the test data.  Compare the RMSE for the isotonic regression
# model to the RMSE for the linear regression model.
instancia2_trans= instancia2.transform(test)
evaluator.evaluate(instancia2_trans)
# (8) Bonus: Plot the isotonic regression model.  In particular, plot the `predictions`
# attribute versus the `boundaries` attribute.  You must convert each attribute from 
# a Spark `DenseVector` to a NumPy array using the `toArray` method.
def plot_ir_model(predictions):
  pdf = predictions.sample(withReplacement=False, fraction=0.1, seed=34512).toPandas()
  plt.scatter("distance", "duration", data=pdf)
  plt.plot(ir_model.boundaries.toArray(), ir_model.predictions.toArray(), color="black")
  plt.xlabel("Distance (m)")
  plt.ylabel("Duration (s)")
  plt.title("Isotonic Regression Model")
plot_ir_model(predictions_test)
