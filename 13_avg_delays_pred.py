from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

# Initialising the Spark environment
conf = SparkConf().setMaster("local[*]").setAppName("Average_Delays")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)

# Importing the previously created average delays data
training_data = sqc.read.format("com.databricks.spark.csv").options(header='false', inferschema='true')\
    .load("/Users/aausuman/Documents/Thesis/Average_Delays/part-00000-019ea495-c99e-41fc-a480-30936afb3ba4-c000.csv")

# Printing the schema
training_data.cache()
training_data.printSchema()

# Preparing the features and target variables
vectorAssembler = VectorAssembler(inputCols = ['CRIM', 'ZN', 'INDUS', 'CHAS', 'NOX', 'RM', 'AGE', 'DIS', 'RAD', 'TAX', 'PT', 'B', 'LSTAT'], outputCol = 'features')
training_df = vectorAssembler.transform(training_data)
training_df = training_df.select(['features', 'MV'])
