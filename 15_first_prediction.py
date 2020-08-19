from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler

# Initialising the Spark environment
conf = SparkConf().setMaster("local[*]").setAppName("Average_Delays")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)
raw_records = sc.textFile("/Users/aausuman/Documents/Thesis/Average_Delays/avg_delay.csv")


# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


# Importing our previously created average delay dataset
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df = cleaning(records_df)

# Prepare data for a machine learning model
vectorAssembler = VectorAssembler(inputCols = ['CRIM', 'ZN', 'INDUS', 'CHAS', 'NOX', 'RM', 'AGE', 'DIS', 'RAD', 'TAX', 'PT', 'B', 'LSTAT'], outputCol = 'features')
vhouse_df = vectorAssembler.transform(house_df)
vhouse_df = vhouse_df.select(['features', 'MV'])
vhouse_df.show(3)