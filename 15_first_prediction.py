from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import VectorAssembler
import time
import six

# Initialising the Spark environment
conf = SparkConf().setMaster("local[*]").setAppName("Average_Delays")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)
raw_records = sc.textFile("/Users/aausuman/Documents/Thesis/Dataset-Day1/siri.20130101.csv")


# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


# Function to remove records with null values and duplicate records
def cleaning(df):
    list_of_columns = df.columns
    expr = ' and '.join('(%s != "null")' % col_name for col_name in list_of_columns)
    df = df.filter(expr)
    df = df.dropDuplicates()
    return df


# Importing and cleaning our data-set, then defining there data types
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df = cleaning(records_df)
records_rdd = records_df.rdd.map(tuple)

fields = [StructField("Timestamp",TimestampType(), True),StructField("LineID", IntegerType(), True),\
          StructField("Direction", IntegerType(), True), StructField("JourneyPatternID", IntegerType(), True),\
          StructField("Timeframe",IntegerType(), True),StructField("VehicleJourneyID", IntegerType(), True),\
          StructField("Operator",StringType(), True),StructField("Congestion", IntegerType(), True),\
          StructField("Lon",DoubleType(), True),StructField("Lat", DoubleType(), True),\
          StructField("Delay",IntegerType(), True),StructField("BlockID", IntegerType(), True),\
          StructField("VehicleID",IntegerType(), True),StructField("StopID", IntegerType(), True),\
          StructField("AtStop",IntegerType(), True)]
schema = StructType(fields)
records_df = sqc.createDataFrame(records_rdd, schema)

# Performing introductory descriptive analysis in the data
records_df.describe()

# Checking correlation in the data
for i in records_df.columns:
    if not( isinstance(records_df.select(i).take(1)[0][0], six.string_types)):
        print( "Correlation to Delay for ", i, records_df.stat.corr('Delay',i))