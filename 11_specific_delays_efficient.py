from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql import Window
import sys
import time as t

# Initialising the Spark environment
conf = SparkConf().setMaster("local[*]").setAppName("Specific_Delays")
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


# Importing and cleaning our data-set
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df = cleaning(records_df)

# Casting Delay Column as a Double type
records_df = records_df.withColumn("Delay", records_df["Delay"].cast(DoubleType()))

# Grouping and getting average delay in lines using groupby
# result_df = records_df.groupBy("LineID", "StopID").avg("Delay")
