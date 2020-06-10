from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import numpy as np

# Initialising the Spark environment (Local machine)
conf = SparkConf().setMaster("local[*]").setAppName("Stop_Locations")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)

# Initialising the Spark environment (Databricks cluster)
# conf = SparkConf()
# sc = SparkContext.getOrCreate(conf = conf)
# sqc = SQLContext(sc)

# Importing Dataset (Local machine)
raw_records = sc.textFile("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")

# Importing Dataset (Databricks cluster)
# raw_records = sc.textFile("/FileStore/tables/siri_20130101-aa346.csv")
# need to process all files on cluster as local machine won't be handle that much

# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields

# Function to handle the null values in a column and reform them to 0
def blank_as_null(x):
    return when(col(x) != "", col(x)).otherwise(0)


# Importing and processing our dataset
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe",\
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay",\
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df_without_empty = records_df.withColumn("LineID", blank_as_null("LineID"))

# Extracting departure patterns with respect to delay value
# If negative, it means that the bus is running before time

