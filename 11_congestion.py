from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import lit
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


# Function to extract data-set's daily day and date
def date_and_day(df):
    first_timestamp = int(float(df.collect()[0]["Timestamp"]))/1000000
    readable_first_timestamp = t.ctime(first_timestamp)
    day = readable_first_timestamp[0:3]
    date = readable_first_timestamp[4:10]
    return day, date


# Creating an empty data-frame for storing busy lines data
relevant_fields = [StructField("LineID",IntegerType(), True), \
                   StructField("Congestion", IntegerType(), True), \
                   StructField("Date",StringType(), True), \
                   StructField("Day", StringType(), True), \
                   ]
schema = StructType(relevant_fields)
congestion_times_df = sqc.createDataFrame(sc.emptyRDD(), schema)

# Importing and cleaning our data-set
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df = cleaning(records_df)

# Getting day and date for this set of records
day, date = date_and_day(records_df)

# Remapping rdd as a PairRDD with LineID as key
records_keyLineID_rdd = records_df.rdd.map(lambda x: (int(str(x["LineID"])), [(int(str(x["LineID"])), \
                                                                               int(str(x["Congestion"])), \
                                                                               int(float(str(x["Timestamp"]))), \
                                                                               int(str(x["AtStop"])))]))

# Reducing (grouping) by the LineID
reduced_byLineID_list = records_keyLineID_rdd.reduceByKey(lambda a, b: a + b).collect()

# Iterating by LineID
for lineID in reduced_byLineID_list:
    within_lineID_rdd = sc.parallelize(lineID[1])
    within_lineID_df = within_lineID_rdd.toDF(schema=["LineID", "Congestion", "Timestamp", "AtStop"])
    within_lineID_df.registerTempTable("records")
    filtered_df = sqc.sql("select MIN(LineID) as LineID, Timestamp from records where Congestion = 1 and AtStop = 0 "
                          "group by Timestamp order by Timestamp")
    filtered_df = filtered_df.withColumn("Date", lit(date))
    filtered_df = filtered_df.withColumn("Day", lit(day))
    congestion_times_df = congestion_times_df.union(filtered_df)

#  Saving the congestion timings in a single csv file
congestion_times_df.coalesce(1).write.csv('/Users/aausuman/Documents/Thesis/Congestions')

