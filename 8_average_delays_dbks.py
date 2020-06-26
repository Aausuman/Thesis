from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
import time as t

# Initialising the Spark environment
conf = SparkConf()
sc = SparkContext.getOrCreate(conf = conf)
sqc = SQLContext(sc)
files_list = dbutils.fs.ls("/FileStore/tables/")
# raw_records = sc.textFile("/FileStore/tables/siri_20130101-aa346.csv")
# raw_records have been iteratively instantiated below

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

# Function to get the average of a column's values
def average_of_column(df, column):
    total = df.select(F.sum(column)).collect()[0][0]
    no_of_records = df.count()
    average = total/no_of_records
    return average

# Function to extract data-set's daily day and date
def date_and_day(df):
    first_timestamp = int(float(df.collect()[0]["Timestamp"]))/1000000
    readable_first_timestamp = t.ctime(first_timestamp)
    day = readable_first_timestamp[0:3]
    date = readable_first_timestamp[4:10]
    return day, date


# Creating an empty data-frame for storing average delay values of all lineIDs
relevant_fields = [StructField("LineID",IntegerType(), True),StructField("Average Delay", IntegerType(), True),\
            StructField("Date", StringType(), True), StructField("Day", StringType(), True)]
schema = StructType(relevant_fields)
average_delay_df = sqc.createDataFrame(sc.emptyRDD(), schema)

# Iterating day wise and Importing and cleaning our data-set
for element in files_list[:8]:
    raw_records = sc.textFile(element.path)
    records_rdd = raw_records.map(pre_process)
    records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                          "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                          "BlockID", "VehicleID", "StopID", "AtStop"])
    # Calling the above defined cleaning function for preprocessing the data
    records_df = cleaning(records_df)

    # Calling the above defined function and getting day and date for this set of records
    day, date = date_and_day(records_df)

    # Remapping rdd as a PairRDD with LineID as key
    records_keyLineID_rdd = records_df.rdd.map(lambda x: (int(str(x["LineID"])), [(int(str(x["LineID"])), \
                                                                                   int(float(str(x["Timestamp"]))), \
                                                                                   int(str(x["Delay"])))]))

    # Reducing (grouping) by the LineID
    reduced_byLineID_list = records_keyLineID_rdd.reduceByKey(lambda a, b: a + b).collect()

    # Iterating by LineID
    for element in reduced_byLineID_list:
        within_lineID_df = sc.parallelize(element[1]).toDF(schema =["LineID", "Timestamp", "Delay"])
        average_delay_for_day = average_of_column(within_lineID_df, 'Delay')
        this_lineID_row = sc.parallelize([(element[0], average_delay_for_day, date, day)]).toDF(schema = ["LineID", \
                                                                                                          "Average Delay", \
                                                                                                          "Date", "Day"])
        average_delay_df = average_delay_df.union(this_lineID_row)

# Saving the average delays in a single csv file (Databricks cluster)
average_delay_df.coalesce(1).write.csv('/FileStore/Average_Delays')