from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
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


# Creating an empty data-frame for storing delay values stop wise of all lineIDs
relevant_fields = [StructField("LineID",IntegerType(), True), \
                   StructField("StopID", IntegerType(), True), \
                   StructField("Average Reaching Delay", IntegerType(), True), \
                   StructField("Date", StringType(), True), \
                   StructField("Day", StringType(), True), \
                   ]
schema = StructType(relevant_fields)
stop_wise_delay_df = sqc.createDataFrame(sc.emptyRDD(), schema)

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
                                                                               int(str(x["StopID"])), \
                                                                               int(float(str(x["Timestamp"]))), \
                                                                               int(str(x["AtStop"])), \
                                                                               int(str(x["Delay"])))]))

# Reducing (grouping) by the LineID
reduced_byLineID_list = records_keyLineID_rdd.reduceByKey(lambda a, b: a + b).collect()

# Iterating by LineID
for lineID in reduced_byLineID_list:
    if lineID[0] == 747:
        within_lineID_rdd = sc.parallelize(lineID[1])
        records_keyStopID_rdd = within_lineID_rdd.map(lambda x: (x[1], [(x[0], x[1], x[2], x[3], x[4])]))
        reduced_byStopID_list = records_keyStopID_rdd.reduceByKey(lambda a, b: a + b).collect()
        for stopID in reduced_byStopID_list:
            within_StopID_rdd = sc.parallelize(stopID[1])
            within_StopID_df = within_StopID_rdd.toDF(schema=["LineID", "StopID", "Timestamp", "AtStop", "Delay"])
            within_StopID_df.registerTempTable("records")
            filtered_df = sqc.sql("with temp as"
                                  "("
                                  "select row_number()over(order by Timestamp ASC) as row, *"
                                  "from records"
                                  ") "
                                  "select t2.LineID, t2.StopID, t2.Timestamp, t2.AtStop, t2.Delay "
                                  "from temp t1 "
                                  "INNER JOIN temp t2 "
                                  "ON t1.row = t2.row+1 "
                                  "where t2.AtStop = 1 and t1.AtStop = 0")
            if len(filtered_df.head(1)) > 0:
                average_reaching_delay = average_of_column(filtered_df, 'Delay')
                this_lineID_row = sc.parallelize([(lineID[0], stopID[0], average_reaching_delay, date, day)]).toDF(schema=["LineID", \
                                                                                                 "StopID", \
                                                                                                 "Average Reaching Delay", \
                                                                                                 "Date",
                                                                                                 "Day"])
                stop_wise_delay_df = stop_wise_delay_df.union(this_lineID_row)

#  Saving the specific delays in a single csv file
stop_wise_delay_df.coalesce(1).write.csv('/Users/aausuman/Documents/Thesis/Specific_Delays')
