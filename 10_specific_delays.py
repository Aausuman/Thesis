from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

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


# Creating an empty data-frame for storing delay values stop wise of all lineIDs
relevant_fields = [StructField("LineID",IntegerType(), True), \
                   StructField("JourneyPatternID", StringType(), True), \
                   StructField("BlockID", StringType(), True), \
                   StructField("VehicleJourneyID", IntegerType(), True), \
                   StructField("StopID", IntegerType(), True), \
                   StructField("Delay", IntegerType(), True), \
                   StructField("Timestamp", IntegerType(), True), \
                   StructField("AtStop", IntegerType(), True)]
schema = StructType(relevant_fields)
stop_wise_delay_df = sqc.createDataFrame(sc.emptyRDD(), schema)

# Importing and cleaning our data-set
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df = cleaning(records_df)

# Remapping rdd as a PairRDD with LineID as key
records_keyLineID_rdd = records_df.rdd.map(lambda x: (int(str(x["LineID"])), [(int(str(x["LineID"])), \
                                                                               str(x["JourneyPatternID"]), \
                                                                               str(x["BlockID"]), \
                                                                               int(str(x["VehicleJourneyID"])), \
                                                                               int(str(x["StopID"])), \
                                                                               int(float(str(x["Timestamp"]))), \
                                                                               int(str(x["AtStop"])), \
                                                                               int(str(x["Delay"])))]))

# Reducing (grouping) by the LineID
reduced_byLineID_list = records_keyLineID_rdd.reduceByKey(lambda a, b: a + b).collect()

# Iterating by LineID
for i in reduced_byLineID_list:
    if i[0] == 747:
        within_lineID_rdd = sc.parallelize(i[1])
        records_keyJPID_rdd = within_lineID_rdd.map(lambda x: (x[1], [(x[0], x[1], x[2], x[3], x[4], x[5], x[6], \
                                                                       x[7])]))
        reduced_byJPID_list = records_keyJPID_rdd.reduceByKey(lambda a, b: a + b).collect()
        for j in reduced_byJPID_list:
            within_JPID_rdd = sc.parallelize(j[1])
            records_keyBID_rdd = within_JPID_rdd.map(lambda x: (x[2], [(x[0], x[1], x[2], x[3], x[4], x[5], x[6], \
                                                                        x[7])]))
            reduced_byBID_list = records_keyBID_rdd.reduceByKey(lambda a, b: a + b).collect()
            for k in reduced_byBID_list:
                within_BID_rdd = sc.parallelize(k[1])
                records_keyVJID_rdd = within_BID_rdd.map(lambda x: (x[3], [(x[0], x[1], x[2], x[3], x[4], x[5], \
                                                                            x[6], x[7])]))
                reduced_byVJID_list = records_keyVJID_rdd.reduceByKey(lambda a, b: a + b).collect()
                for l in reduced_byVJID_list:
                    within_VJID_rdd = sc.parallelize(l[1])
                    records_keyStopID_rdd = within_VJID_rdd.map(lambda x: (x[4], [(x[0], x[1], x[2], x[3], x[4], \
                                                                                   x[5], x[6], x[7])]))
                    reduced_byStopID_list = records_keyStopID_rdd.reduceByKey(lambda a, b: a + b).collect()
                    for m in reduced_byStopID_list:
                        within_StopID_rdd = sc.parallelize(m[1])
                        records_keyTimestamp_rdd = within_StopID_rdd.map(lambda x: (x[5], (x[0], x[1], x[2], x[3], \
                                                                                           x[4], x[5], x[6], x[7])))
                        sorted_byTimestamp_rdd = records_keyTimestamp_rdd.sortByKey().values()
                        sorted_byTimestamp_df = sorted_byTimestamp_rdd.toDF(schema=["LineID", "JourneyPatternID", \
                                                                                    "BlockID", "VehicleJourneyID", \
                                                                                    "StopID", "Timestamp", \
                                                                                    "AtStop", "Delay"])
                        sorted_byTimestamp_df.registerTempTable("records")
                        filtered_df = sqc.sql("select * from records where AtStop = 1 limit 1")
                        stop_wise_delay_df = stop_wise_delay_df.union(filtered_df)

#  Saving the specific delays in a single csv file
stop_wise_delay_df.coalesce(1).write.csv('/Users/aausuman/Documents/Thesis/Specific_Delays')
