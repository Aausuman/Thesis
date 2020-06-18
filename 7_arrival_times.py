from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import time as t
import os

# Initialising the Spark environment (Local machine)
conf = SparkConf().setMaster("local[*]").setAppName("Stop_Locations")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)

# Initialising the Spark environment (Databricks cluster)
# conf = SparkConf()
# sc = SparkContext.getOrCreate(conf = conf)
# sqc = SQLContext(sc)

# Importing Dataset (Local machine)
raw_records_1 = sc.textFile("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")
raw_records_2 = sc.textFile("/Users/aausuman/Downloads/Thesis Dataset/siri.20130109.csv")
raw_records = raw_records_1.union(raw_records_2)

# Importing Dataset (Databricks cluster)
# raw_records = sc.textFile("/FileStore/tables/siri_20130101-aa346.csv")
# Template for reading and concatenating the entire month's data
# folder_path = "/Users/aausuman/Downloads/Thesis Dataset/"
# files = os.listdir(folder_path)
# raw_records = sc.emptyRDD()
# for file in files:
#     if file != '.DS_Store':
#         records = sc.textFile(folder_path + file)
#         raw_records = raw_records.union(records)

# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


# Importing our data-set
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                      "BlockID", "VehicleID", "StopID", "AtStop"])

# Removing records which contain null values in them
list_of_columns = records_df.columns
expr = ' and '.join('(%s != "null")' % col_name for col_name in list_of_columns)
records_df = records_df.filter(expr)

# Removing duplicate records
records_df = records_df.dropDuplicates()

# Remapping rdd as a PairRDD with LineID as key
records_keyLineID_rdd = records_df.rdd.map(lambda x: (int(str(x["LineID"])), [(int(str(x["LineID"])), \
                                                                               int(float(str(x["Timestamp"]))), \
                                                                               str(x["JourneyPatternID"]), \
                                                                               int(str(x["VehicleJourneyID"])), \
                                                                               int(str(x["StopID"])), \
                                                                               int(str(x["VehicleID"])), \
                                                                               str(x["Lat"]), \
                                                                               str(x["Lon"]), \
                                                                               int(str(x["AtStop"])), \
                                                                               int(str(x["Delay"])))]))

# Reducing (grouping) by the LineID
reduced_byLineID_rdd = records_keyLineID_rdd.reduceByKey(lambda a, b: a + b)
reduced_byLineID_list = reduced_byLineID_rdd.collect()

# Iterating by LineID
for element in reduced_byLineID_list:
    if element[0] == 747:
        within_lineID_rdd = sc.parallelize(element[1])
        within_lineID_keyJPID_rdd = within_lineID_rdd.map(lambda x: (x[2], [(x[0], x[1], x[2], x[3], x[4], \
                                                                             x[5], x[6], x[7], x[8], x[9])]))
        reduced_byJPID_rdd = within_lineID_keyJPID_rdd.reduceByKey(lambda a, b: a + b)
        reduced_byJPID_list = reduced_byJPID_rdd.collect()

        # Iterating by LineID's separate JourneyPatternID
        for element_2 in reduced_byJPID_list:
            if element_2[0] == '7470001':
                within_JPID_rdd = sc.parallelize(element_2[1])
                within_JPID_keyVJID_rdd = within_JPID_rdd.map(lambda x: (x[3], [(x[0], x[1], x[2], x[3], x[4], \
                                                                            x[5], x[6], x[7], x[8], x[9])]))
                reduced_byVJID_rdd = within_JPID_keyVJID_rdd.reduceByKey(lambda a, b: a + b)
                reduced_byVJID_list = reduced_byVJID_rdd.collect()

                # Iterating by LineID's separate JourneyPatternID's separate VehicleJourneyID
                for element_3 in reduced_byVJID_list:
                    if element_3[0] == 3493:
                        within_VJID_rdd = sc.parallelize(element_3[1])
                        within_VJID_keyStopID_rdd = within_VJID_rdd.map(lambda x: (x[4], [(x[0], x[1], x[2], x[3], \
                                                                                           x[4], x[5], x[6], x[7], \
                                                                                           x[8], x[9])]))
                        reduced_byStopID_rdd = within_VJID_keyStopID_rdd.reduceByKey(lambda a, b: a + b)
                        reduced_byStopID_list = reduced_byStopID_rdd.collect()

                        # Iterating by LineID's separate JourneyPatternID's separate VehicleJourneyID's separate StopID
                        for element_4 in reduced_byStopID_list:
                            within_StopID_rdd = sc.parallelize(element_4[1])
                            within_StopID_keyTimestamp_rdd = within_StopID_rdd.map(lambda x: (x[1], (x[0], x[1], \
                                                                                                     x[2], x[3], \
                                                                                                     x[4], x[5], \
                                                                                                     x[6], x[7], \
                                                                                                     x[8], x[9])))
                            sorted_byTimestamp_rdd = within_StopID_keyTimestamp_rdd.sortByKey().values()
                            sorted_byTimestamp_df = sorted_byTimestamp_rdd.toDF(schema = ["LineID", "Timestamp", \
                                                                                          "JourneyPatternID", \
                                                                                          "VehicleJourneyID", \
                                                                                          "StopID", "VehicleID", \
                                                                                          "Lat", "Lon", \
                                                                                          "AtStop", "Delay"])
                            sorted_byTimestamp_df.registerTempTable("records")
                            filtered_df = sqc.sql("select * from records where AtStop = 1")
                            filtered_df.show(100, False)
                            # This will give us properly structured dfs in each loop
                            # Each one will contain records of a Stop ID, within a VJID, within a JPID, within a LineID
                            # only records when the AtStop value is equal to 1
                            # We further need to think about the Timestamp of different days to compare Delays now
