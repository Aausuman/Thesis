from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
import time

conf = SparkConf().setMaster("local[*]").setAppName("Stop_Locations")
sc = SparkContext(conf = conf)
sqc = SQLContext(sc)

def pre_process(record):
    fields = record.split(",")
    return fields

def blank_as_null(x):
    return when(col(x) != "", col(x)).otherwise(0)


raw_records = sc.textFile("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe",\
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay",\
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df_without_empty = records_df.withColumn("LineID", blank_as_null("LineID"))

# Remapping records into an RDD by LineID as Key
filtered_data_rdd = records_df_without_empty.rdd.map(lambda x: (str(x["LineID"]),\
                                                                (str(x["LineID"]),\
                                                                 time.ctime(int(str(x["Timestamp"]))/1000000),\
                                                                 str(x["JourneyPatternID"]),\
                                                                 int(str(x["VehicleID"])),\
                                                                 int(str(x["VehicleJourneyID"])),\
                                                                 int(str(x["Delay"])),\
                                                                 str(x["Lon"]), str(x["Lat"]), str(x["StopID"]),\
                                                                 int(str(x["AtStop"])))))

# Grouping those records on LineID
grouped_by_lineID = filtered_data_rdd.groupByKey().mapValues(list)
results_1 = grouped_by_lineID.collect()

for result_1 in results_1:
    # Creating RDD for each LineID inside this for loop
    lineID_rdd = sc.parallelize(result_1[1])

    # Remapping this RDD by StopID as key
    lineID_rdd_2 = lineID_rdd.map(lambda x: (str(x[8]),\
                                                    (str(x[0]), str(x[8]),\
                                                     str(x[1]),\
                                                     str(x[2]),\
                                                     int(str(x[3])),\
                                                     int(str(x[4])), int(str(x[5])), str(x[6]),\
                                                     str(x[7]), int(str(x[9])))))

    # Grouping those records on StopID
    grouped_by_stopID = lineID_rdd_2.groupByKey().mapValues(list)
    results_2 = grouped_by_stopID.collect()

    for result_2 in results_2:
        # Creating RDD for each StopID within the original loop's LineID inside this for loop
        stopID_rdd = sc.parallelize(result_2[1])

        # These lines below, will print records for each stopID in turn for each LineID
        # if result_1[0] == '747':
        #     for x in result_2[1]:
        #         print(x)

        # We can gather a stop's exact coordinates, when a record shows that a bus is stopped at that location
        # And that bus location coordinates will coincide with the Stop's GPS coordinates
        # Sometimes there may be stops on a line where no bus of that line stops,
        # which means that stop may also lie on another line, so we need no worry about it at this time)

        # Converting the stopID rdd into a dataframe
        stopID_df = stopID_rdd.toDF(schema=["LineID", "StopID", "Time", "JourneyPatternID", "VehicleID",\
                                            "VehicleJourneyID",\
                                            "Delay", "Lon", "Lat",\
                                            "AtStop"])

        stopID_df.registerTempTable("records")
        stop_coordinates_df = sqc.sql("select LineID, StopID, Lon, Lat, AtStop \
                                            from records where AtStop = 1 limit 1")

        stop_coordinates_df.show()
        # The above dataframe will have the coordinates of each stop in each line

        # Further planning to store these coordinates separately and then use BaseMap class to produce a plot
        # more to come ahead......
        # ........................