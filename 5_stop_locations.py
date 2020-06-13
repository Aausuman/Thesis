from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
from pyspark.sql.types import *
import time

# Initialising the Spark environment (Local machine)
conf = SparkConf().setMaster("local[*]").setAppName("Stop_Locations")
sc = SparkContext(conf = conf)
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

# Creating an empty data-frame for storing coordinates of all the stops within a lineID
relevant_fields = [StructField("LineID",StringType(), True),StructField("StopID", StringType(), True),\
            StructField("Lon", StringType(), True), StructField("Lat", StringType(), True)]
schema = StructType(relevant_fields)
all_coordinates_df = sqc.createDataFrame(sc.emptyRDD(), schema)

# Remapping records into an RDD by LineID as Key
filtered_data_rdd = records_df_without_empty.rdd.map(lambda x: (str(x["LineID"]), (str(x["LineID"]),\
                                                                        time.ctime(int(str(x["Timestamp"]))/1000000),\
                                                                        str(x["JourneyPatternID"]),\
                                                                        int(str(x["VehicleID"])),\
                                                                        int(str(x["VehicleJourneyID"])),\
                                                                        int(str(x["Delay"])),\
                                                                        str(x["Lon"]), str(x["Lat"]),\
                                                                        str(x["StopID"]),\
                                                                        int(str(x["AtStop"])))))

# Grouping those records on LineID
grouped_by_lineID = filtered_data_rdd.groupByKey().mapValues(list)
results_1 = grouped_by_lineID.collect()

for result_1 in results_1:
    # Just doing for 1 line, as local machine won't be able to handle
    # Will do for all lines on the DataBricks cluster
    if result_1[0] == '747':
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
            stop_coordinates_df = sqc.sql("select LineID, StopID, Lon, Lat \
                                                from records where AtStop = 1 limit 1")

            # Appending these obtained stopIDs into our repository data-frame of stopIDs
            all_coordinates_df = all_coordinates_df.union(stop_coordinates_df)

# Saving the stopID coordinates in a single csv file
all_coordinates_df.coalesce(1).write.csv('/Users/aausuman/Documents/Thesis/StopID_Coordinates')
