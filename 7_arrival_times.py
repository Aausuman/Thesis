from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import time as t

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


# Importing our data-set
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe",\
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay",\
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
# Removing records which contain null values in them
list_of_columns = records_df.columns
expr = ' and '.join('(%s != "null")' % col_name for col_name in list_of_columns)
records_df = records_df.filter(expr)

# Extracting arrival patterns with respect to delay value
# If negative, it means that the bus is running before time
# and vice versa

# Buses run on each line, with a vehicleJourneyId, and may have multiple stops in one "vehicle journey id"
# but only one vehicle ID (logically one bus will run throughout a journey, and may encompass multiple journeys)
# Let's plan to capture delay on each vehicle journey id's stops sequentially

records_keyLineID_rdd = records_df.rdd.map(lambda x: (int(str(x["LineID"])), [(int(str(x["LineID"])),\
                                                                             int(str(x["VehicleJourneyID"])),\
                                                                             int(str(x["StopID"])),\
                                                                             int(str(x["Delay"])))]))
reduced_byLineID_rdd = records_keyLineID_rdd.reduceByKey(lambda a,b: a+b)
reduced_byLineID_list = reduced_byLineID_rdd.collect()

for element in reduced_byLineID_list:
    print(element[0], len(element[1]))
    # within_lineID_rdd = sc.parallelize(element[1])
    # within_lineID_list = within_lineID_rdd.collect()
    # print(element[0], len(within_lineID_list))
