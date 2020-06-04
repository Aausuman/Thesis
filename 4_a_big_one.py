from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when

conf = SparkConf().setMaster("local").setAppName("MapReduce")
sc = SparkContext(conf = conf)
sqc = SQLContext(sc)

raw_records = sc.textFile("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")

# calculating the number of active bus lines for a given day
def pre_process(record):
    fields = record.split(",")
    return fields

def blank_as_null(x):
    return when(col(x) != "", col(x)).otherwise(None)


records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", "BlockID", "VehicleID", "StopID", "AtStop"])
records_df_without_empty = records_df.withColumn("LineID", blank_as_null("LineID"))

records_df_without_empty.registerTempTable("records")
bus_lines_df = sqc.sql("select distinct LineID from records")

for lines in bus_lines_df.rdd.collect():
    print(lines["LineID"])

# calculating how many bus movements take place for each line in this given day
bus_lines_rdd = records_df_without_empty.rdd.map(lambda x: x["LineID"])
bus_lines_rdd_count = bus_lines_rdd.map(lambda x: (x, 1))
bus_lines_rdd_totals = bus_lines_rdd_count.reduceByKey(lambda x, y: x + y)

for line in bus_lines_rdd_totals.collect():
    print("For lineID = " + str(line[0]) + " , number of bus movements on this day are = " + str(line[1]))

# Analysing which line has the most delay
