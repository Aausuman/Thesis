from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import countDistinct

conf = SparkConf()
sc = SparkContext.getOrCreate(conf=conf)
sqc = SQLContext(sc)
files_list = dbutils.fs.ls("/FileStore/tables/")


# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


total_records = sc.emptyRDD()

for element in files_list:
    raw_records = sc.textFile(element.path)
    #     print(raw_records.count())
    total_records = total_records.union(raw_records)

records_rdd = total_records.map(pre_process)

records_df = records_rdd.toDF(
    schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", "VehicleJourneyID", "Operator",
            "Congestion", "Lon", "Lat", "Delay", "BlockID", "VehicleID", "StopID", "AtStop"])

# print(total_records.count())

# print(records_df.agg(countDistinct("Operator")))
records_df.select(countDistinct("Operator")).show()