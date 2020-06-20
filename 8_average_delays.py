from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
import time as t

# Initialising the Spark environment (Local machine)
conf = SparkConf().setMaster("local[*]").setAppName("Average_Delays")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)

# Initialising the Spark environment (Databricks cluster)
# conf = SparkConf()
# sc = SparkContext.getOrCreate(conf = conf)
# sqc = SQLContext(sc)

# Importing Dataset (Local machine)
raw_records = sc.textFile("/Users/aausuman/Documents/Thesis/Dataset-Day1/siri.20130101.csv")

# Importing Dataset (Databricks cluster)
# files_list = dbutils.fs.ls("/FileStore/tables/")
# raw_records = sc.emptyRDD()
#
# for element in files_list:
#     records = sc.textFile(element.path)
#     raw_records = raw_records.union(records)

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
def average_delay(df, column):
    total = df.select(F.sum(column)).collect()[0][0]
    no_of_records = within_lineID_rdd.count()
    average = total/no_of_records
    return average


# Importing and cleaning our data-set
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["Timestamp", "LineID", "Direction", "JourneyPatternID", "Timeframe", \
                                      "VehicleJourneyID", "Operator", "Congestion", "Lon", "Lat", "Delay", \
                                      "BlockID", "VehicleID", "StopID", "AtStop"])
records_df = cleaning(records_df)

# Remapping rdd as a PairRDD with LineID as key
records_keyLineID_rdd = records_df.rdd.map(lambda x: (int(str(x["LineID"])), [(int(str(x["LineID"])), \
                                                                               int(float(str(x["Timestamp"]))), \
                                                                               int(str(x["Delay"])))]))

# Reducing (grouping) by the LineID
reduced_byLineID_rdd = records_keyLineID_rdd.reduceByKey(lambda a, b: a + b)
reduced_byLineID_list = reduced_byLineID_rdd.collect()

# Iterating by LineID
for element in reduced_byLineID_list:
    if element[0] == 747:
        within_lineID_rdd = sc.parallelize(element[1])
        within_lineID_df = within_lineID_rdd.toDF(schema =["LineID", "Timestamp", "Delay"])
        average_delay_for_day = average_delay(within_lineID_df, 'Delay')
        print(str(average_delay_for_day) + " is the average delay for line ID = " + str(element[0]) + \
              "on date = " + )
