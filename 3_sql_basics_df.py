from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setMaster("local").setAppName("SQL_Basics_DF")
sc = SparkContext(conf = conf)
sqc = SQLContext(sc)

# filtering results using predefined functions
initial_df = sqc.read.csv("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")
initial_df.filter(initial_df['_c6'] == 'RD').show()

# filtering results using SQL Queries
# registering the above data-frame as a table
initial_df.registerTempTable("initial_df")
result_df = sqc.sql("select * from initial_df where _c6 == 'RD' ")
result_df.show()
