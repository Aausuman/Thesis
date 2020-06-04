from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# The following code block is for a local machine running spark
conf = SparkConf().setMaster("local[*]").setAppName("Basic")
sc = SparkContext(conf = conf)
sqc = SQLContext(sc)

# The following commented code section is for an online actual cluster of spark
# conf = SparkConf()
# sc = SparkContext.getOrCreate(conf = conf)
# sqc = SQLContext(sc)

initial_rdd = sc.textFile("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")
count_1 = initial_rdd.count()

initial_df = sqc.read.csv("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")
count_2 = initial_df.count()

print("Number of records through RDD for Jan 1, 2013 = " + str(count_1))
print("Number of records through DF for Jan 1, 2013 = " + str(count_2))
