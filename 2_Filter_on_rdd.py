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

def pre_process(line):
    fields = line.split(",")
    return fields


initial_rdd = sc.textFile("/Users/aausuman/Downloads/Thesis Dataset/siri.20130101.csv")
processed_rdd = initial_rdd.map(pre_process)
print("Number of records = " + str(processed_rdd.count()))

filtered_rdd = processed_rdd.filter(lambda x: 'RD' in x[6])
print("Number of records = " + str(filtered_rdd.count()))
