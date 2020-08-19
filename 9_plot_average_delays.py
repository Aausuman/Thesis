from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import matplotlib.pyplot as plt

# Initialising the Spark environment (Local machine)
conf = SparkConf().setMaster("local[*]").setAppName("Average_Delays")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)
raw_records = sc.textFile("/Users/aausuman/Documents/Thesis/Average_Delays_Databricks/Avg_Delays.csv")

# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


# Importing and processing our dataset
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["LineID", "Avg_Delay", "Date", "Day"])

# Remapping records into an RDD by LineID as Key
mapped_records_rdd = records_df.rdd.map(lambda x: (int(x["LineID"]), [(int(x["LineID"]), float(x["Avg_Delay"]), \
                                                                  str(x["Date"]), str(x["Day"]))]))

# Grouping by LineID
reduced_byLineID_list = mapped_records_rdd.reduceByKey(lambda a, b: a + b).collect()

for lineID in reduced_byLineID_list:
    if lineID[0] == 151:
        x_axis = [day[2] for day in lineID[1]]
        y_axis = [day[1] for day in lineID[1]]
        # print(y_axis)
        # print(x_axis)

        plt.title("Average Delay of line ID = " + str(lineID[0]))
        # plt.scatter(x_axis, y_axis, color='darkblue', marker='x')
        plt.plot(y_axis)

        plt.xlabel("Days")
        plt.ylabel("Average Delay")

        plt.grid(True)
        plt.legend()

        plt.show()

