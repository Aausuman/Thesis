from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
import matplotlib.pylab as plt

# Initialising the Spark environment
conf = SparkConf()
sc = SparkContext.getOrCreate(conf = conf)
sqc = SQLContext(sc)


# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


# Reading in the data
data = sc.textFile("/Users/aausuman/Documents/Thesis/Average_Delays_Databricks/Avg_Delays_full_month.csv")
data = data.map(pre_process)
data_df = data.toDF(schema=["LineID", "Avg_Delay", "Date", "Day"])

# Converting spark dataframe to pandas dataframe
data_df_pd = data_df.toPandas()

# Parsing the string date column into a datetime type
data_df_pd['Date'] = pd.to_datetime(data_df_pd['Date'], infer_datetime_format=True)
