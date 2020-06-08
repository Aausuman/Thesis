from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, when
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import numpy as np

# Initialising the Spark environment (Local machine)
conf = SparkConf().setMaster("local[*]").setAppName("Stop_Locations")
sc = SparkContext(conf=conf)
sqc = SQLContext(sc)

# Initialising the Spark environment (Databricks cluster)
# conf = SparkConf()
# sc = SparkContext.getOrCreate(conf = conf)
# sqc = SQLContext(sc)

# preparing schema for the data-frame of stops coordinates we created in the previous python file
fields = [StructField("LineID", StringType(), True), StructField("StopID", StringType(), True), \
          StructField("Lon", StringType(), True), StructField("Lat", StringType(), True)]
schema = StructType(fields)
stops_coordinates_df = sqc.read.csv("/Users/aausuman/Documents/Thesis/StopID_Coordinates/stops_coordinates.csv", schema)

# Casting the Longitude and Latitude columns as double values rather than string
stops_coordinates_df = stops_coordinates_df.withColumn("Lon", col("Lon").cast('double')). \
    withColumn("Lat", col("Lat").cast('double'))

# Extracting min and max values for both Longitude and Latitude to get the boundary box for our map
min_lon = stops_coordinates_df.agg({"Lon": "min"}).collect()[0][0]
max_lon = stops_coordinates_df.agg({"Lon": "max"}).collect()[0][0]

min_lat = stops_coordinates_df.agg({"Lat": "min"}).collect()[0][0]
max_lat = stops_coordinates_df.agg({"Lat": "max"}).collect()[0][0]

boundary_box = (min_lon, max_lon, min_lat, max_lat)

# now we plot
image = plt.imread('/Users/aausuman/Documents/Thesis/Dublin-map.png')
fig, ax = plt.subplots(figsize=(8, 7))
ax.scatter(stops_coordinates_df.select("Lon").rdd.flatMap(lambda x: x).collect(),\
           stops_coordinates_df.select("Lat").rdd.flatMap(lambda x: x).collect(),\
           zorder=1, alpha=0.2, c='b', s=10)
ax.set_title('Plotting Bus Stops on Dublin Bus Transport System')
ax.set_xlim(boundary_box[0], boundary_box[1])
ax.set_ylim(boundary_box[2], boundary_box[3])
ax.imshow(image, zorder=0, extent=boundary_box, aspect='equal')
plt.show()
