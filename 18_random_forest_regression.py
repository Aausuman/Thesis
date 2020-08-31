import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import numpy as np

# Initialising the Spark environment
conf = SparkConf().setMaster("local[*]").setAppName("Average_Delays")
sc = SparkContext.getOrCreate(conf = conf)
sqc = SQLContext(sc)
raw_records = sc.textFile("/FileStore/tables/Pred_Algo_Full_Month.csv")


# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


# Importing and cleaning our data-set
records_rdd = raw_records.map(pre_process)
records_df = records_rdd.toDF(schema=["LineID", "Number_of_congestions", "Number_of_stops", "Average_Delay", \
                                      "Timestamp"])

# Converting spark dataframe to pandas dataframe
records_df_pd = records_df.toPandas()
records_df_pd['Date'] = pd.to_datetime(records_df_pd['Timestamp'], infer_datetime_format=True).dt.date
records_df_pd = records_df_pd.drop(columns=['Timestamp'])

# Remodelling the data
data = []
for index, row in records_df_pd.iterrows():
    data.append([row[1], row[2]])

x = np.array(data).astype(np.float64)
y = np.array(records_df_pd['Average_Delay']).astype(np.float64)

# Splitting into training and testing sets
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.30, random_state=42)

# Applying Random Forest Regression
RandomForestRegModel = RandomForestRegressor()
RandomForestRegModel.fit(x_train, y_train)

y_pred = RandomForestRegModel.predict(x_test)

mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)

# Printing the root mean squared error
print(rmse)
