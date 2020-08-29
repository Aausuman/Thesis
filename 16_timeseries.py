from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
import matplotlib.pylab as plt
from sklearn.metrics import mean_squared_error
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
from statsmodels.tsa.arima_model import ARIMA
import warnings

# Initialising the Spark environment
conf = SparkConf()
sc = SparkContext.getOrCreate(conf = conf)
sqc = SQLContext(sc)


# Function to extract fields from our comma separated data files
def pre_process(record):
    fields = record.split(",")
    return fields


# Reading in the data
data = sc.textFile("/Users/aausuman/Documents/Thesis/Average_Delays_Databricks/Average_Delays_747.csv")
data = data.map(pre_process)
data_df = data.toDF(schema=["LineID", "Avg_Delay", "Date"])

# Converting spark dataframe to pandas dataframe
data_df_pd = data_df.toPandas()

# Parsing the input columns, and dropping the LineID column as it will remain the same for each row
data_df_pd['Date'] = pd.to_datetime(data_df_pd['Date'], infer_datetime_format=True).dt.date
data_df_pd['Avg_Delay'] = pd.to_numeric(data_df_pd['Avg_Delay'])
data_df_pd = data_df_pd.drop(columns=['LineID'])

# Setting index on the Date column
indexed_data = data_df_pd.set_index(['Date'])
indexed_data.plot()

# Since the data is non stationary
# We have to determine the rolling statistics like mean and standard deviation
rol_mean = indexed_data.rolling(window=7).mean()
rol_std = indexed_data.rolling(window=7).std()

# plotting these rolling statistics
orig = plt.plot(indexed_data, color='blue', label='Original')
mean = plt.plot(rol_mean, color='red', label='Rolling Mean')
std = plt.plot(rol_std, color='black', label='Rolling Std')
plt.legend(loc='best')
plt.title('Rolling Mean & Std Deviation')
plt.show()

# Now we make the base naive model - (current value depends on the previous model)
indexed_data_base = pd.concat([indexed_data, indexed_data.shift(1)], axis=1)
indexed_data_base.columns = ['Actual_Avg_Delay', 'Forecast_Avg_Delay']
indexed_data_base.dropna(inplace=True)

# Calculating the error in forecasting above
indexed_data_err = mean_squared_error(indexed_data_base.Actual_Avg_Delay, indexed_data_base.Forecast_Avg_Delay)

# The above value does not look related to the data at hand, which is because we performed a mean SQUARED function
# We need to root it, in order to bring it back to our scale
np.sqrt(indexed_data_err)

# Now we plot the acf and pacf graphs to see the p value and q value
plot_acf(indexed_data)
plot_pacf(indexed_data)
# Since only 0th point is above the curve in both plots
# q = 0
# p = 0
# The difference can vary between 0 to 2
# d = 0-2

# Splitting the dataset into training and test sets
indexed_data_train = indexed_data[0:22]
indexed_data_test = indexed_data[22:31]

# Applying the ARIMA Model
indexed_data_model = ARIMA(indexed_data_train, order=(0,1,0))
indexed_data_model_fit = indexed_data_model.fit()
aic_value = indexed_data_model_fit.aic
indexed_data_forecast = indexed_data_model_fit.forecast(steps=9)[0]
root_mse_arima = np.sqrt(mean_squared_error(indexed_data_test, indexed_data_forecast))

# Parameter tuning the p and q values for test of better fit
p_values = range(0,5)
d_values = range(0,3)
q_values = range(0,5)

# Ignoring warnings for better readability
warnings.filterwarnings("ignore")

# Applying multiple models and seeing which has lowest RMSE
for p in p_values:
    for d in d_values:
        for q in q_values:
            order = (p,d,q)
            train, test = indexed_data[0:22], indexed_data[22:31]
            predictions = list()
            for i in range(len(test)):
                try:
                    model = ARIMA(train, order)
                    model_fit = model.fit(disp=0)
                    pred_y = model_fit.forecast()[0]
                    predictions.append(pred_y)
                    error = np.sqrt(mean_squared_error(test, predictions))
                    print("ARIMA%s RMSE = % .2f"%(order, error))
                except:
                    continue

# The results obtained from above suggest that the naive base model was better than ARIMA
# Hence, autoregressive model might not be the way to go in this
# And transformation of data points into logarithmic form is also not the way to go
# because of the negative nature of the average delay values in certain cases.
# There might not be that strong a trend to perform a time series analysis in this data.
