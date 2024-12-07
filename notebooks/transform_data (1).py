# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.catalog.clearCache()


# COMMAND ----------

# Check if the file exists
if dbutils.fs.ls("dbfs:/mnt/retaildatadl/raw_data/new_retail_data.csv"):
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("badRecordsPath", "dbfs:/mnt/retaildatadl/bad_records_path/") \
        .load("dbfs:/mnt/retaildatadl/raw_data/new_retail_data.csv")
    display(df)
else:
    print("File does not exist at the specified path.")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

df = df.withColumn('Year', year(col('Date')))
df = df.withColumn('Week', weekofyear(col('Date')))

holiday_mapping = {6: "Super Bowl", 36: "Labor Day", 47: "Thanksgiving", 51: "Christmas"}
df = df.withColumn(
    'Holiday',
    when(col('Week') == 6, "Super Bowl")
    .when(col('Week') == 36, "Labor Day")
    .when(col('Week') == 47, "Thanksgiving")
    .when(col('Week') == 51, "Christmas")
    .otherwise(None)
)

# Convert Spark DataFrame to Pandas for plotting
data = df.toPandas()

# Verify data
data.head()


# COMMAND ----------

weekly_sales_2010 = data[data['Year'] == 2010].groupby('Week')['Weekly_Sales'].mean()
weekly_sales_2011 = data[data['Year'] == 2011].groupby('Week')['Weekly_Sales'].mean()
weekly_sales_2012 = data[data['Year'] == 2012].groupby('Week')['Weekly_Sales'].mean()

plt.figure(figsize=(22, 8))
# Plot weekly sales
plt.plot(weekly_sales_2010.index, weekly_sales_2010.values, label='2010')
plt.plot(weekly_sales_2011.index, weekly_sales_2011.values, label='2011')
plt.plot(weekly_sales_2012.index, weekly_sales_2012.values, '*-g', label='2012')

# Customize x and y ticks
plt.xticks(np.arange(1, 53, step=1), fontsize=16)
plt.yticks(fontsize=16)

# Add labels and title
plt.xlabel('Week of Year', fontsize=20, labelpad=20)
plt.ylabel('Sales', fontsize=20, labelpad=20)
plt.title("Average Weekly Sales by Year", fontsize=24)

# Add legend
plt.legend(fontsize=20)

# Show plot
plt.show()

# COMMAND ----------

# Calculate total sales per store in Spark
top_stores = df.groupBy('Store').agg({'Weekly_Sales': 'sum'}) \
    .withColumnRenamed('sum(Weekly_Sales)', 'Total_Sales') \
    .orderBy(col('Total_Sales').desc())

# Convert to Pandas for easier plotting
top_stores_pd = top_stores.toPandas()

# Get the top 5 stores
top_5_stores = top_stores_pd['Store'][:5].tolist()

# Filter data for these stores
filtered_data = df.filter(col('Store').isin(top_5_stores))
filtered_data_pd = filtered_data.toPandas()
import plotly.express as px

# Create the boxplot
fig = px.box(
    filtered_data_pd, 
    x='Store', 
    y='Weekly_Sales', 
    color='Store', 
    title="Top 5 Store Distribution Analysis",
    template='plotly+presentation',
    color_discrete_sequence=px.colors.qualitative.Set1
)

# Customize the layout
fig.update_layout(
    title={
        'y': 0.92,
        'x': 0.5,
        'xanchor': 'center'
    },
    font=dict(
        family='Trebuchet MS',
        size=15
    ),
    legend=dict(
        y=0.85,
        x=0.85,
        xanchor='right',
        yanchor='top'
    ),
    yaxis_title="Sales",
    xaxis_title="Store",
    showlegend=False,
    xaxis=dict(
        type='category'
    )
)

# Order stores by total sales
fig.update_xaxes(categoryorder='total descending')

# Show the plot
fig.show()

# COMMAND ----------

from pyspark.sql.functions import col, when, avg
# Calculate average weekly sales by holiday
holiday_mean = df.groupBy("Week") \
    .agg(avg("Weekly_Sales").alias("Avg_Weekly_Sales")) \
    .withColumnRenamed("Week", "Holiday_Week")

# Map week numbers to holiday names using holiday_mapping
holiday_mean = holiday_mean.withColumn(
    "Holiday",
    when(col("Holiday_Week") == 6, "Super Bowl")
    .when(col("Holiday_Week") == 36, "Labor Day")
    .when(col("Holiday_Week") == 47, "Thanksgiving")
    .when(col("Holiday_Week") == 51, "Christmas")
    .otherwise(None)
)

# Filter rows with non-null holidays and sort by Avg_Weekly_Sales
holiday_mean = holiday_mean.filter(col("Holiday").isNotNull()) \
                           .orderBy(col("Avg_Weekly_Sales").desc())

# Convert to Pandas for visualization
holiday_mean_pd = holiday_mean.toPandas()

# Display the result
print(holiday_mean_pd)

# COMMAND ----------

plt.figure(figsize=(10, 6))
sns.barplot(x="Holiday", y="Avg_Weekly_Sales", data=holiday_mean_pd, palette="viridis")
plt.title("Average Weekly Sales by Holiday", fontsize=16)
plt.xlabel("Holiday", fontsize=14)
plt.ylabel("Average Weekly Sales", fontsize=14)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

import plotly.express as px

# Scatter plot
fig = px.scatter(
    data,
    x='Temperature',
    y='Weekly_Sales',
    title="Temperature vs Weekly Sales",
    color='Holiday_Flag',  # Corrected column name
    labels={'Temperature': 'Temperature', 'Weekly_Sales': 'Weekly Sales'},
    template='plotly+presentation'
)

# Update layout
fig.update_layout(
    title={
        'y': 0.92,
        'x': 0.5
    },
    font=dict(
        family='Trebuchet MS',
        size=15
    )
)

fig.show()


# COMMAND ----------


# Normalize the 'Temperature' column
temp_min = data['Temperature'].min()
temp_max = data['Temperature'].max()

data['Normalized_Temperature'] = (data['Temperature'] - temp_min) / (temp_max - temp_min)

# Verify normalization
data[['Temperature', 'Normalized_Temperature']].head()



# COMMAND ----------

data['Normalized_Temperature']=data['Normalized_Temperature'].fillna(data['Normalized_Temperature'].mean())

# After filling, check if there are any NaNs left
print(data['Normalized_Temperature'].isna().sum())

# COMMAND ----------

# MAGIC %pip install prophet
# MAGIC

# COMMAND ----------

# Ensure necessary columns are available for regressors
data['Unemployment'] = data['Unemployment'].fillna(method='ffill')  # Fill any missing values if needed
data['Fuel_Price'] = data['Fuel_Price'].fillna(method='ffill')  # Fill any missing values if needed

# Normalize 'Unemployment' and 'Fuel_Price' to improve model performance
data['Normalized_Unemployment'] = (data['Unemployment'] - data['Unemployment'].min()) / (data['Unemployment'].max() - data['Unemployment'].min())
data['Normalized_Fuel_Price'] = (data['Fuel_Price'] - data['Fuel_Price'].min()) / (data['Fuel_Price'].max() - data['Fuel_Price'].min())


# COMMAND ----------

from prophet import Prophet

# Rename columns for Prophet
data.rename(columns={'Date': 'ds', 'Weekly_Sales': 'y'}, inplace=True)

# Create a holidays DataFrame
holiday_df = data[['ds', 'Holiday']].dropna().rename(columns={'Holiday': 'holiday'})

# Remove duplicates in the holidays
holiday_df = holiday_df.drop_duplicates()


# COMMAND ----------

model = Prophet(holidays=holiday_df)
model.add_regressor('Normalized_Temperature')
model.add_regressor('Normalized_Unemployment')
model.add_regressor('Normalized_Fuel_Price')

# COMMAND ----------

import pandas as pd

# COMMAND ----------


model.fit(data[['ds', 'y', 'Normalized_Temperature', 'Normalized_Unemployment', 'Normalized_Fuel_Price']])




# COMMAND ----------

future = model.make_future_dataframe(periods=104, freq='W')




# COMMAND ----------

future = future.dropna(subset=['Normalized_Temperature'])

# COMMAND ----------


future['ds'] = pd.to_datetime(future['ds'])
data['ds'] = pd.to_datetime(data['ds'])
future = future.merge(data[['ds', 'Normalized_Temperature', 'Normalized_Unemployment', 'Normalized_Fuel_Price']], on='ds', how='left')




# COMMAND ----------

future['Normalized_Temperature'] = future['Normalized_Temperature'].fillna(future['Normalized_Temperature'].mean())
future['Normalized_Unemployment'] = future['Normalized_Unemployment'].fillna(future['Normalized_Unemployment'].mean())
future['Normalized_Fuel_Price'] = future['Normalized_Fuel_Price'].fillna(future['Normalized_Fuel_Price'].mean())

# Check for remaining NaN values (optional)
print(future.isna().sum())

# COMMAND ----------

forecast = model.predict(future)

# COMMAND ----------

# Filter for only future dates
forecast_future = forecast[forecast['ds'] > data['ds'].max()]


# COMMAND ----------


# View forecasted data
forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].tail()



# COMMAND ----------

# Plot the forecast
model.plot(forecast)
plt.title("Weekly Sales Forecast with Temperature, Unemployment, and Fuel Price")
plt.show()

# Plot the components (showing the individual effects)
model.plot_components(forecast)
plt.show()


# COMMAND ----------

# Convert the forecast DataFrame from Pandas to Spark DataFrame
forecast_spark = spark.createDataFrame(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])

# Save the Spark DataFrame to the data lake
forecast_spark.repartition(1).write.format("csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .mode("overwrite") \
    .save("dbfs:/mnt/retaildatadl/processed_data")
