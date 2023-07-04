import requests
import json
from pyspark.sql import SparkSession
from pyspark import SQLContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col,avg
from pyspark.sql.window import Window
# Define a UDF to calculate the median of an array

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

spark = SparkSession \
    .builder \
    .appName("DataTransformation") \
    .getOrCreate() 


@udf
def calculate_median(array):
    sorted_array = sorted(array)
    n = len(sorted_array)
    if n % 2 == 0:
        return (sorted_array[n // 2 - 1] + sorted_array[n // 2]) / 2
    else:
        return sorted_array[n // 2]

# Step 1: Read the CSV files
data_fields = spark.read.options(header=True).csv("stocks/").withColumn("Symbol", F.input_file_name())
#data_fields_etfs=data_fields_etfs = spark.read.options(header=True).csv("etfs/")
symbol_data = spark.read.csv("symbols_valid_meta.csv", header=True)

print ("Merging to tables done")
# Step 2: Combine the data
combined_data= symbol_data.join(data_fields, on="Symbol")

# Step 3: Convert the dataset to Parquet
combined_data.write.parquet("combined_data.parquet")
print ("Merged temporary pq files with required data structure saved")

# Step 4: Calculate the moving average of the trading volume
window_spec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-30, 0)
combined_data = combined_data.withColumn("vol_moving_avg", avg(col("Volume")).over(window_spec))

# Step 5: Calculate the rolling median of Adj Close
#combined_data = combined_data.withColumn("adj_close_rolling_med", median(col("Adj Close")).over(window_spec))
# Calculate the rolling median of Adj Close using a rolling window of 30 days
window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-30, 0)
    
# Collect the values within the rolling window into an array
combined_data = combined_data.withColumn('adj_close_values', F.collect_list(col('Adj Close')).over(window_spec))
# Apply the UDF to calculate the rolling median
combined_data = combined_data.withColumn('adj_close_rolling_med', calculate_median(col('adj_close_values')))    
# Step 6: Save the modified dataset to Parquet
combined_data.write.parquet("processed_data.parquet")
print ("Tranformed data saved")
