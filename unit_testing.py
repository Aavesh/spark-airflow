import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
from pyspark.sql.window import Window
from my_code import calculate_median  # Replace "my_code" with the filename containing the code

# Initialize Spark session
spark = SparkSession.builder.appName("unit_testing").getOrCreate()

# Load test data
test_data = [
    # Sample test data for the DataFrame
    # Add more test data here to cover different scenarios
    ("AAPL", "2023-01-01", 100, 200),
    ("AAPL", "2023-01-02", 150, 250),
    ("AAPL", "2023-01-03", 200, 300),
]

# Create the test DataFrame
columns = ["Symbol", "Date", "Volume", "Adj Close"]
test_df = spark.createDataFrame(test_data, columns)

# Test calculate_median UDF
def test_calculate_median():
    assert calculate_median([1, 2, 3, 4]) == 2.5
    assert calculate_median([1, 2, 3, 4, 5]) == 3

# Test the moving average calculation
def test_moving_average():
    window_spec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-2, 0)
    result_df = test_df.withColumn("vol_moving_avg", avg(col("Volume")).over(window_spec))
    expected_data = [
        ("AAPL", "2023-01-01", 100, 200, None),
        ("AAPL", "2023-01-02", 150, 250, 125.0),
        ("AAPL", "2023-01-03", 200, 300, 183.33333333333334),
    ]
    expected_df = spark.createDataFrame(expected_data, columns + ["vol_moving_avg"])
    assert result_df.collect() == expected_df.collect()

# Test the rolling median calculation
def test_rolling_median():
    window_spec = Window.partitionBy('Symbol').orderBy('Date').rowsBetween(-2, 0)
    result_df = test_df.withColumn('adj_close_values', F.collect_list(col('Adj Close')).over(window_spec))
    result_df = result_df.withColumn('adj_close_rolling_med', calculate_median(col('adj_close_values')))
    expected_data = [
        ("AAPL", "2023-01-01", 100, 200, None, None),
        ("AAPL", "2023-01-02", 150, 250, None, 225),
        ("AAPL", "2023-01-03", 200, 300, [200, 250], 250),
    ]
    expected_df = spark.createDataFrame(expected_data, columns + ["adj_close_values", "adj_close_rolling_med"])
    assert result_df.collect() == expected_df.collect()

# Clean up resources after testing
def teardown_module(module):
    spark.stop()

if __name__ == "__main__":
    pytest.main([__file__])
