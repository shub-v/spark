import os
import json
import sqlite3
import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session with Delta Lake configuration
spark = SparkSession.builder \
    .appName("GoldTablesConsumptionAnalysis") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.parquet.compression.codec", "uncompressed") \
    .getOrCreate()

# Function to read Delta table
def read_delta_table(table_path):
    return spark.read.format("delta").load(table_path)

# Paths for Delta tables
delta_table_paths = {
    "agreement": "data/delta/agreement",
    "product": "data/delta/product",
    "meterpoint": "data/delta/meterpoint",
    "json_data": "data/delta/json_data"
}

# Read Delta tables into DataFrames
agreement_df = read_delta_table(delta_table_paths["agreement"])
product_df = read_delta_table(delta_table_paths["product"])
meterpoint_df = read_delta_table(delta_table_paths["meterpoint"])
json_data_df = read_delta_table(delta_table_paths["json_data"])

# Ensure the `interval_start` column in json_data_df is in timestamp format
json_data_df = json_data_df.withColumn("interval_start", F.to_timestamp("interval_start"))

# ==============================
# 1. Aggregate Consumption Per Half-Hour
# ==============================
# Aggregate total consumption and distinct meterpoints per half-hour
aggregate_half_hour_df = json_data_df.groupBy("interval_start") \
    .agg(
        F.countDistinct("meterpoint_id").alias("distinct_meterpoint_count"),  # Count distinct meterpoints
        F.sum("consumption_delta").alias("total_consumption_kWh")             # Sum of consumption in kWh
    ) \
    .orderBy("interval_start")

# Show the result
print("Aggregate Consumption Per Half-Hour:")
aggregate_half_hour_df.show(truncate=False)

# Write the result to a new Delta table
aggregate_half_hour_df.write.format("delta").mode("overwrite").save("data/delta_gold/aggregate_half_hour_consumption")


# ==============================
# 2. Average Consumption Per Half-Hour by Product
# ==============================
# First, join `json_data_df` with `agreement_df` and `product_df` to get product information
product_half_hour_df = json_data_df \
    .join(agreement_df, "meterpoint_id", "inner") \
    .join(product_df, "product_id", "inner")

# Now, group by product and half-hour interval, and calculate the average consumption
avg_consumption_per_product_df = product_half_hour_df \
    .groupBy("display_name", "interval_start") \
    .agg(
        F.avg("consumption_delta").alias("avg_consumption_kWh")  # Average consumption in kWh per product
    ) \
    .orderBy("interval_start", "display_name")

# Show the result
print("Average Consumption Per Half-Hour by Product:")
avg_consumption_per_product_df.show(truncate=False)

# Write the result to a new Delta table
avg_consumption_per_product_df.write.format("delta").mode("overwrite").save("data/delta_gold/avg_consumption_per_half_hour_by_product")

# Stop the Spark session
spark.stop()
