import os
import json
import sqlite3
import pandas as pd
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

# Initialize Spark Session with Delta Lake configuration
spark = SparkSession.builder \
    .appName("MultipleJsonAndSqliteToDelta") \
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
# 1. Active Agreements on 1st January 2021
# ==============================
active_date = "2021-01-01"
active_agreements_df = agreement_df.filter(
    (F.col("agreement_valid_from") < active_date) &
    ((F.col("agreement_valid_to").isNull()) | (F.col("agreement_valid_to") > active_date))
)

active_agreements_with_product = active_agreements_df \
    .join(product_df, "product_id", "inner") \
    .join(meterpoint_df, "meterpoint_id", "inner") \
    .select(
        "agreement_id", "meterpoint_id", "display_name", "is_variable"
    )

# Show active agreements
print("Active Agreements on 1st January 2021:")
active_agreements_with_product.show(truncate=False)

# Write the result to a new Delta table
active_agreements_with_product.write.format("delta").mode("overwrite").save("data/delta_analytics/active_agreements_20210101")


# ==============================
# 2. Aggregated Total Consumption Per Half-Hour
# ==============================
aggregated_df = json_data_df.groupBy("interval_start") \
    .agg(
        F.countDistinct("meterpoint_id").alias("distinct_meterpoint_count"),  # Count distinct meterpoints
        F.sum("consumption_delta").alias("total_consumption_kWh")             # Sum of consumption in kWh
    ) \
    .orderBy("interval_start")

# Show the result
print("Aggregated Consumption and Meterpoint Count:")
aggregated_df.show(truncate=False)

# Optionally, write the aggregated result to a new Delta table
aggregated_df.write.format("delta").mode("overwrite").save("data/delta_analytics/half_hour_aggregates")


# ==============================
# 3. Total Consumption Per Product Per Day
# ==============================
# First, join `json_data_df` with `agreement_df` to get the product ID for each meterpoint reading
product_consumption_df = json_data_df \
    .join(agreement_df, "meterpoint_id", "inner") \
    .join(product_df, "product_id", "inner")

# Now, group by product and date (we extract the date part from `interval_start`)
product_daily_consumption_df = product_consumption_df \
    .withColumn("date", F.to_date("interval_start")) \
    .groupBy("display_name", "date") \
    .agg(
        F.countDistinct("meterpoint_id").alias("distinct_meterpoint_count"),  # Count distinct meterpoints
        F.sum("consumption_delta").alias("total_consumption_kWh")             # Sum of consumption in kWh
    ) \
    .orderBy("date", "display_name")

# Show the result
print("Total Consumption Per Product Per Day:")
product_daily_consumption_df.show(truncate=False)

# Write the result to a new Delta table
product_daily_consumption_df.write.format("delta").mode("overwrite").save("data/delta_analytics/product_daily_consumption")

# Stop the Spark session
spark.stop()
