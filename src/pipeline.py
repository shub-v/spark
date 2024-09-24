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
    .config("spark.driver.extraClassPath", "/app/sqlite-jdbc.jar") \
    .config("spark.sql.parquet.compression.codec", "uncompressed") \
    .getOrCreate()


# SQLite database path
db_path = '/app/data/sqlite/case_study.db'

# Define the table names
tables = ['agreement', 'product', 'meterpoint']


def parse_json_to_df(json_file_path):
    """
    Parses a single JSON file and returns a DataFrame.
    """
    try:
        # Read the JSON file as a single string (each JSON file is a single object)
        rdd = spark.sparkContext.textFile(json_file_path)
        json_data = rdd.collect()[0]

        # Convert JSON string into a Python dictionary
        parsed_data = json.loads(json_data)

        # Extract columns and rows from the JSON object
        columns = parsed_data["columns"]
        data = parsed_data["data"]

        # Create an RDD of Row objects and then a DataFrame
        row_rdd = spark.sparkContext.parallelize(data).map(lambda x: Row(*x))
        df = spark.createDataFrame(row_rdd, columns)

        return df

    except json.JSONDecodeError as e:
        print(f"Error parsing JSON file {json_file_path}: {e}")
        return None


def process_all_json_files(directory_path):
    """
    Process all JSON files in a directory and return a single combined DataFrame.
    """
    all_dfs = []

    # List all JSON files in the directory
    json_files = [f for f in os.listdir(directory_path) if f.endswith(".json")]

    if not json_files:
        print("No JSON files found.")
        return None

    # Process each JSON file in parallel
    for filename in json_files:
        json_file_path = os.path.join(directory_path, filename)
        print(f"Processing file: {json_file_path}")

        # Parse each JSON file into a DataFrame
        df = parse_json_to_df(json_file_path)
        if df is not None:
            all_dfs.append(df)

    # Combine all DataFrames into one using union
    if all_dfs:
        combined_df = all_dfs[0]
        for df in all_dfs[1:]:
            combined_df = combined_df.union(df)
        return combined_df

    return None


def load_sqlite_table(table_name, conn):
    """
    Load data from an SQLite table and return a Pandas DataFrame.
    """
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    print(f"\nLoaded {table_name} table:")
    print(df.head())
    return df


def load_all_sqlite_tables(tables, db_path):
    """
    Load all specified SQLite tables into a dictionary of DataFrames.
    """
    with sqlite3.connect(db_path) as conn:
        table_dfs = {}
        for table in tables:
            table_dfs[table] = load_sqlite_table(table, conn)
        return table_dfs


if __name__ == "__main__":
    print(spark.version)
    # Directory containing multiple JSON files
    json_directory_path = "data/readings"

    # Process and combine JSON files
    final_json_df = process_all_json_files(json_directory_path)

    # Write combined JSON DataFrame to Delta Table
    if final_json_df is not None:
        final_json_df.write.format("delta").mode("overwrite").save("data/delta/json_data")
        print("JSON data written to Delta table.")

    # Load all SQLite tables
    table_dfs = load_all_sqlite_tables(tables, db_path)

    # Write SQLite DataFrames to Delta Tables
    for table_name, df in table_dfs.items():
        df_spark = spark.createDataFrame(df)  # Convert Pandas DataFrame to PySpark DataFrame
        delta_table_path = f"data/delta/{table_name}"
        df_spark.write.format("delta").mode("overwrite").save(delta_table_path)
        print(f"{table_name} table written to Delta table.")

    # Stop the Spark session
    spark.stop()
