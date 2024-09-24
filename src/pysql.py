import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('/app/data/sqlite/case_study.db')

# Define the table names
tables = ['agreement', 'product', 'meterpoint']


# Function to process each table
def load_table(table_name):
    print(f"\nLoading table: {table_name}")

    # Query the entire table
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)

    # # Handle table-specific logic
    # if table_name == 'agreement':
    #     # Handle NULL values in 'agreement_valid_to' field for 'agreement' table
    #     df['agreement_valid_to'].fillna('9999-12-31', inplace=True)
    #
    #     # Convert 'agreement_valid_from' and 'agreement_valid_to' columns to date format
    #     df['agreement_valid_from'] = pd.to_datetime(df['agreement_valid_from'], format='%Y-%m-%d', errors='coerce')
    #     df['agreement_valid_to'] = pd.to_datetime(df['agreement_valid_to'], format='%Y-%m-%d', errors='coerce')

    # Print out the first few rows of the table
    print(df.head())
    return df


# Load all tables and store them in a dictionary
table_dfs = {}
for table in tables:
    table_dfs[table] = load_table(table)

# Close the connection to the database
conn.close()

# Now `table_dfs` dictionary holds DataFrames for each table: 'agreement', 'product', and 'meterpoint'
