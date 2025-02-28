# Import necessary modules
from config import conn_params  # Import connection parameters
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col

# Create a Snowflake session
session = Session.builder.configs(conn_params).create()
print(" Snowflake session created successfully!")

# Load raw data from staging table
raw_df = session.table("RAW_DOW30_STAGING")

# Count rows in RAW_DOW30_STAGING
row_count = raw_df.count()
print(f"Number of rows loaded from RAW_DOW30_STAGING: {row_count}")

# Show first few rows of the data
raw_df.show()
