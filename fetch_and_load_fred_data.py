import pandas as pd
from fredapi import Fred
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

###############################################################################
# 1. CONFIGURATION
###############################################################################
# FRED API Key
API_KEY = 'e07ed948f0314d4f0db48710c294a48a'  # Replace with your actual FRED API key

# Snowflake connection parameters
conn_params = {
    'user': 'vamshika12',
    'password': 'Asvmsh@12Agsgr@27',
    'account': 'dgeyskn-tob19336',
    'warehouse': 'COMPUTE_WH',
    'database': 'FRED_FINANCIAL_DATA',
    'schema': 'RAW_DOW30'  # Target schema for raw staging
}

# Define the indices to fetch from FRED
indices = {
    'NASDAQ': 'NASDAQCOM',
    'S&P500': 'SP500',
    'DOW': 'DJIA'
}

###############################################################################
# 2. FETCH RAW DATA FROM FRED API
###############################################################################
def fetch_raw_data_from_fred() -> pd.DataFrame:
    fred = Fred(api_key=API_KEY)
    data_frames = []
    for name, series_id in indices.items():
        # Retrieve the time series data from FRED
        series_data = fred.get_series(series_id)
        df = series_data.reset_index()
        df.columns = ['date', 'close']  # Keep only the date and close price
        df['index_name'] = name       # Tag with the index name
        data_frames.append(df)
    
    # Combine all index data into one DataFrame
    combined_df = pd.concat(data_frames, ignore_index=True)
    
    # Convert 'date' column to datetime for consistency
    combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')
    # Drop any rows with invalid dates
    combined_df = combined_df.dropna(subset=['date'])
    
    return combined_df

###############################################################################
# 3. LOAD RAW DATA INTO SNOWFLAKE STAGING TABLE
###############################################################################
def load_raw_data_to_snowflake(df: pd.DataFrame, conn) -> None:
    # Convert DataFrame column names to uppercase (Snowflake stores unquoted identifiers in uppercase)
    df.columns = [col.upper() for col in df.columns]

    # Convert DATE column to string format (YYYY-MM-DD) to prevent type issues
    df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')

    # Debug: Print the DataFrame schema before insertion
    print("Schema of DataFrame before inserting into Snowflake:")
    print(df.dtypes)

    # Print tables in the RAW_DOW30 schema for debugging
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES IN SCHEMA RAW_DOW30")
    tables = cursor.fetchall()
    print("Tables in RAW_DOW30 schema:")
    for table in tables:
        print(table)
    cursor.close()
    
    # Use write_pandas to bulk load data into the staging table.
    # Fully qualified table name will be "FRED_FINANCIAL_DATA.RAW_DOW30.RAW_DOW30_STAGING"
    success, nchunks, nrows, _ = write_pandas(
        conn, df, "RAW_DOW30_STAGING", 
        database="FRED_FINANCIAL_DATA", 
        schema="RAW_DOW30", 
        quote_identifiers=False  # **Prevents Snowflake from misinterpreting case-sensitive names**
    )
    if success:
        print(f" Successfully loaded {nrows} rows into RAW_DOW30.RAW_DOW30_STAGING.")
    else:
        print(" Data load into RAW_DOW30.RAW_DOW30_STAGING failed.")

###############################################################################
# 4. MAIN EXECUTION
###############################################################################
def main():
    # Connect to Snowflake
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()
    
    # Set the database and schema explicitly
    cursor.execute("USE DATABASE FRED_FINANCIAL_DATA")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW_DOW30")  # Ensure schema exists
    cursor.execute("USE SCHEMA RAW_DOW30")
    
    # Create the staging table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS RAW_DOW30.RAW_DOW30_STAGING (
        DATE DATE,
        INDEX_NAME STRING,
        CLOSE FLOAT
    )
    """
    cursor.execute(create_table_query)
    conn.commit()  # Ensure the table creation is committed

    # Verify that the table exists
    cursor.execute("SHOW TABLES LIKE 'RAW_DOW30_STAGING'")
    result = cursor.fetchall()
    print("Result of SHOW TABLES for RAW_DOW30_STAGING:", result)
    
    cursor.close()
    
    # Fetch raw data from FRED API
    raw_df = fetch_raw_data_from_fred()
    
    # Load raw data into the staging table
    load_raw_data_to_snowflake(raw_df, conn)
    
    # Commit and close connection
    conn.commit()
    conn.close()
    print(" Raw data staged successfully in RAW_DOW30.RAW_DOW30_STAGING.")

if __name__ == "__main__":
    main()
