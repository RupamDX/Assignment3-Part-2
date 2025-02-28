import pandas as pd
from fredapi import Fred
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import datetime

###############################################################################
# 1. CONFIGURATION
###############################################################################
API_KEY = 'e07ed948f0314d4f0db48710c294a48a'  

conn_params = {
    'user': 'vamshika12',
    'password': 'Asvmsh@12Agsgr@27',
    'account': 'dgeyskn-tob19336',
    'warehouse': 'COMPUTE_WH',
    'database': 'FRED_FINANCIAL_DATA',
    'schema': 'RAW_DOW30'
}

indices = {
    'NASDAQ': 'NASDAQCOM',
    'S&P500': 'SP500',
    'DOW': 'DJIA'
}

###############################################################################
# 2. FETCH NEW LIVE DATA FROM FRED API
###############################################################################
def fetch_new_live_data_from_fred() -> pd.DataFrame:
    fred = Fred(api_key=API_KEY)
    data_frames = []
    
    # Fetch only the latest available date for each index
    today = datetime.datetime.today().strftime('%Y-%m-%d')
    
    for name, series_id in indices.items():
        series_data = fred.get_series(series_id, start=today)
        df = series_data.reset_index()
        df.columns = ['date', 'close']
        df['index_name'] = name
        data_frames.append(df)
    
    combined_df = pd.concat(data_frames, ignore_index=True)
    combined_df['date'] = pd.to_datetime(combined_df['date'], errors='coerce')
    combined_df = combined_df.dropna(subset=['date'])
    
    return combined_df

###############################################################################
# 3. APPEND NEW DATA TO STAGING TABLE
###############################################################################
def append_live_data_to_snowflake(df: pd.DataFrame, conn) -> None:
    df.columns = [col.upper() for col in df.columns]
    df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')

    success, nchunks, nrows, _ = write_pandas(
        conn, df, "RAW_DOW30_STAGING", 
        database="FRED_FINANCIAL_DATA", 
        schema="RAW_DOW30", 
        quote_identifiers=False
    )
    
    if success:
        print(f" Successfully appended {nrows} new rows into RAW_DOW30_STAGING.")
    else:
        print(" Data append to RAW_DOW30_STAGING failed.")

###############################################################################
# 4. MAIN EXECUTION
###############################################################################
def main():
    conn = snowflake.connector.connect(**conn_params)
    cursor = conn.cursor()
    
    cursor.execute("USE DATABASE FRED_FINANCIAL_DATA")
    cursor.execute("USE SCHEMA RAW_DOW30")
    
    # Fetch the latest data from FRED API
    new_data_df = fetch_new_live_data_from_fred()
    
    # Append new live data to Snowflake
    append_live_data_to_snowflake(new_data_df, conn)

    # Commit & close
    conn.commit()
    conn.close()
    print(" Live data successfully appended to RAW_DOW30_STAGING.")

if __name__ == "__main__":
    main()
