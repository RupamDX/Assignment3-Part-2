# config.py - Stores Snowflake connection parameters and additional settings

conn_params = {
    'user': 'vamshika12',
    'password': 'Asvmsh@12Agsgr@27',
    'account': 'dgeyskn-tob19336',
    'warehouse': 'COMPUTE_WH',
    'database': 'FRED_FINANCIAL_DATA',
    'schema': 'RAW_DOW30'  # Default schema for raw data
}

# Update Snowpark-specific configurations
snowpark_config = {
    'schema_harmonized': 'HARMONIZED_DOW30',  # Ensure correct schema name
    'table_harmonized': 'DOW30_HARMONIZED',
    'table_staging': 'RAW_DOW30_STAGING'
}


# UDF stage location for Python functions
udf_stage = "@my_stage"
