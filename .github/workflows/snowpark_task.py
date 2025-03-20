from snowflake.snowpark import Session
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SNOWFLAKE_TASKS")

# Snowflake Connection
connection_params = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
}

# Create a Snowpark Session
session = Session.builder.configs(connection_params).create()

def execute_stored_procedure(procedure_name):
    """Executes a stored procedure using Snowpark."""
    try:
        logger.info(f"Executing stored procedure: {procedure_name}")
        result = session.sql(f"CALL {procedure_name}();").collect()
        logger.info(f"Stored Procedure `{procedure_name}` executed successfully: {result[0][0]}")
    except Exception as e:
        logger.error(f"Error executing {procedure_name}: {str(e)}")

# Run the procedure
execute_stored_procedure("FRED_FINANCIAL_DATA.HARMONIZED_DOW30.HARMONIZE_DATA")
execute_stored_procedure("FRED_FINANCIAL_DATA.ANALYTICS_DOW30.UPDATE_DOW30_SP")