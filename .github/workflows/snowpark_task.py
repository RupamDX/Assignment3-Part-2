from snowflake.snowpark import Session
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("SNOWFLAKE_TASKS")

# Snowflake Connection
connection_params = {
    "account": "dgeyskn-tob19336",
    "user": "Vamshika12",
    "password": "Asvmsh@12Agsgr@27",
    "warehouse": "COMPUTE_WH",
    "database": "FRED_FINANCIAL_DATA",
    "schema": "HARMONIZED_DOW30"
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
