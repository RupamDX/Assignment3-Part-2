
from snowflake.snowpark import Session
import logging
import time

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

#session = Session.builder.configs(connection_params).create()

def execute_stored_procedure(procedure_name):
    """Executes a stored procedure using the provided session."""
    try:
        logger.info(f"Executing stored procedure: {procedure_name}")
        session.sql(f"CALL {procedure_name}();").collect()
        logger.info(f"Stored Procedure `{procedure_name}` executed successfully.")
    except Exception as e:
        logger.error(f"Error executing {procedure_name}: {str(e)}")

def check_task_status():
    """Checks the latest execution status of tasks in Snowflake."""
    try:
        logger.info("Fetching latest task execution status...")
        df = session.sql("""
            SELECT NAME, STATE, COMPLETED_TIME
            FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
            ORDER BY COMPLETED_TIME DESC LIMIT 5
        """).collect()
        for row in df:
            logger.info(f"Task: {row['NAME']} | Status: {row['STATE']} | Completed: {row['COMPLETED_TIME']}")
    except Exception as e:
        logger.error(f"Error fetching task status: {str(e)}")

# Step 1: Execute Stored Procedures Using the Provided Session
#execute_stored_procedure("HARMONIZE_DATA()")
execute_stored_procedure("HARMONIZE_DATA()")


# Step 2: Monitor the Task Execution
check_task_status()

print("Stored Procedure Triggered & Execution Status Checked!")