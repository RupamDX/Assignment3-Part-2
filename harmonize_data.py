from snowflake.snowpark import Session
import config  # Import Snowflake connection settings
from snowflake.snowpark.functions import col, call_udf, to_date, trim, upper, array_agg

# ✅ Create Snowpark session
session = Session.builder.configs(config.conn_params).create()

# ✅ Ensure correct database and schema for staging
session.sql(f"USE DATABASE {config.conn_params['database']}").collect()
session.sql("USE SCHEMA RAW_DOW30").collect()

print(f"✅ Connected to {config.conn_params['database']}.RAW_DOW30")

# ✅ Load raw data from staging table
df_raw = session.table("RAW_DOW30.RAW_DOW30_STAGING")

# ✅ Replace NULL values in the `CLOSE` column with 0.0
df_cleaned = df_raw.fillna({"CLOSE": 0.0})

# ✅ Apply transformations: Convert date format & clean index names
df_transformed = (
    df_cleaned.withColumn("DATE", to_date(col("DATE")))  # Convert DATE format
              .withColumn("INDEX_NAME", upper(trim(col("INDEX_NAME"))))  # Ensure consistent casing
)

# ✅ Remove duplicate records (keep only unique `INDEX_NAME` and `DATE`)
df_deduplicated = df_transformed.dropDuplicates(["INDEX_NAME", "DATE"])

# ✅ Switch to the harmonized schema
session.sql("USE SCHEMA HARMONIZED_DOW30").collect()

# ✅ Write cleaned data to the harmonized table
df_deduplicated.write.mode("overwrite").saveAsTable("HARMONIZED_DOW30.DOW30_HARMONIZED")

print("✅ Data successfully harmonized, deduplicated, and written to: HARMONIZED_DOW30.DOW30_HARMONIZED")

# ✅ Step 1: Ensure the data is sorted before aggregation
df_sorted = df_deduplicated.sort(col("INDEX_NAME"), col("DATE"))

# ✅ Step 2: Compute stock volatility using Python UDF
df_volatility = (
    df_sorted.group_by("INDEX_NAME")
             .agg(call_udf("HARMONIZED_DOW30.STOCK_VOLATILITY", array_agg(col("CLOSE"))).alias("VOLATILITY"))  # ✅ Correct alias
)

# ✅ Step 3: Debugging: Check column names
print("Columns in df_volatility:", df_volatility.columns)

# ✅ Step 4: Store volatility results in Snowflake
df_volatility.write.mode("overwrite").saveAsTable("HARMONIZED_DOW30.STOCK_VOLATILITY_DATA")

print("✅ Stock volatility successfully calculated and stored in: HARMONIZED_DOW30.STOCK_VOLATILITY_DATA")
