# Count rows in RAW_DOW30_STAGING
row_count = raw_df.count()
print(f"✅ Number of rows loaded from RAW_DOW30_STAGING: {row_count}")

# Show first few rows
raw_df.show()
