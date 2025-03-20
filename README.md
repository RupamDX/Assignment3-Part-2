# Assignment3-Part-2
# FRED Data Pipeline: Snowflake + Streams + Stored Procedures

Welcome to the **FRED Data Pipeline** repository! This project demonstrates how to:
1. Ingest FRED data into Snowflake (staging).
2. Clean (“harmonize”) the data using **Streams** and **Stored Procedures**.
3. Automate daily updates in an analytics table for 7 days using **GitHub Actions**.
4. Manage environment-specific configurations using **Jinja** templates.

## Architecture Overview

1. **FRED Source**: Download or scrape daily data from FRED’s API.  
2. **Raw Staging**: Data is staged in Snowflake (or uploaded to S3/External Stage, then copied to Snowflake).  
3. **Harmonization**: A Snowflake Stored Procedure processes incremental updates captured by a Snowflake Stream.  
4. **Analytics Table**: Another Stream feeds a stored procedure or transformation logic that creates daily analytics metrics over a rolling 7-day window.  
5. **Automation**: GitHub Actions schedules the pipeline daily.  
6. **Environment Management**: Jinja templates help manage DEV vs. PROD differences (schemas, roles, table names, etc.).  

##Architecture Diagram
![fred_data_pipeline](https://github.com/user-attachments/assets/f64ac4f6-1f51-4e00-b588-ce41bb3b6460)

##AiUseDisclosure
Tools used for debugging and understanding the tool setup flow Chatgpt Gemini
1.Used the tool to setup the applications in local , add integrations on different applications. 2.Used it for debugging and solving errors 3.Helped us understand the flow of different tools and optimize our solutions 4.Understand the basic use of airflow,snowflake and configure it.
