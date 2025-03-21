-- manage_env.j2
-- Jinja template to configure Snowflake environment dynamically

USE ROLE DEV_ROLE;
USE WAREHOUSE DEV_WH;
USE DATABASE DEV_DB;
USE SCHEMA DEV_SCHEMA;

CREATE TABLE IF NOT EXISTS DEV_SCHEMA.SAMPLE_TABLE (
    ID INTEGER AUTOINCREMENT,
    NAME STRING,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

    GRANT ALL PRIVILEGES ON TABLE DEV_SCHEMA.SAMPLE_TABLE TO ROLE DEV_ROLE;
