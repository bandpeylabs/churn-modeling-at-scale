# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Configuration
# MAGIC
# MAGIC This notebook:
# MAGIC - Loads configuration from YAML file
# MAGIC - Creates Unity Catalog structure
# MAGIC - Sets up volumes for raw data storage
# MAGIC - Configures all necessary tables
# MAGIC - Sets up environment variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install ../requirements.txt

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

import mlflow
import yaml
import os
from datetime import datetime, timedelta

# Load configuration from YAML file
with open('./config/environment.yaml', 'r') as file:
    config = yaml.safe_load(file)

config = config['main']

print("Configuration loaded successfully")
print(f"Catalog: {config['catalog']}")
print(f"Schema: {config['schema']}")
print(f"Volume: {config['volume']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Unity Catalog Structure

# COMMAND ----------

# Create catalog
spark.sql(f"CREATE CATALOG IF NOT EXISTS {config['catalog']}")
spark.sql(f"USE CATALOG {config['catalog']}")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['schema']}")
spark.sql(f"USE SCHEMA {config['schema']}")

print(f"Unity Catalog structure created:")
print(f"  Catalog: {config['catalog']}")
print(f"  Schema: {config['schema']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume for Raw Data Storage

# COMMAND ----------

# Create volume for storing raw Wikimedia files
volume_path = f"/Volumes/{config['catalog']}/{config['schema']}/{config['volume']}"

try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config['volume']}")
    print(f"Volume created: {config['volume']}")
    print(f"  Path: {volume_path}")
except Exception as e:
    print(f"Volume creation warning: {e}")
    print("Volume may already exist or permissions may be required")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create All Tables Structure

# COMMAND ----------

# Function to create table if not exists


def create_table_if_not_exists(table_name, schema_definition):
    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} {schema_definition}")
        print(f"Table created/verified: {table_name}")
    except Exception as e:
        print(f"Table creation warning for {table_name}: {e}")


# Bronze layer tables
bronze_tables = config['tables']['bronze']
for table_key, table_name in bronze_tables.items():
    if table_key == 'wikimedia_pageviews':
        schema_def = """
        (
            project STRING,
            page_title STRING,
            view_count INT,
            access_method STRING,
            source_file STRING,
            ingestion_timestamp TIMESTAMP,
            data_source STRING,
            file_date STRING
        )
        USING DELTA
        PARTITIONED BY (file_date, project)
        """
    else:
        schema_def = """
        (
            id STRING,
            data STRING,
            ingestion_timestamp TIMESTAMP,
            data_source STRING
        )
        USING DELTA
        """

    create_table_if_not_exists(table_name, schema_def)

# Silver layer tables
silver_tables = config['tables']['silver']
for table_key, table_name in silver_tables.items():
    schema_def = """
    (
        project STRING,
        page_title STRING,
        view_count INT,
        access_method STRING,
        cleaned_timestamp TIMESTAMP,
        data_quality_score DOUBLE
    )
    USING DELTA
    """
    create_table_if_not_exists(table_name, schema_def)

# Gold layer tables
gold_tables = config['tables']['gold']
for table_key, table_name in gold_tables.items():
    if table_key == 'predictions':
        schema_def = """
        (
            user_id STRING,
            churn_probability DOUBLE,
            prediction_date DATE,
            model_version STRING,
            created_timestamp TIMESTAMP
        )
        USING DELTA
        """
    else:
        schema_def = """
        (
            id STRING,
            features STRING,
            created_timestamp TIMESTAMP
        )
        USING DELTA
        """

    create_table_if_not_exists(table_name, schema_def)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Environment Variables

# COMMAND ----------

# Set environment variables for use in other notebooks

# Set config as environment variables
for key, value in config.items():
    if isinstance(value, dict):
        for sub_key, sub_value in value.items():
            if isinstance(sub_value, dict):
                for sub_sub_key, sub_sub_value in sub_value.items():
                    env_key = f"{key}_{sub_key}_{sub_sub_key}".upper()
                    os.environ[env_key] = str(sub_sub_value)
            else:
                env_key = f"{key}_{sub_key}".upper()
                os.environ[env_key] = str(sub_value)
    else:
        env_key = key.upper()
        os.environ[env_key] = str(value)

print("Environment variables set")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set Up ML Environment

# COMMAND ----------

# Set up MLflow experiment
ml_config = config['ml']
experiment_name = ml_config['experiment_name']

# Create MLflow experiment
mlflow.set_experiment(experiment_name)

print(f"MLflow experiment created: {experiment_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Data Source URLs

# COMMAND ----------

# Generate Wikimedia download URLs using regex pattern
wikimedia_config = config['data_sources']['wikimedia']
base_url = wikimedia_config['base_url']
start_date = wikimedia_config['start_date']
end_date = wikimedia_config['end_date']

# Simple regex-based file generation
urls = []
start_dt = datetime.strptime(start_date, '%Y%m%d')
end_dt = datetime.strptime(end_date, '%Y%m%d')

current_dt = start_dt
while current_dt <= end_dt:
    date_str = current_dt.strftime('%Y%m%d')
    for hour in range(24):
        hour_str = f"{hour:06d}"  # 000000, 010000, etc.
        filename = f"pageviews-{date_str}-{hour_str}.gz"
        url = f"{base_url}/{filename}"
        urls.append({
            'url': url,
            'filename': filename
        })
    current_dt += timedelta(days=1)

print(f"Generated {len(urls)} download URLs")
print("Sample URLs:")
for url_info in urls[:3]:
    print(f"  {url_info['filename']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Environment Setup

# COMMAND ----------

# Verify catalog and schema
current_catalog = spark.sql("SELECT current_catalog() as catalog").collect()[
    0]['catalog']
current_schema = spark.sql("SELECT current_schema() as schema").collect()[
    0]['schema']

print("=== Environment Verification ===")
print(f"Current Catalog: {current_catalog}")
print(f"Current Schema: {current_schema}")

# List tables
tables = spark.sql("SHOW TABLES").collect()
print(f"Tables in schema: {len(tables)}")
for table in tables:
    print(f"  - {table['tableName']}")

# List volumes
try:
    volumes = spark.sql("SHOW VOLUMES").collect()
    print(f"Volumes: {len(volumes)}")
    for volume in volumes:
        print(f"  - {volume['volumeName']}")
except Exception as e:
    print(f"Volumes: Error listing volumes - {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC **Environment setup completed**
# MAGIC
# MAGIC **Created:**
# MAGIC - Unity Catalog: `{config['catalog']}`
# MAGIC - Schema: `{config['schema']}`
# MAGIC - Volume: `{config['volume']}` for raw data storage
# MAGIC - Bronze tables: {list(bronze_tables.values())}
# MAGIC - Silver tables: {list(silver_tables.values())}
# MAGIC - Gold tables: {list(gold_tables.values())}
# MAGIC - Environment variables for configuration
# MAGIC - Wikimedia download URLs generated
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Run the ingestion notebook to download and process data
# MAGIC - Create Silver and Gold layer tables as needed

# COMMAND ----------

# Store config in notebook context for other notebooks to access
dbutils.notebook.exit({
    "status": "success",
    "config": config,
    "urls": urls,
    "message": "Environment setup completed successfully"
})
