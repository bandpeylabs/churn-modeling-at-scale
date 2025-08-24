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

# MAGIC %pip install -r ./requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, lit
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

try:
    spark.sql(
        f"CREATE CATALOG IF NOT EXISTS {config['catalog']}"
    )
    print(f"Catalog created: {config['catalog']}")
except Exception as e:
    print(f"Catalog creation warning: {e}")
    print("Catalog may already exist or permissions may be required")

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

# Bronze layer tables
bronze_tables = config['tables']['bronze']
for table_key, table_name in bronze_tables.items():
    # Create empty table - schema will be inferred from data
    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} (dummy STRING) USING DELTA")
        spark.sql(f"DELETE FROM {table_name}")  # Clear dummy data
        print(f"Empty table created: {table_name}")
    except Exception as e:
        print(f"Table creation warning for {table_name}: {e}")

# Silver layer tables
silver_tables = config['tables']['silver']
for table_key, table_name in silver_tables.items():
    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} (dummy STRING) USING DELTA")
        spark.sql(f"DELETE FROM {table_name}")  # Clear dummy data
        print(f"Empty table created: {table_name}")
    except Exception as e:
        print(f"Table creation warning for {table_name}: {e}")

# Gold layer tables
gold_tables = config['tables']['gold']
for table_key, table_name in gold_tables.items():
    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} (dummy STRING) USING DELTA")
        spark.sql(f"DELETE FROM {table_name}")  # Clear dummy data
        print(f"Empty table created: {table_name}")
    except Exception as e:
        print(f"Table creation warning for {table_name}: {e}")

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

try:
    # Create experiment if it doesn't exist
    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        experiment_id = mlflow.create_experiment(experiment_name)
        print(
            f"MLflow experiment created: {experiment_name} (ID: {experiment_id})")
    else:
        print(
            f"MLflow experiment found: {experiment_name} (ID: {experiment.experiment_id})")

    # Set as active experiment
    mlflow.set_experiment(experiment_name)
except Exception as e:
    print(f"MLflow experiment setup warning: {e}")
    print("Continuing without MLflow experiment setup")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Files List and Create URLs

# COMMAND ----------

# Load the files list from CSV
# Build absolute path using current user's workspace path
current_user = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get()
files_csv_path = f"/dbfs/Workspace/Users/{current_user}/churn-modeling-at-scale/config/files.csv"

# Read CSV with proper schema
files_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(files_csv_path)

# Convert timestamp string to proper timestamp type

files_df = files_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "dd-MMM-yyyy HH:mm")
)

# Get base URL from config and create full URLs
wikimedia_config = config['data_sources']['wikimedia']
base_url = wikimedia_config['base_url']

# Add full_url column by combining base_url with filename
files_df = files_df.withColumn(
    "full_url",
    concat(lit(base_url), lit("/"), col("filename"))
)

print(f"Loaded {files_df.count()} files from CSV")
print("Sample files with URLs:")
files_df.select("filename", "timestamp", "file_size_bytes",
                "full_url").show(5, truncate=False)

# Store the DataFrame for use in other notebooks
urls_df = files_df

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

# Store config in notebook context for other notebooks to access
dbutils.notebook.exit({
    "status": "success",
    "config": config,
    "urls_df": urls_df,
    "message": "Environment setup completed successfully"
})
