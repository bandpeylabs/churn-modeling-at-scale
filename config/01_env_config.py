# Databricks notebook source
# MAGIC %md
# MAGIC # Environment Configuration
# MAGIC
# MAGIC Sets up Unity Catalog, tables, and loads Wikimedia file metadata

# COMMAND ----------

# MAGIC %pip install -r ./requirements.txt
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, lit, concat
import yaml
import os

# Load configuration
with open('./config/environment.yaml', 'r') as file:
    config = yaml.safe_load(file)['main']

print(f"Configuration loaded: {config['catalog']}.{config['schema']}")

# COMMAND ----------

# Create Unity Catalog structure
spark.sql(f"CREATE CATALOG IF NOT EXISTS {config['catalog']}")
spark.sql(f"USE CATALOG {config['catalog']}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config['schema']}")
spark.sql(f"USE SCHEMA {config['schema']}")

# Create volume
volume_path = f"/Volumes/{config['catalog']}/{config['schema']}/{config['volume']}"
try:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {config['volume']}")
    print(f"Volume created: {volume_path}")
except Exception as e:
    print(f"Volume exists: {volume_path}")

# COMMAND ----------

# Create tables
tables = {
    'bronze_wikimedia_pageviews': 'bronze',
    'bronze_user_behavior': 'bronze',
    'bronze_subscription_data': 'bronze',
    'silver_user_features': 'silver',
    'silver_churn_features': 'silver',
    'gold_churn_predictions': 'gold',
    'gold_churn_insights': 'gold'
}

for table_name, layer in tables.items():
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            temp_column STRING
        ) USING DELTA
        """)
        spark.sql(f"ALTER TABLE {table_name} DROP COLUMN temp_column")
        print(f"Created {layer} table: {table_name}")
    except Exception as e:
        print(f"Table {table_name} exists")

# COMMAND ----------

# Load Wikimedia files metadata
current_user = dbutils.notebook.entry_point.getDbutils(
).notebook().getContext().userName().get()
files_csv_path = f"file:/Workspace/Users/{current_user}/churn-modeling-at-scale/config/files.csv"

files_df = spark.read.option("header", "true").option(
    "inferSchema", "true").csv(files_csv_path)
files_df = files_df.withColumn("timestamp", to_timestamp(
    col("timestamp"), "dd-MMM-yyyy HH:mm"))

# Add full URLs
wikimedia_config = config['data_sources']['wikimedia']
base_url = wikimedia_config['base_url']
files_df = files_df.withColumn("full_url", concat(
    lit(base_url), lit("/"), col("filename")))

urls_df = files_df
print(f"Loaded {urls_df.count()} Wikimedia files")

# COMMAND ----------

# Exit with configuration
dbutils.notebook.exit({
    "status": "success",
    "config": config,
    "urls_df": urls_df,
    "volume_path": volume_path,
    "message": "Environment setup completed"
})
