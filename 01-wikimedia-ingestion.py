# Databricks notebook source
# MAGIC %md
# MAGIC # Loading the variables and setting structure of data assets

# COMMAND ----------

# MAGIC %run ./config/01_env_config

# COMMAND ----------

# MAGIC %md
# MAGIC # Wikimedia Pageview Data Ingestion
# MAGIC
# MAGIC **Purpose**: Download and ingest raw Wikimedia pageview data into Bronze layer
# MAGIC
# MAGIC **Data Source**: Wikimedia pageviews dump (January 2025)
# MAGIC **Target**: Bronze layer Delta table for raw data preservation
# MAGIC
# MAGIC **Best Practices Used:**
# MAGIC - Delta Lake for ACID transactions
# MAGIC - Autoloader for streaming file ingestion
# MAGIC - Volume storage for raw files
# MAGIC - Proper partitioning for performance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Configuration

# COMMAND ----------

from pyspark.sql.functions import split, regexp_extract
import yaml
import os
import requests
import gzip
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit, col, input_file_name
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Load configuration
with open('./config/environment.yaml', 'r') as file:
    config = yaml.safe_load(file)['main']

print("Configuration loaded")
print(f"Catalog: {config['catalog']}")
print(f"Schema: {config['schema']}")
print(f"Volume: {config['volume']}")
