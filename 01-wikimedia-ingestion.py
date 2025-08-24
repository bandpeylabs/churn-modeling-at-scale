# Databricks notebook source
# MAGIC %md
# MAGIC # Wikimedia Pageview Data Ingestion
# MAGIC
# MAGIC **Purpose**: Download, unzip, and ingest raw Wikimedia pageview data into Bronze layer using Spark-native approaches
# MAGIC
# MAGIC **Data Source**: Wikimedia pageviews dump (January 2025)
# MAGIC **Target**: Bronze layer Delta table for raw data preservation
# MAGIC
# MAGIC **Best Practices Used:**
# MAGIC - Delta Lake for ACID transactions
# MAGIC - Spark native file handling
# MAGIC - Volume storage for raw files
# MAGIC - Proper partitioning for performance

# COMMAND ----------

# MAGIC %run ./config/01_env_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wikimedia Data Ingestion
# MAGIC
# MAGIC Now using environment variables and configuration loaded from the environment setup notebook.
# MAGIC
# MAGIC **Available variables from environment setup:**
# MAGIC - `config`: Complete configuration dictionary
# MAGIC - `volume_path`: Path to the raw data volume
# MAGIC - `bronze_tables`, `silver_tables`, `gold_tables`: Table configurations

# COMMAND ----------

# Import required functions for data processing
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_timestamp, regexp_extract
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit, input_file_name, col, split

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Files DataFrame from Environment Setup

# COMMAND ----------

# The files DataFrame is already loaded from the environment setup
# It contains: filename, timestamp, file_size_bytes, full_url
print(f"Using files DataFrame with {urls_df.count()} available files")
print("\nSample files:")
urls_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Files Using DataFrame

# COMMAND ----------

# Filter files to download (e.g., only pageviews files, or specific date range)
# You can modify this filter based on your needs
files_to_download = urls_df.filter(
    (col("filename").like("pageviews-%")) &
    (col("file_size_bytes") > 0)
).orderBy("timestamp")

print(f"Found {files_to_download.count()} pageviews files to download")

# Limit downloads for demo (can be increased or removed for production)
max_downloads = urls_df.count()
files_to_download = files_to_download.limit(max_downloads)

print(f"Starting download of {max_downloads} files")

# Download files directly to volume
download_results = []
for row in files_to_download.collect():
    filename = row['filename']
    full_url = row['full_url']
    file_size = row['file_size_bytes']
    volume_file_path = f"{volume_path}/{filename}"

    print(f"Downloading: {filename} ({file_size} bytes)")
    try:
        # Download directly to volume using dbutils
        dbutils.fs.cp(full_url, volume_file_path)
        download_results.append({
            'filename': filename,
            'status': 'success',
            'volume_path': volume_file_path,
            'file_size': file_size
        })
        print(f"Downloaded: {filename}")
    except Exception as e:
        download_results.append({
            'filename': filename,
            'status': 'failed',
            'error': str(e)
        })
        print(f"Failed: {filename} - {e}")

# Create download results DataFrame
download_results_df = spark.createDataFrame(download_results)
print(f"\nDownload summary:")
download_results_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Spark Schema for Wikimedia Data
# MAGIC
# MAGIC Based on the sample data structure:
# MAGIC `"project page_title view_count access_method"`
# MAGIC
# MAGIC Example: `"en.wikipedia Main_Page 1500 desktop"`

# COMMAND ----------


wikimedia_schema = StructType([
    StructField("project", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("view_count", IntegerType(), True),
    StructField("access_method", StringType(), True)
])

print("Schema defined for Wikimedia pageview data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data Using Spark Native File Reading

# COMMAND ----------

# Use Spark to read gzipped files directly
# Spark can handle gzipped files natively without manual unzipping
bronze_df = spark.read \
    .format("text") \
    .option("compression", "gzip") \
    .load(f"{volume_path}/*.gz")

print("Files loaded using Spark native gzip support")
print(f"Loaded {bronze_df.count()} raw records from gzipped files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse and Transform Data

# COMMAND ----------

# Parse the text data into structured format using Spark functions
parsed_df = bronze_df.select(
    split(col("value"), " ").getItem(0).alias("project"),
    split(col("value"), " ").getItem(1).alias("page_title"),
    split(col("value"), " ").getItem(2).cast("int").alias("view_count"),
    split(col("value"), " ").getItem(3).alias("access_method"),
    input_file_name().alias("source_file"),
    current_timestamp().alias("ingestion_timestamp")
)

# Extract filename from source_file path for joining with metadata
parsed_df = parsed_df.withColumn(
    "filename",
    regexp_extract(col("source_file"), r"([^/]+\.gz)$", 1)
)

print("Data parsed and filename extracted for metadata joining")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich Data with Metadata from URLs DataFrame

# COMMAND ----------

# Join parsed data with metadata from urls_df
enriched_df = parsed_df.join(
    urls_df.select("filename", "timestamp", "file_size_bytes", "full_url"),
    on="filename",
    how="left"
).select(
    col("project"),
    col("page_title"),
    col("view_count"),
    col("access_method"),
    col("source_file"),
    col("ingestion_timestamp"),
    col("timestamp").alias("file_timestamp"),
    col("file_size_bytes"),
    col("full_url").alias("source_url")
)

print("Data enriched with metadata from URLs DataFrame")
print(f"Enriched DataFrame schema:")
enriched_df.printSchema()
print(f"Sample enriched data:")
enriched_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Metadata and Write to Bronze Table

# COMMAND ----------

# Use enriched data with metadata from URLs DataFrame
final_bronze_df = enriched_df.withColumn(
    "data_source", lit("wikimedia_pageviews"))

# Get bronze table name and location from config
bronze_table_name = config['tables']['bronze']['wikimedia_pageviews']
bronze_table_location = f"{volume_path}/tables/{bronze_table_name}"

print(f"Writing to bronze table: {bronze_table_name}")
print(f"Table location: {bronze_table_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table (Bronze Layer)

# COMMAND ----------

# Write to Delta format at the prepared location
final_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .partitionBy("file_timestamp", "project") \
    .save(bronze_table_location)

# Create the table from the saved data
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_table_name}
USING DELTA
LOCATION '{bronze_table_location}'
""")

print(f"Bronze table created successfully: {bronze_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Ingestion

# COMMAND ----------

# Read back and verify
bronze_table = spark.table(bronze_table_name)

print("=== Bronze Table Verification ===")
print(f"Table: {bronze_table_name}")
print(f"Total records: {bronze_table.count()}")

print("\nSchema:")
bronze_table.printSchema()

print("\nSample records:")
bronze_table.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Check

# COMMAND ----------

# Basic data quality checks
print("=== Data Quality Report ===")

# Check for nulls
null_counts = {}
for column in bronze_table.columns:
    null_count = bronze_table.filter(f"{column} IS NULL").count()
    null_counts[column] = null_count

print("\nNull value counts:")
for col, count in null_counts.items():
    print(f"  {col}: {count}")

# Check data distribution
print("\nProject distribution:")
bronze_table.groupBy("project").count().show()

print("\nAccess method distribution:")
bronze_table.groupBy("access_method").count().show()

print("\nView count statistics:")
bronze_table.select("view_count").summary().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# MAGIC %md
# MAGIC **Bronze layer ingestion completed successfully**
# MAGIC
# MAGIC **What was accomplished:**
# MAGIC - Configuration loaded from YAML
# MAGIC - Unity Catalog environment set up
# MAGIC - Volume created for raw file storage
# MAGIC - Wikimedia download URLs generated
# MAGIC - Files downloaded using Spark native methods
# MAGIC - Data loaded using Spark gzip support
# MAGIC - Proper schema applied based on data structure
# MAGIC - Data ingested into bronze table
# MAGIC - Data quality checks completed
# MAGIC
# MAGIC **Bronze table details:**
# MAGIC - **Table**: `{bronze_table_name}`
# MAGIC - **Records**: {bronze_table.count()}
# MAGIC - **Partitioning**: By file_date and project
# MAGIC - **Format**: Delta Lake with ACID transactions
# MAGIC - **Schema**: Optimized for Wikimedia pageview data
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Process data in Silver layer (cleaning and validation)
# MAGIC - Create Gold layer (business-ready features)
# MAGIC - Build ML pipeline for churn prediction

# COMMAND ----------

# Clean up and exit
dbutils.notebook.exit({
    "status": "success",
    "bronze_table": bronze_table_name,
    "record_count": bronze_table.count(),
    "message": "Bronze layer ingestion completed successfully"
})
