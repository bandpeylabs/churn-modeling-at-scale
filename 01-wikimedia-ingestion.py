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
# max_downloads = urls_df.count()
max_downloads = 50
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

# Get bronze table name from config
bronze_table_name = config['tables']['bronze']['wikimedia_pageviews']

print(f"Writing to bronze table: {bronze_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table (Bronze Layer)

# COMMAND ----------

# Write directly to the Delta table with schema overwrite
final_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("file_timestamp", "project") \
    .saveAsTable(bronze_table_name)

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

# MAGIC %md
# MAGIC ### Null Value Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for nulls in each column
# MAGIC SELECT
# MAGIC   'project' as column_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(project) as non_null_count,
# MAGIC   COUNT(*) - COUNT(project) as null_count,
# MAGIC   ROUND(((COUNT(*) - COUNT(project)) / COUNT(*)) * 100, 2) as null_percentage
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'page_title' as column_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(page_title) as non_null_count,
# MAGIC   COUNT(*) - COUNT(page_title) as null_count,
# MAGIC   ROUND(((COUNT(*) - COUNT(page_title)) / COUNT(*)) * 100, 2) as null_percentage
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'view_count' as column_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(view_count) as non_null_count,
# MAGIC   COUNT(*) - COUNT(view_count) as null_count,
# MAGIC   ROUND(((COUNT(*) - COUNT(view_count)) / COUNT(*)) * 100, 2) as null_percentage
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'access_method' as column_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(access_method) as non_null_count,
# MAGIC   COUNT(*) - COUNT(access_method) as null_count,
# MAGIC   ROUND(((COUNT(*) - COUNT(access_method)) / COUNT(*)) * 100, 2) as null_percentage
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC ORDER BY column_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Project Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Project distribution for visualization
# MAGIC SELECT
# MAGIC   project,
# MAGIC   COUNT(*) as record_count
# MAGIC FROM ${bronze_table_name}
# MAGIC GROUP BY project
# MAGIC ORDER BY record_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Access Method Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Access method distribution for visualization
# MAGIC SELECT
# MAGIC   access_method,
# MAGIC   COUNT(*) as record_count
# MAGIC FROM ${bronze_table_name}
# MAGIC GROUP BY access_method
# MAGIC ORDER BY record_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View count statistics
# MAGIC SELECT
# MAGIC   COUNT(*) as total_records,
# MAGIC   MIN(view_count) as min_views,
# MAGIC   MAX(view_count) as max_views,
# MAGIC   AVG(view_count) as avg_views,
# MAGIC   STDDEV(view_count) as stddev_views,
# MAGIC   PERCENTILE(view_count, 0.25) as p25_views,
# MAGIC   PERCENTILE(view_count, 0.50) as median_views,
# MAGIC   PERCENTILE(view_count, 0.75) as p75_views,
# MAGIC   PERCENTILE(view_count, 0.95) as p95_views
# MAGIC FROM ${bronze_table_name}
# MAGIC WHERE view_count IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### File Timestamp Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- File timestamp distribution for visualization
# MAGIC SELECT
# MAGIC   file_timestamp,
# MAGIC   COUNT(*) as record_count
# MAGIC FROM ${bronze_table_name}
# MAGIC GROUP BY file_timestamp
# MAGIC ORDER BY file_timestamp;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Volume by File

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data volume by source file for visualization
# MAGIC SELECT
# MAGIC   filename,
# MAGIC   COUNT(*) as record_count
# MAGIC FROM ${bronze_table_name}
# MAGIC GROUP BY filename
# MAGIC ORDER BY record_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Summary statistics overview
# MAGIC SELECT
# MAGIC   'Total Records' as metric,
# MAGIC   COUNT(*) as value
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Total Files Processed' as metric,
# MAGIC   COUNT(DISTINCT filename) as value
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Unique Projects' as metric,
# MAGIC   COUNT(DISTINCT project) as value
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Unique Page Titles' as metric,
# MAGIC   COUNT(DISTINCT page_title) as value
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Date Range Start' as metric,
# MAGIC   MIN(file_timestamp) as value
# MAGIC FROM ${bronze_table_name}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Date Range End' as metric,
# MAGIC   MAX(file_timestamp) as value
# MAGIC FROM ${bronze_table_name};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall data quality summary
# MAGIC SELECT
# MAGIC   'Records with Complete Data' as quality_metric,
# MAGIC   COUNT(*) as record_count,
# MAGIC   ROUND((COUNT(*) / (SELECT COUNT(*) FROM ${bronze_table_name})) * 100, 2) as percentage
# MAGIC FROM ${bronze_table_name}
# MAGIC WHERE
# MAGIC   project IS NOT NULL
# MAGIC   AND page_title IS NOT NULL
# MAGIC   AND view_count IS NOT NULL
# MAGIC   AND access_method IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Total Records' as quality_metric,
# MAGIC   COUNT(*) as record_count,
# MAGIC   100.0 as percentage
# MAGIC FROM ${bronze_table_name};

# COMMAND ----------

# Clean up and exit
dbutils.notebook.exit({
    "status": "success",
    "bronze_table": bronze_table_name,
    "record_count": bronze_table.count(),
    "message": "Bronze layer ingestion completed successfully"
})
