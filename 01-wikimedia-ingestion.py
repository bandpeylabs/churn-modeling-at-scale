# Databricks notebook source

# COMMAND ----------

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
from datetime import datetime, timedelta
from pyspark.sql.functions import current_timestamp, lit, input_file_name, col, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Download URLs

# COMMAND ----------

# Generate URLs using simple iteration
# Using variables loaded from environment notebook
base_url = config['data_sources']['wikimedia']['base_url']
start_date = config['data_sources']['wikimedia']['start_date']
end_date = config['data_sources']['wikimedia']['end_date']

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
            'filename': filename,
            'date': current_dt.strftime('%Y-%m-%d'),
            'hour': hour_str
        })
    current_dt += timedelta(days=1)

print(f"Generated {len(urls)} download URLs")
print("Sample URLs:")
for url_info in urls[:3]:
    print(f"  {url_info['filename']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Files Using Spark

# COMMAND ----------

# Use Spark to download files to volume
# This is more efficient than Python requests for large files
download_count = 0
max_downloads = 5  # Can be increased for more files

print(
    f"Starting download of {max_downloads} files from {len(urls)} total available files")

for url_info in urls[:max_downloads]:
    url = url_info['url']
    filename = url_info['filename']
    local_path = f"{volume_path}/{filename}"

    print(f"Downloading: {filename}")
    try:
        # Use Spark to download file
        spark.sparkContext.addFile(url)
        # Copy from Spark temp location to volume
        dbutils.fs.cp(
            f"file://{spark.sparkContext.getLocalFile(filename)}", local_path)
        download_count += 1
        print(f"Downloaded: {filename}")
    except Exception as e:
        print(f"Failed: {filename} - {e}")

print(f"Downloaded {download_count}/{max_downloads} files")
print(f"Total files available: {len(urls)} (31 days × 24 hours = 744 files)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Spark Schema for Wikimedia Data

# MAGIC Based on the sample data structure:
# MAGIC `"project page_title view_count access_method"`
# MAGIC
# MAGIC Example: `"en.wikipedia Main_Page 1500 desktop"`

# COMMAND ----------

# Define schema for Wikimedia pageview data
wikimedia_schema = StructType([
    # e.g., "en.wikipedia", "de.wikipedia"
    StructField("project", StringType(), True),
    # e.g., "Main_Page", "Python_(programming_language)"
    StructField("page_title", StringType(), True),
    StructField("view_count", IntegerType(), True),      # Number of pageviews
    # e.g., "desktop", "mobile-web"
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

print("Data parsed and structured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Metadata and Write to Bronze Table

# COMMAND ----------

# Add metadata columns
final_bronze_df = parsed_df.withColumn("data_source", lit("wikimedia_pageviews")) \
                           .withColumn("file_date", lit("2025-01"))

# Get bronze table name from config
bronze_table_name = config['tables']['bronze']['wikimedia_pageviews']

print(f"Writing to bronze table: {bronze_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table (Bronze Layer)

# COMMAND ----------

# Write to Delta table with partitioning
final_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("file_date", "project") \
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
