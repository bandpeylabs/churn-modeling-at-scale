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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Optimization & Partitioning
# MAGIC
# MAGIC With 5.3 billion records, we need to optimize the table for performance and efficient querying.

# COMMAND ----------

# Table optimization for large-scale data
print("Starting optimization for large-scale data (5.3B records)...")

# 1. Optimize table layout and compression
spark.sql(f"OPTIMIZE {bronze_table_name}")

# 2. Z-order clustering on frequently queried columns
spark.sql(
    f"OPTIMIZE {bronze_table_name} ZORDER BY (project, access_method, view_count)")

# 3. Vacuum old files (retain 7 days for safety)
spark.sql(f"VACUUM {bronze_table_name} RETAIN 168 HOURS")

# 4. Update table statistics for query optimization
spark.sql(f"ANALYZE TABLE {bronze_table_name} COMPUTE STATISTICS")
spark.sql(
    f"ANALYZE TABLE {bronze_table_name} COMPUTE STATISTICS FOR ALL COLUMNS")

print("Table optimization completed")

# COMMAND ----------

# Verify optimization results
optimized_files = spark.sql(
    f"DESCRIBE DETAIL {bronze_table_name}").collect()[0]
print(f"Optimized table details:")
print(f"Number of files: {optimized_files['numFiles']}")
print(f"Size in bytes: {optimized_files['sizeInBytes']:,}")
print(f"Partitioning columns: {optimized_files['partitionColumns']}")

# Show sample partition distribution
partition_stats = spark.sql(f"""
    SELECT 
        file_timestamp,
        project,
        COUNT(*) as record_count,
        COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {bronze_table_name}) as percentage
    FROM {bronze_table_name}
    GROUP BY file_timestamp, project
    ORDER BY record_count DESC
    LIMIT 10
""")

print("\nTop 10 partitions by record count:")
partition_stats.show()

# COMMAND ----------

# Clean up and exit
dbutils.notebook.exit({
    "status": "success",
    "bronze_table": bronze_table_name,
    "record_count": bronze_table.count(),
    "optimized": True,
    "partition_columns": ["file_timestamp", "project"],
    "message": "Bronze layer ingestion and optimization completed successfully"
})
