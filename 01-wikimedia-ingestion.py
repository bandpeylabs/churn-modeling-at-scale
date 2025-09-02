# Databricks notebook source
# MAGIC %md
# MAGIC # Wikimedia Data Ingestion
# MAGIC
# MAGIC Downloads and ingests Wikimedia pageview data using daily sampling strategy

# COMMAND ----------

# MAGIC %run ./config/01_env_config

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, regexp_extract, rand, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp, lit, input_file_name, col, split
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Sampling Strategy
# MAGIC
# MAGIC Pick one random file per day during peak hours (12-16 UTC) to reduce data volume

# COMMAND ----------

# Extract date and hour from filename for grouping
files_with_datetime = urls_df.filter(
    (col("filename").like("pageviews-%")) &
    (col("file_size_bytes") > 0)
).withColumn(
    "date", regexp_extract(col("filename"), r"pageviews-(\d{8})", 1)
).withColumn(
    "hour", regexp_extract(
        col("filename"), r"pageviews-\d{8}-(\d{2})", 1).cast("int")
).filter(
    col("hour").between(12, 16)  # Peak hours
)

print(f"Found {files_with_datetime.count()} files during peak hours")

# Group by date and pick one random file per day
window_spec = Window.partitionBy("date").orderBy(rand(seed=42))
daily_sampled_files = files_with_datetime.withColumn(
    "row_number", row_number().over(window_spec)
).filter(col("row_number") == 1).drop("date", "hour", "row_number")

files_to_download = daily_sampled_files.orderBy("timestamp")
print(f"Daily sampling completed: {files_to_download.count()} files selected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Files

# COMMAND ----------

# Download files to volume
download_results = []
for row in files_to_download.collect():
    filename = row['filename']
    full_url = row['full_url']
    volume_file_path = f"{volume_path}/{filename}"

    try:
        dbutils.fs.cp(full_url, volume_file_path)
        download_results.append({'filename': filename, 'status': 'success'})
        print(f"Downloaded: {filename}")
    except Exception as e:
        download_results.append(
            {'filename': filename, 'status': 'failed', 'error': str(e)})
        print(f"Failed: {filename} - {e}")

print(
    f"Download completed: {len([r for r in download_results if r['status'] == 'success'])}/{len(download_results)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Data

# COMMAND ----------

# Define schema
wikimedia_schema = StructType([
    StructField("project", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("view_count", IntegerType(), True),
    StructField("access_method", StringType(), True)
])

# Load and parse data
bronze_df = spark.read.format("text").option(
    "compression", "gzip").load(f"{volume_path}/*.gz")

parsed_df = bronze_df.select(
    split(col("value"), " ").getItem(0).alias("project"),
    split(col("value"), " ").getItem(1).alias("page_title"),
    split(col("value"), " ").getItem(2).cast("int").alias("view_count"),
    split(col("value"), " ").getItem(3).alias("access_method"),
    input_file_name().alias("source_file"),
    current_timestamp().alias("ingestion_timestamp")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enrich and Write

# COMMAND ----------

# Enrich with metadata
enriched_df = parsed_df.join(
    urls_df.select("filename", "timestamp", "file_size_bytes", "full_url"),
    on="filename",
    how="left"
).select(
    col("project"), col("page_title"), col("view_count"), col("access_method"),
    col("source_file"), col("ingestion_timestamp"),
    col("timestamp").alias("file_timestamp"), col(
        "file_size_bytes"), col("full_url").alias("source_url")
).withColumn("data_source", lit("wikimedia_pageviews"))

# Write to bronze
bronze_table_name = config['tables']['bronze']['wikimedia_pageviews']
enriched_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(
    "file_timestamp", "project").saveAsTable(bronze_table_name)

print(f"Bronze table created: {bronze_table_name}")
print(f"Records ingested: {enriched_df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Table

# COMMAND ----------

# Optimize for performance
spark.sql(f"OPTIMIZE {bronze_table_name}")
spark.sql(
    f"OPTIMIZE {bronze_table_name} ZORDER BY (project, access_method, view_count)")
spark.sql(f"ANALYZE TABLE {bronze_table_name} COMPUTE STATISTICS")

print("Table optimization completed")

# COMMAND ----------

# Ingestion completed successfully
print(f"Bronze table created: {bronze_table_name}")
print(f"Records ingested: {enriched_df.count():,}")
print("Ingestion completed successfully")
