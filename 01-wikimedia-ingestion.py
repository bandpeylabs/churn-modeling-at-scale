# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Wikimedia Pageview Data Ingestion
# MAGIC
# MAGIC **Purpose**: Ingest raw Wikimedia pageview data and create the Bronze layer
# MAGIC
# MAGIC **Data Source**: Wikimedia pageviews dump (January 2025)
# MAGIC **Target**: Bronze layer Delta table for raw data preservation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Set up configuration
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
dbutils.widgets.text(
    "raw_data_path", "/tmp/wikimedia_pageviews", "Raw data path")
dbutils.widgets.text("bronze_table_name",
                     "bronze_wikimedia_pageviews", "Bronze table name")
dbutils.widgets.text("database_name", "churn_modeling", "Database name")

# Get widget values
raw_data_path = dbutils.widgets.get("raw_data_path")
bronze_table_name = dbutils.widgets.get("bronze_table_name")
database_name = dbutils.widgets.get("database_name")

print(f"Configuration:")
print(f"  Raw data path: {raw_data_path}")
print(f"  Bronze table: {bronze_table_name}")
print(f"  Database: {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Database and Check Data

# COMMAND ----------

# Create database if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

print(f"Using database: {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Sample Wikimedia Data
# MAGIC
# MAGIC For this exercise, we'll create sample data that mimics the Wikimedia pageview structure.
# MAGIC In production, you would download from: https://dumps.wikimedia.org/other/pageviews/2025/2025-01/

# COMMAND ----------

# Create sample Wikimedia pageview data
# This mimics the actual Wikimedia pageview format
sample_data = [
    ("2025-01-01", "en.wikipedia", "Main_Page", "desktop", 1500, "US"),
    ("2025-01-01", "en.wikipedia", "Main_Page", "mobile-web", 800, "US"),
    ("2025-01-01", "en.wikipedia",
     "Python_(programming_language)", "desktop", 200, "US"),
    ("2025-01-01", "en.wikipedia",
     "Python_(programming_language)", "mobile-web", 150, "US"),
    ("2025-01-01", "en.wikipedia", "Machine_learning", "desktop", 180, "US"),
    ("2025-01-01", "en.wikipedia", "Machine_learning", "mobile-web", 120, "US"),
    ("2025-01-01", "de.wikipedia", "Hauptseite", "desktop", 300, "DE"),
    ("2025-01-01", "de.wikipedia", "Hauptseite", "mobile-web", 200, "DE"),
    ("2025-01-02", "en.wikipedia", "Main_Page", "desktop", 1600, "US"),
    ("2025-01-02", "en.wikipedia", "Main_Page", "mobile-web", 850, "US"),
    ("2025-01-02", "en.wikipedia", "Data_science", "desktop", 220, "US"),
    ("2025-01-02", "en.wikipedia", "Data_science", "mobile-web", 180, "US"),
    ("2025-01-03", "en.wikipedia", "Main_Page", "desktop", 1400, "US"),
    ("2025-01-03", "en.wikipedia", "Main_Page", "mobile-web", 750, "US"),
    ("2025-01-03", "en.wikipedia", "Apache_Spark", "desktop", 250, "US"),
    ("2025-01-03", "en.wikipedia", "Apache_Spark", "mobile-web", 200, "US"),
]

# Create DataFrame with proper schema

schema = StructType([
    StructField("date", StringType(), True),
    StructField("project", StringType(), True),
    StructField("page_title", StringType(), True),
    StructField("access_method", StringType(), True),
    StructField("view_count", IntegerType(), True),
    StructField("country_code", StringType(), True)
])

df = spark.createDataFrame(sample_data, schema)

print("Sample data created:")
df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest into Bronze Layer
# MAGIC
# MAGIC The Bronze layer preserves raw data exactly as received with minimal transformation.

# COMMAND ----------

# Add ingestion timestamp

bronze_df = df.withColumn("ingestion_timestamp", current_timestamp()) \
              .withColumn("data_source", lit("wikimedia_pageviews")) \
              .withColumn("file_date", lit("2025-01"))

print("Bronze layer data with metadata:")
bronze_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table (Bronze Layer)

# COMMAND ----------

# Write to Delta table with partitioning
bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("file_date", "project") \
    .saveAsTable(f"{database_name}.{bronze_table_name}")

print(f"✅ Bronze layer created: {database_name}.{bronze_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Data Ingestion

# COMMAND ----------

# Read back and verify
bronze_table = spark.table(f"{database_name}.{bronze_table_name}")

print("Bronze table schema:")
bronze_table.printSchema()

print(f"\nTotal records: {bronze_table.count()}")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ **Bronze layer created successfully**
# MAGIC - Raw data preserved with minimal transformation
# MAGIC - Metadata added (ingestion timestamp, data source, file date)
# MAGIC - Partitioned by file_date and project for efficient querying
# MAGIC - Data quality checks completed
# MAGIC
# MAGIC **Next steps**:
# MAGIC - Process data in Silver layer (cleaning and validation)
# MAGIC - Create Gold layer (business-ready features)

# COMMAND ----------

# Clean up widgets
dbutils.widgets.removeAll()
