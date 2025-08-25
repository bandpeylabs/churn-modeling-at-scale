# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Engineering & Engagement KPIs
# MAGIC
# MAGIC **Purpose**: Transform raw behavioral data into actionable features for churn prediction
# MAGIC
# MAGIC **Key Objectives**:
# MAGIC - Compute engagement KPIs (DAU/WAU/MAU, session length, content diversity)
# MAGIC - Create behavioral pattern features
# MAGIC - Engineer subscription lifecycle features
# MAGIC - Prepare feature matrix for modeling

# COMMAND ----------

# MAGIC %run ./config/01_env_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering Overview
# MAGIC
# MAGIC We'll transform the raw behavioral and subscription data into a comprehensive feature matrix that captures user engagement patterns, behavioral trends, and subscription characteristics.

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Bronze Layer Data

# COMMAND ----------

# Load bronze layer tables
user_behavior_table = config['tables']['bronze']['user_behavior']
subscription_table = config['tables']['bronze']['subscription_data']

user_behavior_df = spark.table(user_behavior_table)
subscription_df = spark.table(subscription_table)

print(f"User behavior data loaded: {user_behavior_df.count():,} records")
print(f"Subscription data loaded: {subscription_df.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compute Engagement KPIs

# COMMAND ----------

# MAGIC %md
# MAGIC ### Daily Active Users (DAU)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily Active Users calculation
# MAGIC WITH daily_users AS (
# MAGIC   SELECT
# MAGIC     DATE(timestamp) as date,
# MAGIC     COUNT(DISTINCT user_id) as dau
# MAGIC   FROM bronze_user_behavior
# MAGIC   GROUP BY DATE(timestamp)
# MAGIC )
# MAGIC SELECT
# MAGIC   date,
# MAGIC   dau,
# MAGIC   LAG(dau, 1) OVER (ORDER BY date) as dau_prev_day,
# MAGIC   ROUND((dau - LAG(dau, 1) OVER (ORDER BY date)) * 100.0 / LAG(dau, 1) OVER (ORDER BY date), 2) as dau_change_pct
# MAGIC FROM daily_users
# MAGIC ORDER BY date;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Weekly Active Users (WAU)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Weekly Active Users calculation
# MAGIC WITH weekly_users AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('week', timestamp) as week_start,
# MAGIC     COUNT(DISTINCT user_id) as wau
# MAGIC   FROM bronze_user_behavior
# MAGIC   GROUP BY DATE_TRUNC('week', timestamp)
# MAGIC )
# MAGIC SELECT
# MAGIC   week_start,
# MAGIC   wau,
# MAGIC   LAG(wau, 1) OVER (ORDER BY week_start) as wau_prev_week,
# MAGIC   ROUND((wau - LAG(wau, 1) OVER (ORDER BY week_start)) * 100.0 / LAG(wau, 1) OVER (ORDER BY week_start), 2) as wau_change_pct
# MAGIC FROM weekly_users
# MAGIC ORDER BY week_start;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monthly Active Users calculation
# MAGIC WITH monthly_users AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('month', timestamp) as month_start,
# MAGIC     COUNT(DISTINCT user_id) as mau
# MAGIC   FROM bronze_user_behavior
# MAGIC   GROUP BY DATE_TRUNC('month', timestamp)
# MAGIC )
# MAGIC SELECT
# MAGIC   month_start,
# MAGIC   mau,
# MAGIC   LAG(mau, 1) OVER (ORDER BY month_start) as mau_prev_month,
# MAGIC   ROUND((mau - LAG(mau, 1) OVER (ORDER BY month_start)) * 100.0 / LAG(mau, 1) OVER (ORDER BY month_start), 2) as mau_change_pct
# MAGIC FROM monthly_users
# MAGIC ORDER BY month_start;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Session Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Session length and frequency analysis
# MAGIC WITH session_metrics AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     session_id,
# MAGIC     COUNT(*) as actions_per_session,
# MAGIC     SUM(COALESCE(duration_seconds, 0)) as session_duration_seconds,
# MAGIC     MAX(timestamp) - MIN(timestamp) as session_time_span
# MAGIC   FROM ${user_behavior_table}
# MAGIC   WHERE session_id IS NOT NULL
# MAGIC   GROUP BY user_id, session_id
# MAGIC )
# MAGIC SELECT
# MAGIC   ROUND(AVG(actions_per_session), 2) as avg_actions_per_session,
# MAGIC   ROUND(AVG(session_duration_seconds), 2) as avg_session_duration_seconds,
# MAGIC   ROUND(AVG(session_time_span), 2) as avg_session_time_span,
# MAGIC   ROUND(STDDEV(actions_per_session), 2) as stddev_actions_per_session,
# MAGIC   ROUND(STDDEV(session_duration_seconds), 2) as stddev_session_duration,
# MAGIC   PERCENTILE(actions_per_session, 0.25) as p25_actions,
# MAGIC   PERCENTILE(actions_per_session, 0.50) as median_actions,
# MAGIC   PERCENTILE(actions_per_session, 0.75) as p75_actions,
# MAGIC   PERCENTILE(actions_per_session, 0.95) as p95_actions
# MAGIC FROM session_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Adoption Analysis

# MAGIC %sql
# MAGIC -- Feature adoption by user
# MAGIC WITH feature_adoption AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     COUNT(DISTINCT feature_name) as features_used,
# MAGIC     COUNT(CASE WHEN action_type = 'feature_use' THEN 1 END) as total_feature_uses,
# MAGIC     COUNT(CASE WHEN action_type = 'feature_use' THEN 1 END) * 100.0 / COUNT(*) as feature_usage_ratio
# MAGIC   FROM ${user_behavior_table}
# MAGIC   WHERE feature_name IS NOT NULL
# MAGIC   GROUP BY user_id
# MAGIC )
# MAGIC SELECT
# MAGIC   features_used,
# MAGIC   COUNT(*) as user_count,
# MAGIC   ROUND(AVG(total_feature_uses), 2) as avg_feature_uses,
# MAGIC   ROUND(AVG(feature_usage_ratio), 2) as avg_feature_usage_ratio
# MAGIC FROM feature_adoption
# MAGIC GROUP BY features_used
# MAGIC ORDER BY features_used;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create User-Level Features

# MAGIC Now we'll create a comprehensive feature matrix at the user level for churn modeling.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Engagement Features

# COMMAND ----------

# Create engagement features DataFrame
engagement_features = user_behavior_df.groupBy("user_id").agg(
    # Activity frequency
    count("*").alias("total_actions"),
    countDistinct("session_id").alias("total_sessions"),
    countDistinct("date(timestamp)").alias("active_days"),

    # Feature usage
    countDistinct("feature_name").alias("features_adopted"),
    sum(when(col("action_type") == "feature_use", 1).otherwise(
        0)).alias("feature_usage_count"),

    # Session metrics
    avg("duration_seconds").alias("avg_session_duration"),
    stddev("duration_seconds").alias("stddev_session_duration"),

    # Device diversity
    countDistinct("device_type").alias("device_types_used"),

    # Location diversity
    countDistinct("location").alias("locations_used"),

    # Recent activity (last 7 days)
    sum(when(col("timestamp") >= date_sub(current_timestamp(), 7),
        1).otherwise(0)).alias("actions_last_7_days"),

    # Recent activity (last 30 days)
    sum(when(col("timestamp") >= date_sub(current_timestamp(), 30),
        1).otherwise(0)).alias("actions_last_30_days")
)

print("Engagement features created")
engagement_features.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Behavioral Pattern Features

# MAGIC %sql
# MAGIC -- Create behavioral pattern features
# MAGIC WITH user_patterns AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     action_type,
# MAGIC     COUNT(*) as action_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY user_id), 2) as action_percentage
# MAGIC   FROM ${user_behavior_table}
# MAGIC   GROUP BY user_id, action_type
# MAGIC ),
# MAGIC user_behavior_summary AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     MAX(CASE WHEN action_type = 'login' THEN action_percentage ELSE 0 END) as login_percentage,
# MAGIC     MAX(CASE WHEN action_type = 'page_view' THEN action_percentage ELSE 0 END) as page_view_percentage,
# MAGIC     MAX(CASE WHEN action_type = 'feature_use' THEN action_percentage ELSE 0 END) as feature_use_percentage,
# MAGIC     MAX(CASE WHEN action_type = 'support_ticket' THEN action_percentage ELSE 0 END) as support_ticket_percentage
# MAGIC   FROM user_patterns
# MAGIC   GROUP BY user_id
# MAGIC )
# MAGIC SELECT * FROM user_behavior_summary LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Subscription Lifecycle Features

# MAGIC %sql
# MAGIC -- Create subscription lifecycle features
# MAGIC SELECT
# MAGIC   user_id,
# MAGIC   plan_type,
# MAGIC   billing_cycle,
# MAGIC   payment_method,
# MAGIC   subscription_start,
# MAGIC   subscription_end,
# MAGIC   is_churned,
# MAGIC   churn_reason,
# MAGIC   DATEDIFF(CURRENT_DATE(), subscription_start) as days_since_subscription,
# MAGIC   CASE
# MAGIC     WHEN subscription_end IS NOT NULL THEN DATEDIFF(subscription_end, subscription_start)
# MAGIC     ELSE DATEDIFF(CURRENT_DATE(), subscription_start)
# MAGIC   END as subscription_length_days,
# MAGIC   CASE
# MAGIC     WHEN subscription_end IS NOT NULL THEN 'churned'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 30 THEN 'new'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 90 THEN 'early'
# MAGIC     ELSE 'established'
# MAGIC   END as subscription_stage
# MAGIC FROM ${subscription_table}
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Feature Engineering Pipeline

# MAGIC Now let's create a comprehensive feature engineering pipeline that combines all features.

# COMMAND ----------

# Create comprehensive feature matrix


def create_feature_matrix(user_behavior_df, subscription_df):
    """Create comprehensive feature matrix for churn modeling"""

    # 1. Engagement features
    engagement_features = user_behavior_df.groupBy("user_id").agg(
        # Activity metrics
        count("*").alias("total_actions"),
        countDistinct("session_id").alias("total_sessions"),
        countDistinct("date(timestamp)").alias("active_days"),

        # Feature adoption
        countDistinct("feature_name").alias("features_adopted"),
        sum(when(col("action_type") == "feature_use", 1).otherwise(
            0)).alias("feature_usage_count"),

        # Session quality
        avg("duration_seconds").alias("avg_session_duration"),
        stddev("duration_seconds").alias("stddev_session_duration"),

        # Diversity metrics
        countDistinct("device_type").alias("device_types_used"),
        countDistinct("location").alias("locations_used"),

        # Recency metrics
        sum(when(col("timestamp") >= date_sub(current_timestamp(), 7),
            1).otherwise(0)).alias("actions_last_7_days"),
        sum(when(col("timestamp") >= date_sub(current_timestamp(), 30),
            1).otherwise(0)).alias("actions_last_30_days"),

        # Frequency metrics
        sum(when(col("timestamp") >= date_sub(current_timestamp(), 7),
            1).otherwise(0)).alias("actions_last_week"),
        sum(when(col("timestamp") >= date_sub(current_timestamp(), 14),
            1).otherwise(0)).alias("actions_last_2_weeks"),
        sum(when(col("timestamp") >= date_sub(current_timestamp(), 30),
            1).otherwise(0)).alias("actions_last_month")
    )

    # 2. Behavioral pattern features
    behavioral_features = user_behavior_df.groupBy("user_id").pivot("action_type").agg(
        count("*").alias("count")
    ).fillna(0)

    # Rename columns for clarity
    behavioral_features = behavioral_features.select(
        col("user_id"),
        col("login").alias("login_count"),
        col("page_view").alias("page_view_count"),
        col("feature_use").alias("feature_use_count"),
        col("support_ticket").alias("support_ticket_count")
    )

    # 3. Feature usage patterns
    feature_usage = user_behavior_df.filter(col("action_type") == "feature_use").groupBy("user_id").pivot("feature_name").agg(
        count("*").alias("usage_count")
    ).fillna(0)

    # 4. Subscription features
    subscription_features = subscription_df.select(
        col("user_id"),
        col("plan_type"),
        col("billing_cycle"),
        col("payment_method"),
        col("subscription_start"),
        col("subscription_end"),
        col("is_churned"),
        col("churn_reason"),
        datediff(current_date(), col("subscription_start")
                 ).alias("days_since_subscription"),
        when(col("subscription_end").isNotNull(),
             datediff(col("subscription_end"), col("subscription_start")))
        .otherwise(datediff(current_date(), col("subscription_start")))
        .alias("subscription_length_days")
    )

    # 5. Join all features
    feature_matrix = engagement_features \
        .join(behavioral_features, "user_id", "left") \
        .join(feature_usage, "user_id", "left") \
        .join(subscription_features, "user_id", "left")

    # 6. Add derived features
    feature_matrix = feature_matrix.withColumn(
        "avg_actions_per_session",
        when(col("total_sessions") > 0, col(
            "total_actions") / col("total_sessions"))
        .otherwise(0)
    ).withColumn(
        "avg_actions_per_day",
        when(col("active_days") > 0, col("total_actions") / col("active_days"))
        .otherwise(0)
    ).withColumn(
        "feature_usage_ratio",
        when(col("total_actions") > 0, col("feature_usage_count")
             * 100.0 / col("total_actions"))
        .otherwise(0)
    ).withColumn(
        "support_ticket_ratio",
        when(col("total_actions") > 0, col("support_ticket_count")
             * 100.0 / col("total_actions"))
        .otherwise(0)
    ).withColumn(
        "subscription_stage",
        when(col("subscription_length_days") <= 30, "new")
        .when(col("subscription_length_days") <= 90, "early")
        .when(col("subscription_length_days") <= 365, "established")
        .otherwise("veteran")
    )

    return feature_matrix


# Create the feature matrix
print("Creating comprehensive feature matrix...")
feature_matrix = create_feature_matrix(user_behavior_df, subscription_df)

print(
    f"Feature matrix created with {feature_matrix.count():,} users and {len(feature_matrix.columns)} features")
print("\nFeature matrix schema:")
feature_matrix.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Matrix Preview

# COMMAND ----------

# Show sample of feature matrix
print("Sample feature matrix:")
feature_matrix.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Statistics

# MAGIC %sql
# MAGIC -- Feature matrix statistics
# MAGIC SELECT
# MAGIC   'total_actions' as feature_name,
# MAGIC   COUNT(*) as total_users,
# MAGIC   ROUND(AVG(total_actions), 2) as mean_value,
# MAGIC   ROUND(STDDEV(total_actions), 2) as stddev_value,
# MAGIC   MIN(total_actions) as min_value,
# MAGIC   MAX(total_actions) as max_value,
# MAGIC   PERCENTILE(total_actions, 0.25) as p25_value,
# MAGIC   PERCENTILE(total_actions, 0.50) as median_value,
# MAGIC   PERCENTILE(total_actions, 0.75) as p75_value,
# MAGIC   PERCENTILE(total_actions, 0.95) as p95_value
# MAGIC FROM ${feature_matrix_table}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'features_adopted' as feature_name,
# MAGIC   COUNT(*) as total_users,
# MAGIC   ROUND(AVG(features_adopted), 2) as mean_value,
# MAGIC   ROUND(STDDEV(features_adopted), 2) as stddev_value,
# MAGIC   MIN(features_adopted) as min_value,
# MAGIC   MAX(features_adopted) as max_value,
# MAGIC   PERCENTILE(features_adopted, 0.25) as p25_value,
# MAGIC   PERCENTILE(features_adopted, 0.50) as median_value,
# MAGIC   PERCENTILE(features_adopted, 0.75) as p75_value,
# MAGIC   PERCENTILE(features_adopted, 0.95) as p95_value
# MAGIC FROM ${feature_matrix_table}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'subscription_length_days' as feature_name,
# MAGIC   COUNT(*) as total_users,
# MAGIC   ROUND(AVG(subscription_length_days), 2) as mean_value,
# MAGIC   ROUND(STDDEV(subscription_length_days), 2) as stddev_value,
# MAGIC   MIN(subscription_length_days) as min_value,
# MAGIC   MAX(subscription_length_days) as max_value,
# MAGIC   PERCENTILE(subscription_length_days, 0.25) as p25_value,
# MAGIC   PERCENTILE(subscription_length_days, 0.50) as median_value,
# MAGIC   PERCENTILE(subscription_length_days, 0.75) as p75_value,
# MAGIC   PERCENTILE(subscription_length_days, 0.95) as p95_value
# MAGIC FROM ${feature_matrix_table};

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Feature Correlation Analysis

# MAGIC %sql
# MAGIC -- Feature correlation with churn
# MAGIC SELECT
# MAGIC   'total_actions' as feature_name,
# MAGIC   ROUND(CORR(total_actions, is_churned), 4) as correlation_with_churn
# MAGIC FROM ${feature_matrix_table}
# MAGIC WHERE is_churned IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'features_adopted' as feature_name,
# MAGIC   ROUND(CORR(features_adopted, is_churned), 4) as correlation_with_churn
# MAGIC FROM ${feature_matrix_table}
# MAGIC WHERE is_churned IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'avg_actions_per_session' as feature_name,
# MAGIC   ROUND(CORR(avg_actions_per_session, is_churn), 4) as correlation_with_churn
# MAGIC FROM ${feature_matrix_table}
# MAGIC WHERE is_churned IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'support_ticket_ratio' as feature_name,
# MAGIC   ROUND(CORR(support_ticket_ratio, is_churn), 4) as correlation_with_churn
# MAGIC FROM ${feature_matrix_table}
# MAGIC WHERE is_churned IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'subscription_length_days' as feature_name,
# MAGIC   ROUND(CORR(subscription_length_days, is_churn), 4) as correlation_with_churn
# MAGIC FROM ${feature_matrix_table}
# MAGIC WHERE is_churned IS NOT NULL
# MAGIC
# MAGIC ORDER BY ABS(correlation_with_churn) DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Features to Silver Layer

# COMMAND ----------

# Write feature matrix to silver layer
user_features_table = config['tables']['silver']['user_features']
feature_matrix.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("subscription_stage") \
    .saveAsTable(user_features_table)

print(f"Feature matrix written to silver layer: {user_features_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Feature Quality Assessment

# MAGIC %sql
# MAGIC -- Feature quality summary
# MAGIC SELECT
# MAGIC   COUNT(*) as total_users,
# MAGIC   SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_users,
# MAGIC   SUM(CASE WHEN is_churned = false THEN 1 ELSE 0 END) as active_users,
# MAGIC   ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate_percentage
# MAGIC FROM ${user_features_table};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature completeness check
# MAGIC SELECT
# MAGIC   'total_actions' as feature_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(total_actions) as non_null_count,
# MAGIC   COUNT(*) - COUNT(total_actions) as null_count,
# MAGIC   ROUND(((COUNT(*) - COUNT(total_actions)) / COUNT(*)) * 100, 2) as null_percentage
# MAGIC FROM ${user_features_table}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'features_adopted' as feature_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(features_adopted) as non_null_count,
# MAGIC   COUNT(*) - COUNT(features_adopted) as null_count,
# MAGIC   ROUND(((COUNT(*) - COUNT(features_adopted)) / COUNT(*)) * 100, 2) as null_percentage
# MAGIC FROM ${user_features_table}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'subscription_length_days' as feature_name,
# MAGIC   COUNT(*) as total_records,
# MAGIC   COUNT(subscription_length_days) as non_null_count,
# MAGIC   COUNT(*) - COUNT(subscription_length_days) as null_count,
# MAGIC   ROUND(((COUNT(*) - COUNT(subscription_length_days)) / COUNT(*)) * 100, 2) as null_percentage
# MAGIC FROM ${user_features_table}
# MAGIC
# MAGIC ORDER BY feature_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Summary & Next Steps

# MAGIC %sql
# MAGIC -- Feature engineering summary
# MAGIC SELECT
# MAGIC   'Feature Matrix' as component,
# MAGIC   COUNT(*) as user_count,
# MAGIC   COUNT(*) as feature_count,
# MAGIC   'Ready for modeling' as status
# MAGIC FROM ${user_features_table}
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Churn Distribution' as component,
# MAGIC   SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC   SUM(CASE WHEN is_churned = false THEN 1 ELSE 0 END) as active_count,
# MAGIC   CONCAT(ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2), '%') as churn_rate
# MAGIC FROM ${user_features_table};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Achievements & Next Steps
# MAGIC
# MAGIC **Feature Engineering Complete**: We've successfully created a comprehensive feature matrix with:
# MAGIC
# MAGIC **Engagement KPIs**:
# MAGIC - DAU/WAU/MAU calculations
# MAGIC - Session length and frequency metrics
# MAGIC - Feature adoption patterns
# MAGIC
# MAGIC **Behavioral Features**:
# MAGIC - Action type distributions
# MAGIC - Device and location diversity
# MAGIC - Recency and frequency metrics
# MAGIC
# MAGIC **Subscription Features**:
# MAGIC - Lifecycle stage classification
# MAGIC - Plan and billing characteristics
# MAGIC - Churn indicators
# MAGIC
# MAGIC **Next Steps**: Proceed to churn modeling with the prepared feature matrix.

# COMMAND ----------

# Clean up and exit
dbutils.notebook.exit({
    "status": "success",
    "feature_matrix_table": user_features_table,
    "total_features": len(feature_matrix.columns),
    "total_users": feature_matrix.count(),
    "message": "Feature engineering completed successfully. Feature matrix ready for churn modeling."
})
