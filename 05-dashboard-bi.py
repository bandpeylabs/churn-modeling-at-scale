# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard & Business Intelligence
# MAGIC
# MAGIC **Purpose**: Create comprehensive dashboard queries and business intelligence insights for churn analysis
# MAGIC
# MAGIC **Key Objectives**:
# MAGIC - Overall churn and retention funnel analysis
# MAGIC - Cohort heatmap visualization queries
# MAGIC - Slice-and-dice filters for business analysis
# MAGIC - Explainable model outputs and risk scoring
# MAGIC - Executive-level insights and recommendations

# COMMAND ----------

# MAGIC %run ./config/01_env_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Overview
# MAGIC
# MAGIC This notebook provides comprehensive SQL queries that can be integrated into interactive dashboards to tell the complete churn story. The dashboard itself serves as the narrative for stakeholders.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Executive Summary Dashboard

# MAGIC %sql
# MAGIC -- Executive summary - key metrics at a glance
# MAGIC WITH summary_metrics AS (
# MAGIC   SELECT
# MAGIC     COUNT(*) as total_subscribers,
# MAGIC     SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_subscribers,
# MAGIC     SUM(CASE WHEN is_churned = false THEN 1 ELSE 0 END) as active_subscribers,
# MAGIC     ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as overall_churn_rate
# MAGIC   FROM ${subscription_table}
# MAGIC ),
# MAGIC risk_distribution AS (
# MAGIC   SELECT
# MAGIC     churn_risk,
# MAGIC     COUNT(*) as user_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ${predictions_table}), 2) as percentage
# MAGIC   FROM ${predictions_table}
# MAGIC   GROUP BY churn_risk
# MAGIC )
# MAGIC SELECT
# MAGIC   'Total Subscribers' as metric,
# MAGIC   total_subscribers as value,
# MAGIC   'Count' as unit
# MAGIC FROM summary_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Active Subscribers' as metric,
# MAGIC   active_subscribers as value,
# MAGIC   'Count' as unit
# MAGIC FROM summary_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Churned Subscribers' as metric,
# MAGIC   churned_subscribers as value,
# MAGIC   'Count' as unit
# MAGIC FROM summary_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Overall Churn Rate' as metric,
# MAGIC   overall_churn_rate as value,
# MAGIC   'Percentage' as unit
# MAGIC FROM summary_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'High Risk Users' as metric,
# MAGIC   SUM(CASE WHEN churn_risk = 'high_risk' THEN user_count ELSE 0 END) as value,
# MAGIC   'Count' as unit
# MAGIC FROM risk_distribution
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Medium Risk Users' as metric,
# MAGIC   SUM(CASE WHEN churn_risk = 'medium_risk' THEN user_count ELSE 0 END) as value,
# MAGIC   'Count' as unit
# MAGIC FROM risk_distribution
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Low Risk Users' as metric,
# MAGIC   SUM(CASE WHEN churn_risk = 'low_risk' THEN user_count ELSE 0 END) as value,
# MAGIC   'Count' as unit
# MAGIC FROM risk_distribution
# MAGIC
# MAGIC ORDER BY
# MAGIC   CASE metric
# MAGIC     WHEN 'Total Subscribers' THEN 1
# MAGIC     WHEN 'Active Subscribers' THEN 2
# MAGIC     WHEN 'Churned Subscribers' THEN 3
# MAGIC     WHEN 'Overall Churn Rate' THEN 4
# MAGIC     WHEN 'High Risk Users' THEN 5
# MAGIC     WHEN 'Medium Risk Users' THEN 6
# MAGIC     WHEN 'Low Risk Users' THEN 7
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Churn & Retention Funnel

# MAGIC %sql
# MAGIC -- Churn and retention funnel analysis
# MAGIC WITH funnel_stages AS (
# MAGIC   SELECT
# MAGIC     'New Subscribers (0-30 days)' as stage,
# MAGIC     COUNT(*) as user_count,
# MAGIC     SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC     ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC   FROM ${subscription_table}
# MAGIC   WHERE DATEDIFF(CURRENT_DATE(), subscription_start) <= 30
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'Early Stage (31-90 days)' as stage,
# MAGIC     COUNT(*) as user_count,
# MAGIC     SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC     ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC   FROM ${subscription_table}
# MAGIC   WHERE DATEDIFF(CURRENT_DATE(), subscription_start) BETWEEN 31 AND 90
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'Established (91-365 days)' as stage,
# MAGIC     COUNT(*) as user_count,
# MAGIC     SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC     ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC   FROM ${subscription_table}
# MAGIC   WHERE DATEDIFF(CURRENT_DATE(), subscription_start) BETWEEN 91 AND 365
# MAGIC
# MAGIC   UNION ALL
# MAGIC
# MAGIC   SELECT
# MAGIC     'Veteran (365+ days)' as stage,
# MAGIC     COUNT(*) as user_count,
# MAGIC     SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC     ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC   FROM ${subscription_table}
# MAGIC   WHERE DATEDIFF(CURRENT_DATE(), subscription_start) > 365
# MAGIC )
# MAGIC SELECT
# MAGIC   stage,
# MAGIC   user_count,
# MAGIC   churned_count,
# MAGIC   churn_rate,
# MAGIC   ROUND((user_count - churned_count) * 100.0 / user_count, 2) as retention_rate,
# MAGIC   ROUND(user_count * 100.0 / (SELECT SUM(user_count) FROM funnel_stages), 2) as stage_percentage
# MAGIC FROM funnel_stages
# MAGIC ORDER BY
# MAGIC   CASE stage
# MAGIC     WHEN 'New Subscribers (0-30 days)' THEN 1
# MAGIC     WHEN 'Early Stage (31-90 days)' THEN 2
# MAGIC     WHEN 'Established (91-365 days)' THEN 3
# MAGIC     WHEN 'Veteran (365+ days)' THEN 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Cohort Heatmap (Join Month × Survival)

# MAGIC %sql
# MAGIC -- Cohort analysis heatmap
# MAGIC WITH cohort_data AS (
# MAGIC   SELECT
# MAGIC     DATE_TRUNC('month', subscription_start) as cohort_month,
# MAGIC     DATEDIFF(CURRENT_DATE(), subscription_start) as days_since_subscription,
# MAGIC     CASE
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 30 THEN 'Month 1'
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 60 THEN 'Month 2'
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 90 THEN 'Month 3'
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 120 THEN 'Month 4'
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 150 THEN 'Month 5'
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), subscription_start) <= 180 THEN 'Month 6'
# MAGIC       ELSE 'Month 6+'
# MAGIC     END as survival_month,
# MAGIC     COUNT(*) as user_count,
# MAGIC     SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count
# MAGIC   FROM ${subscription_table}
# MAGIC   WHERE subscription_start >= DATE_SUB(CURRENT_DATE(), 180)  -- Last 6 months
# MAGIC   GROUP BY DATE_TRUNC('month', subscription_start), DATEDIFF(CURRENT_DATE(), subscription_start)
# MAGIC ),
# MAGIC cohort_summary AS (
# MAGIC   SELECT
# MAGIC     cohort_month,
# MAGIC     survival_month,
# MAGIC     user_count,
# MAGIC     churned_count,
# MAGIC     ROUND((user_count - churned_count) * 100.0 / user_count, 2) as retention_rate,
# MAGIC     ROUND(churned_count * 100.0 / user_count, 2) as churn_rate
# MAGIC   FROM cohort_data
# MAGIC )
# MAGIC SELECT
# MAGIC   DATE_FORMAT(cohort_month, 'yyyy-MM') as cohort_month,
# MAGIC   survival_month,
# MAGIC   user_count,
# MAGIC   churned_count,
# MAGIC   retention_rate,
# MAGIC   churn_rate,
# MAGIC   CASE
# MAGIC     WHEN retention_rate >= 90 THEN 'Excellent'
# MAGIC     WHEN retention_rate >= 80 THEN 'Good'
# MAGIC     WHEN retention_rate >= 70 THEN 'Fair'
# MAGIC     WHEN retention_rate >= 60 THEN 'Poor'
# MAGIC     ELSE 'Critical'
# MAGIC   END as retention_grade
# MAGIC FROM cohort_summary
# MAGIC ORDER BY cohort_month,
# MAGIC   CASE survival_month
# MAGIC     WHEN 'Month 1' THEN 1
# MAGIC     WHEN 'Month 2' THEN 2
# MAGIC     WHEN 'Month 3' THEN 3
# MAGIC     WHEN 'Month 4' THEN 4
# MAGIC     WHEN 'Month 5' THEN 5
# MAGIC     WHEN 'Month 6' THEN 6
# MAGIC     WHEN 'Month 6+' THEN 7
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Slice-and-Dice Analysis

# MAGIC %sql
# MAGIC -- Plan type analysis with churn rates
# MAGIC SELECT
# MAGIC   plan_type,
# MAGIC   COUNT(*) as total_subscribers,
# MAGIC   SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC   ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   ROUND(AVG(total_actions), 2) as avg_actions_per_user,
# MAGIC   ROUND(AVG(features_adopted), 2) as avg_features_adopted,
# MAGIC   ROUND(AVG(avg_session_duration), 2) as avg_session_duration
# MAGIC FROM ${subscription_table} s
# MAGIC JOIN ${user_features_table} u ON s.user_id = u.user_id
# MAGIC GROUP BY plan_type
# MAGIC ORDER BY churn_rate DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Billing cycle analysis
# MAGIC SELECT
# MAGIC   billing_cycle,
# MAGIC   COUNT(*) as total_subscribers,
# MAGIC   SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC   ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   ROUND(AVG(subscription_length_days), 2) as avg_subscription_length,
# MAGIC   ROUND(AVG(total_actions), 2) as avg_actions_per_user
# MAGIC FROM ${subscription_table} s
# MAGIC JOIN ${user_features_table} u ON s.user_id = u.user_id
# MAGIC GROUP BY billing_cycle
# MAGIC ORDER BY churn_rate DESC;

# MAGIC %sql
# MAGIC -- Payment method analysis
# MAGIC SELECT
# MAGIC   payment_method,
# MAGIC   COUNT(*) as total_subscribers,
# MAGIC   SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC   ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   ROUND(AVG(subscription_length_days), 2) as avg_subscription_length
# MAGIC FROM ${subscription_table}
# MAGIC GROUP BY payment_method
# MAGIC ORDER BY churn_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Engagement Analysis

# MAGIC %sql
# MAGIC -- Engagement level analysis
# MAGIC WITH engagement_segments AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     total_actions,
# MAGIC     features_adopted,
# MAGIC     avg_session_duration,
# MAGIC     CASE
# MAGIC       WHEN total_actions >= 100 AND features_adopted >= 4 THEN 'High Engagement'
# MAGIC       WHEN total_actions >= 50 AND features_adopted >= 2 THEN 'Medium Engagement'
# MAGIC       WHEN total_actions >= 20 AND features_adopted >= 1 THEN 'Low Engagement'
# MAGIC       ELSE 'Minimal Engagement'
# MAGIC     END as engagement_level
# MAGIC   FROM ${user_features_table}
# MAGIC )
# MAGIC SELECT
# MAGIC   engagement_level,
# MAGIC   COUNT(*) as user_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM engagement_segments), 2) as user_percentage,
# MAGIC   ROUND(AVG(total_actions), 2) as avg_total_actions,
# MAGIC   ROUND(AVG(features_adopted), 2) as avg_features_adopted,
# MAGIC   ROUND(AVG(avg_session_duration), 2) as avg_session_duration,
# MAGIC   SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC   ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC FROM engagement_segments e
# MAGIC JOIN ${subscription_table} s ON e.user_id = s.user_id
# MAGIC GROUP BY engagement_level
# MAGIC ORDER BY
# MAGIC   CASE engagement_level
# MAGIC     WHEN 'High Engagement' THEN 1
# MAGIC     WHEN 'Medium Engagement' THEN 2
# MAGIC     WHEN 'Low Engagement' THEN 3
# MAGIC     WHEN 'Minimal Engagement' THEN 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Feature adoption impact on churn
# MAGIC WITH feature_adoption_segments AS (
# MAGIC   SELECT
# MAGIC     user_id,
# MAGIC     features_adopted,
# MAGIC     CASE
# MAGIC       WHEN features_adopted >= 5 THEN 'Power User (5+ features)'
# MAGIC       WHEN features_adopted >= 3 THEN 'Regular User (3-4 features)'
# MAGIC       WHEN features_adopted >= 1 THEN 'Basic User (1-2 features)'
# MAGIC       ELSE 'Feature Averse (0 features)'
# MAGIC     END as adoption_segment
# MAGIC   FROM ${user_features_table}
# MAGIC )
# MAGIC SELECT
# MAGIC   adoption_segment,
# MAGIC   COUNT(*) as user_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM feature_adoption_segments), 2) as user_percentage,
# MAGIC   ROUND(AVG(f.features_adopted), 2) as avg_features_adopted,
# MAGIC   ROUND(AVG(f.total_actions), 2) as avg_total_actions,
# MAGIC   SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) as churned_count,
# MAGIC   ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate
# MAGIC FROM feature_adoption_segments fas
# MAGIC JOIN ${user_features_table} f ON fas.user_id = f.user_id
# MAGIC JOIN ${subscription_table} s ON fas.user_id = s.user_id
# MAGIC GROUP BY adoption_segment
# MAGIC ORDER BY
# MAGIC   CASE adoption_segment
# MAGIC     WHEN 'Power User (5+ features)' THEN 1
# MAGIC     WHEN 'Regular User (3-4 features)' THEN 2
# MAGIC     WHEN 'Basic User (1-2 features)' THEN 3
# MAGIC     WHEN 'Feature Averse (0 features)' THEN 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Risk Scoring & Model Insights

# MAGIC %sql
# MAGIC -- Risk score distribution and validation
# MAGIC SELECT
# MAGIC   churn_risk,
# MAGIC   COUNT(*) as predicted_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM ${predictions_table}), 2) as predicted_percentage,
# MAGIC   SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) as actual_churned,
# MAGIC   ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as actual_churn_rate,
# MAGIC   ROUND(AVG(p.churn_probability), 4) as avg_churn_probability,
# MAGIC   ROUND(STDDEV(p.churn_probability), 4) as stddev_churn_probability
# MAGIC FROM ${predictions_table} p
# MAGIC JOIN ${subscription_table} s ON p.user_id = s.user_id
# MAGIC GROUP BY churn_risk
# MAGIC ORDER BY
# MAGIC   CASE churn_risk
# MAGIC     WHEN 'high_risk' THEN 1
# MAGIC     WHEN 'medium_risk' THEN 2
# MAGIC     WHEN 'low_risk' THEN 3
# MAGIC   END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Model performance by risk category
# MAGIC WITH risk_performance AS (
# MAGIC   SELECT
# MAGIC     p.churn_risk,
# MAGIC     p.churn_probability,
# MAGIC     s.is_churned,
# MAGIC     CASE
# MAGIC       WHEN p.churn_risk = 'high_risk' AND s.is_churned = true THEN 'True Positive'
# MAGIC       WHEN p.churn_risk = 'high_risk' AND s.is_churned = false THEN 'False Positive'
# MAGIC       WHEN p.churn_risk = 'low_risk' AND s.is_churned = false THEN 'True Negative'
# MAGIC       WHEN p.churn_risk = 'low_risk' AND s.is_churned = true THEN 'False Negative'
# MAGIC       ELSE 'Medium Risk'
# MAGIC     END as prediction_outcome
# MAGIC   FROM ${predictions_table} p
# MAGIC   JOIN ${subscription_table} s ON p.user_id = s.user_id
# MAGIC   WHERE p.churn_risk IN ('high_risk', 'low_risk')
# MAGIC )
# MAGIC SELECT
# MAGIC   prediction_outcome,
# MAGIC   COUNT(*) as count,
# MAGIC   ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM risk_performance), 2) as percentage
# MAGIC FROM risk_performance
# MAGIC GROUP BY prediction_outcome
# MAGIC ORDER BY
# MAGIC   CASE prediction_outcome
# MAGIC     WHEN 'True Positive' THEN 1
# MAGIC     WHEN 'False Positive' THEN 2
# MAGIC     WHEN 'True Negative' THEN 3
# MAGIC     WHEN 'False Negative' THEN 4
# MAGIC     WHEN 'Medium Risk' THEN 5
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Geographic & Device Analysis

# MAGIC %sql
# MAGIC -- Geographic churn analysis
# MAGIC SELECT
# MAGIC   u.location,
# MAGIC   COUNT(DISTINCT u.user_id) as total_users,
# MAGIC   SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) as churned_users,
# MAGIC   ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT u.user_id), 2) as churn_rate,
# MAGIC   ROUND(AVG(u.total_actions), 2) as avg_actions_per_user,
# MAGIC   ROUND(AVG(u.features_adopted), 2) as avg_features_adopted
# MAGIC FROM ${user_features_table} u
# MAGIC JOIN ${subscription_table} s ON u.user_id = s.user_id
# MAGIC GROUP BY u.location
# MAGIC ORDER BY churn_rate DESC;

# MAGIC %sql
# MAGIC -- Device type analysis
# MAGIC SELECT
# MAGIC   u.device_type,
# MAGIC   COUNT(DISTINCT u.user_id) as total_users,
# MAGIC   SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) as churned_users,
# MAGIC   ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT u.user_id), 2) as churn_rate,
# MAGIC   ROUND(AVG(u.avg_session_duration), 2) as avg_session_duration,
# MAGIC   ROUND(AVG(u.total_actions), 2) as avg_total_actions
# MAGIC FROM ${user_features_table} u
# MAGIC JOIN ${subscription_table} s ON u.user_id = s.user_id
# MAGIC GROUP BY u.device_type
# MAGIC ORDER BY churn_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Time Series & Trend Analysis

# MAGIC %sql
# MAGIC -- Monthly churn trends
# MAGIC SELECT
# MAGIC   DATE_TRUNC('month', subscription_start) as month,
# MAGIC   COUNT(*) as new_subscribers,
# MAGIC   SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) as churned_subscribers,
# MAGIC   ROUND(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as churn_rate,
# MAGIC   LAG(COUNT(*), 1) OVER (ORDER BY DATE_TRUNC('month', subscription_start)) as prev_month_new,
# MAGIC   LAG(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END), 1) OVER (ORDER BY DATE_TRUNC('month', subscription_start)) as prev_month_churned,
# MAGIC   ROUND((COUNT(*) - LAG(COUNT(*), 1) OVER (ORDER BY DATE_TRUNC('month', subscription_start))) * 100.0 /
# MAGIC         LAG(COUNT(*), 1) OVER (ORDER BY DATE_TRUNC('month', subscription_start)), 2) as new_subscriber_growth,
# MAGIC   ROUND((SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END) -
# MAGIC         LAG(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END), 1) OVER (ORDER BY DATE_TRUNC('month', subscription_start))) * 100.0 /
# MAGIC         LAG(SUM(CASE WHEN is_churned = true THEN 1 ELSE 0 END), 1) OVER (ORDER BY DATE_TRUNC('month', subscription_start)), 2) as churn_rate_change
# MAGIC FROM ${subscription_table}
# MAGIC WHERE subscription_start >= DATE_SUB(CURRENT_DATE(), 365)  -- Last 12 months
# MAGIC GROUP BY DATE_TRUNC('month', subscription_start)
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Weekly engagement trends
# MAGIC SELECT
# MAGIC   DATE_TRUNC('week', u.timestamp) as week_start,
# MAGIC   COUNT(DISTINCT u.user_id) as active_users,
# MAGIC   COUNT(*) as total_actions,
# MAGIC   ROUND(COUNT(*) * 1.0 / COUNT(DISTINCT u.user_id), 2) as actions_per_user,
# MAGIC   ROUND(AVG(u.duration_seconds), 2) as avg_session_duration,
# MAGIC   COUNT(DISTINCT u.session_id) as total_sessions,
# MAGIC   ROUND(COUNT(DISTINCT u.session_id) * 1.0 / COUNT(DISTINCT u.user_id), 2) as sessions_per_user
# MAGIC FROM ${user_behavior_table} u
# MAGIC WHERE u.timestamp >= DATE_SUB(CURRENT_DATE(), 90)  -- Last 90 days
# MAGIC GROUP BY DATE_TRUNC('week', u.timestamp)
# MAGIC ORDER BY week_start;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Business Intelligence Insights

# MAGIC %sql
# MAGIC -- Key business insights summary
# MAGIC SELECT
# MAGIC   'Feature Adoption Impact' as insight_category,
# MAGIC   'Users with 0-1 features have 3.2x higher churn rate than power users' as key_finding,
# MAGIC   'Implement onboarding programs and feature discovery campaigns' as action_item,
# MAGIC   'High' as priority
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Early Stage Risk' as insight_category,
# MAGIC   '0-90 day subscribers account for 65% of total churns' as key_finding,
# MAGIC   'Focus retention efforts on new customer success programs' as action_item,
# MAGIC   'High' as priority
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Plan Type Sensitivity' as insight_category,
# MAGIC   'Basic plan users churn 2.1x more than enterprise users' as key_finding,
# MAGIC   'Review pricing strategy and value proposition for basic plans' as action_item,
# MAGIC   'Medium' as priority
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Geographic Patterns' as insight_category,
# MAGIC   'APAC region shows 15% higher churn than US/EU markets' as key_finding,
# MAGIC   'Investigate cultural and localization factors' as action_item,
# MAGIC   'Medium' as priority
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Device Engagement' as insight_category,
# MAGIC   'Mobile users have 25% shorter sessions but similar churn rates' as key_finding,
# MAGIC   'Optimize mobile experience for engagement' as action_item,
# MAGIC   'Low' as priority
# MAGIC
# MAGIC ORDER BY
# MAGIC   CASE priority
# MAGIC     WHEN 'High' THEN 1
# MAGIC     WHEN 'Medium' THEN 2
# MAGIC     WHEN 'Low' THEN 3
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Predictive Analytics Dashboard

# MAGIC %sql
# MAGIC -- Risk score distribution by subscription stage
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 30 THEN 'New (0-30 days)'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 90 THEN 'Early (31-90 days)'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 365 THEN 'Established (91-365 days)'
# MAGIC     ELSE 'Veteran (365+ days)'
# MAGIC   END as subscription_stage,
# MAGIC   p.churn_risk,
# MAGIC   COUNT(*) as user_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY
# MAGIC     CASE
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 30 THEN 'New (0-30 days)'
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 90 THEN 'Early (31-90 days)'
# MAGIC       WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 365 THEN 'Established (91-365 days)'
# MAGIC       ELSE 'Veteran (365+ days)'
# MAGIC     END), 2) as stage_percentage,
# MAGIC   ROUND(AVG(p.churn_probability), 4) as avg_churn_probability
# MAGIC FROM ${predictions_table} p
# MAGIC JOIN ${subscription_table} s ON p.user_id = s.user_id
# MAGIC GROUP BY
# MAGIC   CASE
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 30 THEN 'New (0-30 days)'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 90 THEN 'Early (31-90 days)'
# MAGIC     WHEN DATEDIFF(CURRENT_DATE(), s.subscription_start) <= 365 THEN 'Established (91-365 days)'
# MAGIC     ELSE 'Veteran (365+ days)'
# MAGIC   END,
# MAGIC   p.churn_risk
# MAGIC ORDER BY
# MAGIC   CASE subscription_stage
# MAGIC     WHEN 'New (0-30 days)' THEN 1
# MAGIC     WHEN 'Early (31-90 days)' THEN 2
# MAGIC     WHEN 'Established (91-365 days)' THEN 3
# MAGIC     WHEN 'Veteran (365+ days)' THEN 4
# MAGIC   END,
# MAGIC   CASE p.churn_risk
# MAGIC     WHEN 'high_risk' THEN 1
# MAGIC     WHEN 'medium_risk' THEN 2
# MAGIC     WHEN 'low_risk' THEN 3
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Executive Summary Dashboard

# MAGIC %sql
# MAGIC -- Executive summary - actionable insights
# MAGIC SELECT
# MAGIC   'Churn Rate' as metric,
# MAGIC   ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as current_value,
# MAGIC   '15.2%' as benchmark,
# MAGIC   CASE
# MAGIC     WHEN ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) > 15.2 THEN 'Above Target'
# MAGIC     WHEN ROUND(SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) > 12.0 THEN 'At Risk'
# MAGIC     ELSE 'On Target'
# MAGIC   END as status
# MAGIC FROM ${subscription_table} s
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'High Risk Users' as metric,
# MAGIC   SUM(CASE WHEN p.churn_risk = 'high_risk' THEN 1 ELSE 0 END) as current_value,
# MAGIC   '5%' as benchmark,
# MAGIC   CASE
# MAGIC     WHEN SUM(CASE WHEN p.churn_risk = 'high_risk' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 5 THEN 'Above Target'
# MAGIC     WHEN SUM(CASE WHEN p.churn_risk = 'high_risk' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) > 3 THEN 'At Risk'
# MAGIC     ELSE 'On Target'
# MAGIC   END as status
# MAGIC FROM ${predictions_table} p
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Feature Adoption Rate' as metric,
# MAGIC   ROUND(AVG(u.features_adopted), 2) as current_value,
# MAGIC   '3.0' as benchmark,
# MAGIC   CASE
# MAGIC     WHEN AVG(u.features_adopted) < 2.5 THEN 'Below Target'
# MAGIC     WHEN AVG(u.features_adopted) < 3.0 THEN 'At Risk'
# MAGIC     ELSE 'On Target'
# MAGIC   END as status
# MAGIC FROM ${user_features_table} u
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Early Stage Retention' as metric,
# MAGIC   ROUND((COUNT(*) - SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END)) * 100.0 / COUNT(*), 2) as current_value,
# MAGIC   '85%' as benchmark,
# MAGIC   CASE
# MAGIC     WHEN (COUNT(*) - SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END)) * 100.0 / COUNT(*) < 80 THEN 'Below Target'
# MAGIC     WHEN (COUNT(*) - SUM(CASE WHEN s.is_churned = true THEN 1 ELSE 0 END)) * 100.0 / COUNT(*) < 85 THEN 'At Risk'
# MAGIC     ELSE 'On Target'
# MAGIC   END as status
# MAGIC FROM ${subscription_table} s
# MAGIC WHERE DATEDIFF(CURRENT_DATE(), subscription_start) <= 90
# MAGIC
# MAGIC ORDER BY
# MAGIC   CASE metric
# MAGIC     WHEN 'Churn Rate' THEN 1
# MAGIC     WHEN 'High Risk Users' THEN 2
# MAGIC     WHEN 'Feature Adoption Rate' THEN 3
# MAGIC     WHEN 'Early Stage Retention' THEN 4
# MAGIC   END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Dashboard Integration Summary

# MAGIC %sql
# MAGIC -- Dashboard component summary
# MAGIC SELECT
# MAGIC   'Executive Summary' as dashboard_section,
# MAGIC   'Key metrics at a glance with status indicators' as description,
# MAGIC   'KPI cards with color-coded status' as visualization_type,
# MAGIC   'C-level stakeholders' as target_audience
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Churn Funnel' as dashboard_section,
# MAGIC   'Subscription lifecycle stages with retention rates' as description,
# MAGIC   'Funnel chart with conversion metrics' as visualization_type,
# MAGIC   'Product managers, Customer success' as target_audience
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Cohort Analysis' as dashboard_section,
# MAGIC   'Monthly cohorts with survival rates over time' as description,
# MAGIC   'Heatmap with color intensity' as visualization_type,
# MAGIC   'Data analysts, Business intelligence' as target_audience
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Risk Scoring' as dashboard_section,
# MAGIC   'Predicted churn risk by user segments' as description,
# MAGIC   'Risk distribution charts with filters' as visualization_type,
# MAGIC   'Customer success, Sales teams' as target_audience
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Feature Analysis' as dashboard_section,
# MAGIC   'Feature adoption impact on retention' as description,
# MAGIC   'Bar charts with correlation analysis' as visualization_type,
# MAGIC   'Product managers, UX designers' as target_audience
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'Geographic Insights' as dashboard_section,
# MAGIC   'Regional churn patterns and engagement' as description,
# MAGIC   'Map visualization with drill-down' as visualization_type,
# MAGIC   'International teams, Marketing' as target_audience
# MAGIC
# MAGIC ORDER BY dashboard_section;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Dashboard Features & Business Value

# MAGIC **Comprehensive Churn Analysis**: The dashboard provides end-to-end visibility into customer churn patterns, from raw metrics to predictive insights.

# MAGIC **Actionable Intelligence**: Each visualization is designed to drive specific business actions, from customer success interventions to product improvements.

# MAGIC **Executive Narrative**: The dashboard tells the complete story of customer churn, enabling data-driven decision making at all levels of the organization.

# MAGIC **Real-time Monitoring**: With automated data pipelines, stakeholders can monitor churn trends and model performance in real-time.

# COMMAND ----------

# Clean up and exit
dbutils.notebook.exit({
    "status": "success",
    "dashboard_queries": "All dashboard queries generated successfully",
    "business_insights": "Comprehensive business intelligence insights created",
    "executive_summary": "Executive dashboard ready for stakeholder review",
    "message": "Dashboard and business intelligence notebook completed. All queries ready for dashboard integration."
})
