# Databricks notebook source
# MAGIC %md
# MAGIC total customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_customers
# MAGIC FROM clustered_customers_kmeans;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Average lifetime value (overall)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(AVG(Lifetime_Value), 2) AS avg_ltv
# MAGIC FROM clustered_customers_kmeans;

# COMMAND ----------

# MAGIC %md
# MAGIC overall churn rate %

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ROUND(SUM(Churned) / COUNT(*), 2) AS overall_churn_rate
# MAGIC FROM clustered_customers_kmeans;

# COMMAND ----------

# MAGIC %md
# MAGIC customers per cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Cluster_Name,
# MAGIC   COUNT(*) AS customers
# MAGIC FROM clustered_customers_kmeans
# MAGIC GROUP BY Cluster_Name
# MAGIC ORDER BY customers DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC average lifetime value per cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Cluster_Name,
# MAGIC   ROUND(AVG(Lifetime_Value), 2) AS avg_ltv
# MAGIC FROM clustered_customers_kmeans
# MAGIC GROUP BY Cluster_Name
# MAGIC ORDER BY avg_ltv DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC average churn rate per cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Cluster_Name,
# MAGIC   ROUND(SUM(Churned) * 100.0 / COUNT(*), 2) AS churn_rate
# MAGIC FROM clustered_customers_kmeans
# MAGIC GROUP BY Cluster_Name
# MAGIC ORDER BY churn_rate DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC average login frequency (engagement)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Cluster_Name,
# MAGIC   ROUND(AVG(Login_Frequency), 2) AS avg_login_frequency
# MAGIC FROM clustered_customers_kmeans
# MAGIC GROUP BY Cluster_Name
# MAGIC ORDER BY avg_login_frequency DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC purchase behavior per cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Cluster_Name,
# MAGIC   ROUND(AVG(Total_Purchases), 2) AS avg_purchases,
# MAGIC   ROUND(AVG(Average_Order_Value), 2) AS avg_order_value
# MAGIC FROM clustered_customers_kmeans
# MAGIC GROUP BY Cluster_Name
# MAGIC ORDER BY avg_purchases DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC average order value per cluster
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Cluster_Name,
# MAGIC   ROUND(AVG(Average_Order_Value), 2) AS avg_order_value
# MAGIC FROM clustered_customers_kmeans
# MAGIC GROUP BY Cluster_Name;

# COMMAND ----------

# MAGIC %md
# MAGIC top value cluster

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   Cluster_Name,
# MAGIC   ROUND(AVG(Lifetime_Value), 2) AS avg_ltv
# MAGIC FROM clustered_customers_kmeans
# MAGIC GROUP BY Cluster_Name
# MAGIC ORDER BY avg_ltv DESC
# MAGIC LIMIT 1;
