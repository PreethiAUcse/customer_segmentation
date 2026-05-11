# Databricks notebook source
from pyspark.sql.functions import col

# Read Silver (cleaned)
df_silver = spark.sql("SELECT * FROM silver_customer_data")

# Pick strong numeric features for K-Means (all available in your raw dataset)
kmeans_feature_cols = [
    "Age",
    "Membership_Years",
    "Login_Frequency",
    "Session_Duration_Avg",
    "Pages_Per_Session",
    "Cart_Abandonment_Rate",
    "Wishlist_Items",
    "Total_Purchases",
    "Average_Order_Value",
    "Days_Since_Last_Purchase",
    "Discount_Usage_Rate",
    "Returns_Rate",
    "Email_Open_Rate",
    "Customer_Service_Calls",
    "Product_Reviews_Written",
    "Social_Media_Engagement_Score",
    "Mobile_App_Usage",
    "Payment_Method_Diversity",
    "Lifetime_Value",
    "Credit_Balance"
]

# Select + cast numeric columns defensively (good practice for Spark ML)
df_gold_kmeans = df_silver.select(
    col("customer_id"),
    col("Gender"),      # optional for analysis later; not used directly by KMeans unless encoded
    col("Country"),     # optional (same note)
    *[col(c).cast("double").alias(c) for c in kmeans_feature_cols],
    col("Churned").cast("int").alias("Churned"),
    col("Signup_Quarter")
)

# Save as a separate Gold table (won't affect existing dashboards)
df_gold_kmeans.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_customer_kmeans_features")

print("gold_customer_kmeans_features created")

# COMMAND ----------

df_for_kmeans = spark.sql("SELECT * FROM gold_customer_kmeans_features")

# COMMAND ----------

df_for_kmeans.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC describe gold_customer_kmeans_features

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(CASE WHEN Lifetime_Value IS NULL THEN 1 END) AS null_ltv,
# MAGIC   COUNT(CASE WHEN Total_Purchases IS NULL THEN 1 END) AS null_purchases
# MAGIC FROM gold_customer_kmeans_features;
