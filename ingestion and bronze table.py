# Databricks notebook source
dbutils.fs.mkdirs("/Volumes/preethiworkspace_7405611394106500/default/customer_data/ecommerce_customer_churn_dataset.csv")

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/tables/customer_data/ecommerce_customer_churn_dataset.csv

# COMMAND ----------

df= spark.read.csv("/Volumes/preethiworkspace_7405607314198475/default/customer_segmentation/ecommerce_customer_churn_dataset.csv", header=True, inferSchema=True)
df.show(5)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import count, when, col

# 1. Read CSV (replace with your actual path)
df = spark.read.csv("/Volumes/preethiworkspace_7405607314198475/default/customer_segmentation/ecommerce_customer_churn_dataset.csv", header=True, inferSchema=True)

# 2. Add customer_id using monotonically_increasing_id
df1 = df.withColumn("customer_id", monotonically_increasing_id()+1)

# 3. Check schema
df1.printSchema()

# 4. Count records
print(f"Total records: {df1.count()}")

# 5. Display first 5 rows
df1.show(5)

# 6. Check for null values
df1.select([count(when(col(c).isNull(), c)).alias(c) for c in df1.columns]).show()

# COMMAND ----------

df1.show(5)

# COMMAND ----------

df1.createOrReplaceTempView("bronze_customer_data")
print("table created successfully")

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC CREATE OR REPLACE TABLE bronze_customer_data AS
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/preethiworkspace_7405607314198475/default/customer_segmentation/ecommerce_customer_churn_dataset.csv',
# MAGIC   format => 'csv',
# MAGIC   header => true,
# MAGIC   inferSchema => true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_customer_data

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df = spark.read.csv(
    "/Volumes/preethiworkspace_7405607314198475/default/customer_segmentation/ecommerce_customer_churn_dataset.csv",
    header=True,
    inferSchema=True
)

df_bronze = df.withColumn("customer_id", monotonically_increasing_id() + 1)

df_bronze.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_customer_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_customer_data
