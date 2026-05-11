# Databricks notebook source
# =========================================================
# SILVER LAYER – DATA CLEANING & STANDARDIZATION
# =========================================================

from pyspark.sql.functions import (
    col, count, when, mean, trim, lower, initcap
)

print("=" * 80)
print("STEP 0: READ BRONZE DATA")
print("=" * 80)

df_bronze = spark.sql("SELECT * FROM bronze_customer_data")
print(f"Total records (Bronze): {df_bronze.count()}")
df_bronze.printSchema()




# COMMAND ----------

# =========================================================
# STEP 1: BASIC DATA VALIDATION
# =========================================================
print("=" * 80)
print("NULL VALUE PROFILE")
print("=" * 80)

null_counts = df_bronze.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_bronze.columns
])
null_counts.show(1, vertical=True)




# COMMAND ----------

# =========================================================
# STEP 2: REMOVE INVALID / LOGICALLY WRONG RECORDS
# =========================================================
print("=" * 80)
print("REMOVE INVALID NUMERIC RECORDS")
print("=" * 80)

df_cleaned = df_bronze.filter(col("Total_Purchases") >= 0)
print(f"Removed {df_bronze.count() - df_cleaned.count()} rows with negative purchases")




# COMMAND ----------

# =========================================================
# STEP 3: STRING NORMALIZATION (VERY IMPORTANT)
# =========================================================
print("=" * 80)
print("NORMALIZING CATEGORICAL COLUMNS")
print("=" * 80)

df_cleaned = df_cleaned \
    .withColumn("Gender", trim(col("Gender"))) \
    .withColumn("Country", initcap(trim(col("Country")))) \
    .withColumn("City", initcap(trim(col("City"))))




# COMMAND ----------

# =========================================================
# STEP 4: GENDER STANDARDIZATION (NO 'OTHER', NO NULLS)
# =========================================================
print("=" * 80)
print("STANDARDIZING GENDER VALUES")
print("=" * 80)

df_cleaned = df_cleaned.withColumn(
    "Gender",
    when(lower(col("Gender")) == "male", "Male")
    .when(lower(col("Gender")) == "female", "Female")
)
df_cleaned = df_cleaned.filter(col("Gender").isNotNull())



# COMMAND ----------

# =========================================================
# STEP 5: NUMERICAL IMPUTATION (MEAN STRATEGY)
# =========================================================
print("=" * 80)
print("IMPUTING MISSING NUMERICAL VALUES")
print("=" * 80)

age_mean = df_cleaned.agg(mean("Age")).first()[0]
session_mean = df_cleaned.agg(mean("Session_Duration_Avg")).first()[0]
pages_mean = df_cleaned.agg(mean("Pages_Per_Session")).first()[0]
wishlist_mean = df_cleaned.agg(mean("Wishlist_Items")).first()[0]
days_mean = df_cleaned.agg(mean("Days_Since_Last_Purchase")).first()[0]

df_filled = df_cleaned \
    .fillna(age_mean, ["Age"]) \
    .fillna(session_mean, ["Session_Duration_Avg"]) \
    .fillna(pages_mean, ["Pages_Per_Session"]) \
    .fillna(wishlist_mean, ["Wishlist_Items"]) \
    .fillna(days_mean, ["Days_Since_Last_Purchase"]) \
    .fillna(0, [
        "Customer_Service_Calls",
        "Email_Open_Rate",
        "Returns_Rate",
        "Credit_Balance"
    ])




# COMMAND ----------

# =========================================================
# STEP 6: REMOVE DUPLICATES
# =========================================================
print("=" * 80)
print("REMOVING DUPLICATES")
print("=" * 80)

before = df_filled.count()
df_silver = df_filled.dropDuplicates()
after = df_silver.count()

print(f"Duplicate rows removed: {before - after}")
print(f"Final record count (Silver): {after}")




# COMMAND ----------

# =========================================================
# STEP 7: FINAL DATA VALIDATION
# =========================================================
print("=" * 80)
print("FINAL VALIDATION CHECKS")
print("=" * 80)

# Gender must have only Male / Female
df_silver.groupBy("Gender").count().show()

# No NULLs allowed in Gender
df_silver.filter(col("Gender").isNull()).show()




# COMMAND ----------

# =========================================================
# STEP 8: WRITE SILVER TABLE
# =========================================================
print("=" * 80)
print("WRITING SILVER TABLE")
print("=" * 80)

df_silver.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_customer_data")

print(" silver_customer_data table created successfully")
df_silver.show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC select gender, count(*) as numb from silver_customer_data group by gender

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM silver_customer_data WHERE Gender IS NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from silver_customer_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select country, count(*) as customers from silver_customer_data group by country
