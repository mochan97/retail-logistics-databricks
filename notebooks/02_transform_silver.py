# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC **Read the Bronze data**

# COMMAND ----------


from pyspark.sql import functions as F

SOURCE_TABLE = "workspace.bronze.shipments"
TARGET_TABLE = "workspace.silver.shipments"

bronze_df = spark.table(SOURCE_TABLE)

display(bronze_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **Convert data **types****

# COMMAND ----------


typed_df = (
    bronze_df
    .withColumn(
        "planned_delivery",
        F.to_date(F.col("planned_delivery"), "yyyy-MM-dd")
    )
    .withColumn(
        "actual_delivery",
        F.to_date(F.col("actual_delivery"), "yyyy-MM-dd")
    )
)

typed_df.printSchema()
display(typed_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **Add data quality checks**

# COMMAND ----------


valid_condition = (
    F.col("shipment_id").isNotNull()
    & F.col("store_id").isNotNull()
    & F.col("planned_delivery").isNotNull()
    & F.col("actual_delivery").isNotNull()
    & (F.col("ordered_units") > 0)
    & (F.col("delivered_units") >= 0)
)

valid_df = typed_df.filter(valid_condition)
invalid_df = typed_df.filter(~valid_condition)

valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(f"Valid records: {valid_count}")
print(f"Invalid records: {invalid_count}")

display(invalid_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **Apply logistics business rules**

# COMMAND ----------


silver_df = (
    valid_df
    .withColumn(
        "delivery_timeliness",
        F.when(
            F.col("actual_delivery") <= F.col("planned_delivery"),
            F.lit("ON_TIME")
        ).otherwise(F.lit("LATE"))
    )
    .withColumn(
        "fulfillment_status",
        F.when(
            F.col("delivered_units") < F.col("ordered_units"),
            F.lit("UNDER_DELIVERY")
        )
        .when(
            F.col("delivered_units") > F.col("ordered_units"),
            F.lit("OVER_DELIVERY")
        )
        .otherwise(F.lit("COMPLETE"))
    )
)

display(silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **Check duplicate shipment IDs**

# COMMAND ----------


duplicates_df = (
    silver_df
    .groupBy("shipment_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_count = duplicates_df.count()

print(f"Duplicate shipment IDs: {duplicate_count}")

assert duplicate_count == 0, (
    "Data quality failure: duplicate shipment IDs found"
)


# COMMAND ----------

# MAGIC %md
# MAGIC **Write the Silver Delta table**

# COMMAND ----------


spark.sql(
    "CREATE SCHEMA IF NOT EXISTS workspace.silver"
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully created {TARGET_TABLE}")


# COMMAND ----------

# MAGIC %md
# MAGIC **Validate with Spark SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     shipment_id,
# MAGIC     store_id,
# MAGIC     delivery_timeliness,
# MAGIC     fulfillment_status
# MAGIC FROM workspace.silver.shipments
# MAGIC ORDER BY shipment_id;