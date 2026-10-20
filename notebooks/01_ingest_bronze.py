# Databricks notebook source

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

# Configuration
CATALOG = "workspace"
SCHEMA = "bronze"
VOLUME = "logistic_landing"

CSV_PATH = (
    f"/Volumes/{CATALOG}/{SCHEMA}/"
    f"{VOLUME}/shipments.csv"
)

TARGET_TABLE = f"{CATALOG}.{SCHEMA}.shipments"

# Define source schema
shipment_schema = StructType([
    StructField("shipment_id", StringType(), True),
    StructField("distribution_center", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("planned_delivery", StringType(), True),
    StructField("actual_delivery", StringType(), True),
    StructField("ordered_units", IntegerType(), True),
    StructField("delivered_units", IntegerType(), True),
])

# Read shipments from the landing volume
shipments_df = (
    spark.read
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(shipment_schema)
    .csv(CSV_PATH)
)

# Add ingestion metadata
bronze_df = (
    shipments_df
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "source_system",
        F.lit("synthetic_wms")
    )
    .withColumn(
        "source_file",
        F.lit(CSV_PATH)
    )
)

# Write to Bronze Delta table
(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Successfully loaded CSV into {TARGET_TABLE}")

display(spark.table(TARGET_TABLE))


# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN workspace.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE VOLUME workspace.bronze.logistic_landing

# COMMAND ----------

volume_path = "/Volumes/workspace/bronze/logistic_landing/"

files = dbutils.fs.ls(volume_path)

display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     shipment_id,
# MAGIC     store_id,
# MAGIC     ordered_units,
# MAGIC     delivered_units,
# MAGIC     source_system,
# MAGIC     source_file
# MAGIC FROM workspace.bronze.shipments
# MAGIC ORDER BY shipment_id;
# MAGIC