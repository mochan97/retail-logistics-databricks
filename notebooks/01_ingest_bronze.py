# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, DoubleType
)

# Synthetic retail logistics data
records = [
    ("S001", "DC01", "STORE101", "2026-10-18", "2026-10-18", 120, 120),
    ("S002", "DC01", "STORE102", "2026-10-18", "2026-10-19", 80, 80),
    ("S003", "DC02", "STORE103", "2026-10-19", "2026-10-19", 150, 145),
    ("S004", "DC02", "STORE104", "2026-10-19", "2026-10-19", 90, 90),
    ("S005", "DC01", "STORE105", "2026-10-19", "2026-10-20", 60, 60)
]

schema = StructType([
    StructField("shipment_id", StringType(), False),
    StructField("distribution_center", StringType(), False),
    StructField("store_id", StringType(), False),
    StructField("planned_delivery", StringType(), False),
    StructField("actual_delivery", StringType(), False),
    StructField("ordered_units", IntegerType(), False),
    StructField("delivered_units", IntegerType(), False)
])

df = spark.createDataFrame(records, schema)

# Add ingestion metadata
bronze_df = (
    df.withColumn("ingestion_timestamp", F.current_timestamp())
      .withColumn("source_system", F.lit("synthetic_erp"))
)

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")

bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.bronze.shipments")

display(spark.table("workspace.bronze.shipments"))
