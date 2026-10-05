# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff

# COMMAND ----------

df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

from pyspark.sql.functions import max, min

df.agg(max("tpep_pickup_datetime"), min("tpep_pickup_datetime")).display()

# COMMAND ----------

df = df.filter("tpep_pickup_datetime >= '2026-01-01' and tpep_pickup_datetime <= '2026-07-01'")

# COMMAND ----------

df = df.select(
    when(col("VendorID") == 1, "Creative Mobile Techonologies, LLC")
    .when(col("VendorID") == 2, "Curb Mobility, LLC")
    .when(col("VendorID") == 6, "Myle Technologies Inc")
    .when(col("VendorID") == 7, "Helyx")
    .otherwise("Unknown")
    .alias("vendor"),
    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    timestamp_diff('MINUTE', col("tpep_dropoff_datetime"), col("tpep_pickup_datetime")).alias("trip_duration"),
    col("passenger_count"),
    col("trip_distance"),
    
    when(col("RatecodeID") == 1, "Standard rate")
    .when(col("RatecodeID") == 2, "JFK")
    .when(col("RatecodeID") == 3, "Newark")
    .when(col("RatecodeID") == 4, "Nassau or Westchester")
    .when(col("RatecodeID") == 5, "Negotiated fare")
    .when(col("RatecodeID") == 6, "Group ride")
    .otherwise("Unknown")
    .alias("rate_type"),

    "store_and_fwd_flag",

    col("PULocationID").alias("pu_location_id"),
    col("DOLocationID").alias("do_location_id"),

    when(col("payment_type") == 0, "Flex Fare trip")
    .when(col("payment_type") == 1, "Credit card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 6, "Voided trip")
    .otherwise("Unknown")
    .alias("payment_type"),
    
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tolls_amount"),
    col("improvement_surcharge"),    
    col("total_amount"),
    col("congestion_surcharge"),
    col("Airport_fee").alias("airport_fee"),
    col("cbd_congestion_fee"),
    col("processed_timestamp")
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")

# COMMAND ----------

#spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed").display()