# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# Used to wild-card load only selected data sources. In some cases, union may be needed 
df = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/2026-*")


# COMMAND ----------

# Used to selectively load only selected data sources
df_202601 = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/2026-01")
df_202602 = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/2026-02")
df_202603 = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/2026-03")
df_202604 = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/2026-04")
df_202605 = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/2026-05")
df_202606 = spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/2026-06")
df = df_202601.union(df_202602).union(df_202603).union(df_202604).union(df_202605).union(df_202606)

# COMMAND ----------

# Used to selectively load only selected data sources
paths = [
    "/Volumes/nyctaxi/00_landing/data_sources/2026-01",
    "/Volumes/nyctaxi/00_landing/data_sources/2026-02",
    "/Volumes/nyctaxi/00_landing/data_sources/2026-03",
    "/Volumes/nyctaxi/00_landing/data_sources/2026-04",
    "/Volumes/nyctaxi/00_landing/data_sources/2026-05",
    "/Volumes/nyctaxi/00_landing/data_sources/2026-06"
]

df = spark.read.format("parquet").load(paths)

# COMMAND ----------

df = df.withColumn("processed_timestamp", current_timestamp())

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

#spark.read.table("nyctaxi.01_bronze.yellow_trips_raw").display()

# COMMAND ----------

# Negative fares
# df = spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")
# display(df.count())
# df.select("fare_amount").where("fare_amount < 0").display()
# print('\n')
# display(df.select("fare_amount").where("fare_amount < 0").count())