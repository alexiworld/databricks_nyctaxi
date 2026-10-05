# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

#display(df)

# COMMAND ----------

df = df.\
    groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
        agg(
            count("*").alias("total_trips"),
            round(avg(df.passenger_count), 1).alias("average_passengers"),
            round(avg(df.trip_distance), 1).alias("average_distance"),
            round(avg(df.fare_amount), 2).alias("average_fare_per_trip"),
            max(df.fare_amount).alias("max_fare"),
            min(df.fare_amount).alias("min_fare"),
            round(sum(df.total_amount), 2).alias("total_revenue")
        )

# COMMAND ----------

#display(df)

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")

# COMMAND ----------

# To confirm the loaded data timeframe is correct 
# for the incremental loading part of the course
spark.read.table("nyctaxi.02_silver.yellow_trips_cleansed").\
   agg(max("tpep_pickup_datetime"), min("tpep_pickup_datetime")).\
       display()