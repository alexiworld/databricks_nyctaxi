# Databricks notebook source
# MAGIC %md
# MAGIC ## Which vendor makes the most revenue?

# COMMAND ----------

df = spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain plan (next explain cells added at a later time to demonstrate it)

# COMMAND ----------

from pyspark.sql.functions import count
df = df.groupBy('pu_borough', 'do_borough').agg(count('*').alias("number_of_trips")).orderBy('number_of_trips', ascending=False).limit(1)

# COMMAND ----------

df.explain()

# COMMAND ----------

df.explain(mode='cost')

# COMMAND ----------

df.explain(mode='formatted')

# COMMAND ----------

df.explain(mode='extended')

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import sum, round

dfa = df.select('vendor', 'total_amount').groupBy('vendor').agg({'total_amount':'sum'}).orderBy('sum(total_amount)').orderBy('sum(total_amount)', ascending=False).limit(1)

dfa = df.select('vendor', 'total_amount').groupBy('vendor').agg({'total_amount':'sum'}).withColumnRenamed('sum(total_amount)', 'total').orderBy('total', ascending=False).limit(1)

# without import sum, pyspark falls to default sum and raises an error for incompatible types
dfa = df.select('vendor', 'total_amount').groupBy('vendor').agg(sum('total_amount').alias('total')).orderBy('total', ascending=False).limit(1)

dfa = df.select('vendor', 'total_amount').groupBy('vendor').sum('total_amount').withColumnRenamed('sum(total_amount)', 'total').orderBy('total', ascending=False).limit(1)

dfa = df.groupBy('vendor').agg(round(sum('total_amount').alias('total'), 2)).orderBy('total', ascending=False).limit(1)

display(dfa)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is the most popular pickup borough?

# COMMAND ----------

# My take on the problem:
df.groupBy('pu_borough').count().orderBy('count', ascending=False).limit(1).display()

# Instructor take on the problem:
from pyspark.sql.functions import count

df.\
    groupBy('pu_borough').\
        agg(
            count('*').alias('number_of_trips')
        ).\
        orderBy('number_of_trips', ascending=False).limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which is the most common journey (borough to borough)?

# COMMAND ----------

df.groupBy('pu_borough', 'do_borough').agg(count('*').alias("number_of_trips")).orderBy('number_of_trips', ascending=False).limit(1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a time series chart showing the number of trips and total revenue per day

# COMMAND ----------



# COMMAND ----------

df = spark.read.table("nyctaxi.03_gold.daily_trip_summary")

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col("pickup_date").alias("date"), "total_trips", "total_revenue").display()

# COMMAND ----------

spark.conf.getAll