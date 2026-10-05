# Databricks notebook source
import urllib.request
import shutil
import os

# COMMAND ----------

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"

response = urllib.request.urlopen(url)

dir_path = "/Volumes/nyctaxi/00_landing/data_sources/2026-01"
os.makedirs(dir_path, exist_ok=True)

file_path = os.path.join(dir_path, "yellow_tripdata_2026-01.parquet")

with open(file_path, 'wb') as f:
    shutil.copyfileobj(response, f)

# COMMAND ----------

