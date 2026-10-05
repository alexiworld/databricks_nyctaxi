# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import urllib.request
import os
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta

formatted_today_date = date.today().strftime("%Y-%m")
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/{formatted_today_date}"
print(dir_path)

three_months_ago = date.today() - relativedelta(months=3)
formatted_date = three_months_ago.strftime("%Y-%m")

# Define the local directory for this date's data
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/{formatted_date}"
print(dir_path)


# COMMAND ----------

# Databricks notebook source
import sys
import os
# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

import urllib.request
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_target_yyyymm
from modules.data_loader.file_downloader import download_file

# Obtains the year-month for 3 months prior to the current month in yyyy-MM format
formatted_date = get_target_yyyymm(3)

# Define the local directory for this date's data
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/{formatted_date}"

# Define the full path for the downloaded file
local_path = f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

try:
    # Check if the file already exists
    dbutils.fs.ls(local_path)

    # If the file already exists then set continue_downstream to no
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already downloaded, aborting downstream tasks")
except:
    try:
        # Construct the URL for the Parquet file corresponding to this month
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"

        # Download the file
        # Create the local directory for this date's data
        download_file(url, dir_path, local_path)
        
        # Set continue_downstream to yes if the file was loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File succesfully uploaded in current run")
    except Exception as e:
        # Set continue downstream to no if the file was not loaded
        dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"File download failed: {str(e)}")

# COMMAND ----------

# Purposely set to trigger the next step in the nyctaxi job/pipeline
dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")