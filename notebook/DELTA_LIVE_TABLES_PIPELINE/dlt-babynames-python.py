# Databricks notebook source
# MAGIC %md # Delta Live Tables quickstart (Python)
# MAGIC
# MAGIC A notebook that provides an example Delta Live Tables pipeline to:
# MAGIC
# MAGIC - Read raw CSV data from a publicly available dataset into a table.
# MAGIC - Read the records from the raw data table and use a Delta Live Tables query and expectations to create a new table that contains cleansed data.
# MAGIC - Perform an analysis on the prepared data with a Delta Live Tables query.

# COMMAND ----------

# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Download the dataset and store it to a Unity Catalog volume
import os

os.environ["UNITY_CATALOG_VOLUME_PATH"] = "/Volumes/mycatalog/default/dld/"
os.environ["DATASET_DOWNLOAD_URL"] = "https://health.data.ny.gov/api/views/jxy9-yhdk/rows.csv"
os.environ["DATASET_DOWNLOAD_FILENAME"] = "rows.csv"

dbutils.fs.cp(f"{os.environ.get('DATASET_DOWNLOAD_URL')}", f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}")

# COMMAND ----------

# DBTITLE 1,Ingest the raw data into a table
@dlt.table(
  comment="Popular baby first names in New York. This data was ingested from the New York State Departement of Health."
)
def baby_names_raw():
  df = spark.read.csv(f"{os.environ.get('UNITY_CATALOG_VOLUME_PATH')}{os.environ.get('DATASET_DOWNLOAD_FILENAME')}", header=True, inferSchema=True)
  df_renamed_column = df.withColumnRenamed("First Name", "First_Name")
  return df_renamed_column

# COMMAND ----------

# DBTITLE 1,Clean and prepare data
@dlt.table(
  comment="New York popular baby first name data cleaned and prepared for analysis."
)
@dlt.expect("valid_first_name", "First_Name IS NOT NULL")
@dlt.expect_or_fail("valid_count", "Count > 0")
def baby_names_prepared():
  return (
    dlt.read("baby_names_raw")
      .withColumnRenamed("Year", "Year_Of_Birth")
      .select("Year_Of_Birth", "First_Name", "Count")
  )

# COMMAND ----------

# DBTITLE 1,Top baby names 2021
@dlt.table(
  comment="A table summarizing counts of the top baby names for New York for 2021."
)
def top_baby_names_2021():
  return (
    dlt.read("baby_names_prepared")
      .filter(expr("Year_Of_Birth == 2021"))
      .groupBy("First_Name")
      .agg(sum("Count").alias("Total_Count"))
      .sort(desc("Total_Count"))
      .limit(10)
  )
