# Databricks notebook source
from pyspark.sql import SparkSession

# Create or get the Spark session
spark = SparkSession.builder.appName("DisplayCurrentUser").getOrCreate()

# Use Databricks utilities to get the current user
current_user = spark.sql("SELECT current_user()").collect()[0][0]

print("Current User:", current_user)


# COMMAND ----------

# MAGIC %sql
# MAGIC use CATALOG `hive_metastore`

# COMMAND ----------

# Import functions
from pyspark.sql.functions import col, current_timestamp

# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

df = spark.read.table(table_name)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %fs ls "/databricks-datasets/songs/data-001"

# COMMAND ----------

# MAGIC %fs head --maxBytes=10000 "/databricks-datasets/songs/README.md"
