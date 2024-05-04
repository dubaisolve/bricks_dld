# Databricks notebook source
# MAGIC %md
# MAGIC # Load and process data incrementally with Delta Live Tables flows

# COMMAND ----------

# MAGIC %md
# MAGIC What is a flow?
# MAGIC In Delta Live Tables, a flow is a streaming query that processes source data incrementally to update a target streaming table. Most Delta Live Tables datasets you create in a pipeline define the flow as part of the query and do not require explicitly defining the flow. For example, you create a streaming table in Delta Live Tables in a single DDL command instead of using separate table and flow statements to create the streaming table:
# MAGIC
# MAGIC  Note
# MAGIC
# MAGIC This CREATE FLOW example is provided for illustrative purposes only and includes keywords that are not valid Delta Live Tables syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE STREAMING TABLE raw_data
# MAGIC AS SELECT * FROM source_data("/path/to/source/data")
# MAGIC
# MAGIC -- The above query is equivalent to the following statements:
# MAGIC CREATE STREAMING TABLE raw_data;
# MAGIC
# MAGIC CREATE FLOW raw_data
# MAGIC AS INSERT INTO raw_data BY NAME
# MAGIC SELECT * FROM source_data("/path/to/source/data");

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE append_target; -- Required only if the target table doesn't exist.
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   flow_name
# MAGIC AS INSERT INTO
# MAGIC   target_table BY NAME
# MAGIC SELECT * FROM
# MAGIC   source;

# COMMAND ----------

import dlt

dlt.create_streaming_table("<target-table-name>") # Required only if the target table doesn't exist.

@dlt.append_flow(
  target = "<target-table-name>",
  name = "<flow-name>", # optional, defaults to function name
  spark_conf = {"<key>" : "<value", "<key" : "<value>"}, # optional
  comment = "<comment>") # optional
def <function-name>():
  return (<streaming query>)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE kafka_target;
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   topic1
# MAGIC AS INSERT INTO
# MAGIC   kafka_target BY NAME
# MAGIC SELECT * FROM
# MAGIC   read_kafka(bootstrapServers => 'host1:port1,...', subscribe => 'topic1');
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   topic2
# MAGIC AS INSERT INTO
# MAGIC   kafka_target BY NAME
# MAGIC SELECT * FROM
# MAGIC   read_kafka(bootstrapServers => 'host1:port1,...', subscribe => 'topic2');

# COMMAND ----------

import dlt

dlt.create_streaming_table("kafka_target")

# Kafka stream from multiple topics
@dlt.append_flow(target = "kafka_target")
def topic1():
  return (
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,...")
      .option("subscribe", "topic1")
      .load()
  )

@dlt.append_flow(target = "kafka_target")
def topic2():
  return (
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,...")
      .option("subscribe", "topic2")
      .load()
  )

# COMMAND ----------

# MAGIC %md
# MAGIC Example: Run a one-time data backfill
# MAGIC The following examples run a query to append historical data to a streaming table:
# MAGIC
# MAGIC  Note
# MAGIC
# MAGIC To ensure a true one-time backfill when the backfill query is part of a pipeline that runs on a scheduled basis or continuously, remove the query after running the pipeline once. To append new data if it arrives in the backfill directory, leave the query in place.

# COMMAND ----------

import dlt

@dlt.table()
def csv_target():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .load("path/to/sourceDir")

@dlt.append_flow(target = "csv_target")
def backfill():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","csv")
    .load("path/to/backfill/data/dir")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE csv_target
# MAGIC AS SELECT * FROM
# MAGIC   cloud_files(
# MAGIC     "path/to/sourceDir",
# MAGIC     "csv"
# MAGIC   );
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   backfill
# MAGIC AS INSERT INTO
# MAGIC   csv_target BY NAME
# MAGIC SELECT * FROM
# MAGIC   cloud_files(
# MAGIC     "path/to/backfill/data/dir",
# MAGIC     "csv"
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC Example: Use append flow processing instead of UNION
# MAGIC Instead of using a query with a UNION clause, you can use append flow queries to combine multiple sources and write to a single streaming table. Using append flow queries instead of UNION allows you to append to a streaming table from multiple sources without running a full refresh.
# MAGIC
# MAGIC The following Python example includes a query that combines multiple data sources with a UNION clause:

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

@dlt.create_table(name="raw_orders")
def unioned_raw_orders():
  raw_orders_us =
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .load("/path/to/orders/us")

  raw_orders_eu =
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .load("/path/to/orders/eu")

  return raw_orders_us.union(raw_orders_eu)

# COMMAND ----------

dlt.create_streaming_table("raw_orders")

@dlt.append_flow(target="raw_orders")
def raw_oders_us():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("/path/to/orders/us")

@dlt.append_flow(target="raw_orders")
def raw_orders_eu():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("/path/to/orders/eu")

# Additional flows can be added without the full refresh that a UNION query would require:
@dlt.append_flow(target="raw_orders")
def raw_orders_apac():
  return spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .load("/path/to/orders/apac")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE raw_orders;
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   raw_orders_us
# MAGIC AS INSERT INTO
# MAGIC   raw_orders BY NAME
# MAGIC SELECT * FROM
# MAGIC   cloud_files(
# MAGIC     "/path/to/orders/us",
# MAGIC     "csv"
# MAGIC   );
# MAGIC
# MAGIC CREATE FLOW
# MAGIC   raw_orders_eu
# MAGIC AS INSERT INTO
# MAGIC   raw_orders BY NAME
# MAGIC SELECT * FROM
# MAGIC   cloud_files(
# MAGIC     "/path/to/orders/eu",
# MAGIC     "csv"
# MAGIC   );
# MAGIC
# MAGIC -- Additional flows can be added without the full refresh that a UNION query would require:
# MAGIC CREATE FLOW
# MAGIC   raw_orders_apac
# MAGIC AS INSERT INTO
# MAGIC   raw_orders BY NAME
# MAGIC SELECT * FROM
# MAGIC   cloud_files(
# MAGIC     "/path/to/orders/apac",
# MAGIC     "csv"
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC # Combine streaming tables and materialized views in a single pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING TABLE streaming_bronze
# MAGIC AS SELECT * FROM cloud_files(
# MAGIC   "abfss://path/to/raw/data", "json"
# MAGIC )
# MAGIC
# MAGIC CREATE OR REFRESH STREAMING TABLE streaming_silver
# MAGIC AS SELECT * FROM STREAM(LIVE.streaming_bronze) WHERE...
# MAGIC
# MAGIC CREATE OR REFRESH LIVE TABLE live_gold
# MAGIC AS SELECT count(*) FROM LIVE.streaming_silver GROUP BY user_id
