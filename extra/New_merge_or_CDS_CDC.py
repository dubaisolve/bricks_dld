# Databricks notebook source
# MAGIC %md
# MAGIC # Change data capture (CDC)

# COMMAND ----------

# MAGIC %md
# MAGIC APPLY CHANGES API: Simplify change data capture in Delta Live Tables
# MAGIC Article
# MAGIC 04/26/2024
# MAGIC 4 contributors
# MAGIC In this article
# MAGIC How is CDC implemented with Delta Live Tables?
# MAGIC What data objects are used for Delta Live Tables CDC processing?
# MAGIC Get data about records processed by a Delta Live Tables CDC query
# MAGIC Limitations
# MAGIC Show 5 more
# MAGIC Delta Live Tables simplifies change data capture (CDC) with the APPLY CHANGES API. Previously, the MERGE INTO statement was commonly used for processing CDC records on Azure Databricks. However, MERGE INTO can produce incorrect results because of out-of-sequence records, or require complex logic to re-order records.
# MAGIC
# MAGIC By automatically handling out-of-sequence records, the APPLY CHANGES API in Delta Live Tables ensures correct processing of CDC records and removes the need to develop complex logic for handling out-of-sequence records.
# MAGIC
# MAGIC The APPLY CHANGES API is supported in the Delta Live Tables SQL and Python interfaces, including support for updating tables with SCD type 1 and type 2:
# MAGIC
# MAGIC Use SCD type 1 to update records directly. History is not retained for records that are updated.
# MAGIC Use SCD type 2 to retain a history of records, either on all updates or on updates to a specified set of columns.

# COMMAND ----------

# MAGIC %md
# MAGIC Get data about records processed by a Delta Live Tables CDC query
# MAGIC The following metrics are captured by apply changes queries:
# MAGIC
# MAGIC num_upserted_rows: The number of output rows upserted into the dataset during an update.
# MAGIC num_deleted_rows: The number of existing output rows deleted from the dataset during an update.
# MAGIC The num_output_rows metric, which is output for non-CDC flows, is not captured for apply changes queries.

# COMMAND ----------

# MAGIC %md
# MAGIC Process SCD type 1 updates
# MAGIC The following code example demonstrates processing SCD type 1 updates:

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def users():
  return spark.readStream.format("delta").table("cdc_data.users")

dlt.create_streaming_table("target")

dlt.apply_changes(
  target = "target",
  source = "users",
  keys = ["userId"],
  sequence_by = col("sequenceNum"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create and populate the target table.
# MAGIC CREATE OR REFRESH STREAMING TABLE target;
# MAGIC
# MAGIC APPLY CHANGES INTO
# MAGIC   live.target
# MAGIC FROM
# MAGIC   stream(cdc_data.users)
# MAGIC KEYS
# MAGIC   (userId)
# MAGIC APPLY AS DELETE WHEN
# MAGIC   operation = "DELETE"
# MAGIC APPLY AS TRUNCATE WHEN
# MAGIC   operation = "TRUNCATE"
# MAGIC SEQUENCE BY
# MAGIC   sequenceNum
# MAGIC COLUMNS * EXCEPT
# MAGIC   (operation, sequenceNum)
# MAGIC STORED AS
# MAGIC   SCD TYPE 1;

# COMMAND ----------

# MAGIC %md
# MAGIC Process SCD type 2 updates
# MAGIC The following code example demonstrates processing SCD type 2 updates:

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, expr

@dlt.view
def users():
  return spark.readStream.format("delta").table("cdc_data.users")

dlt.create_streaming_table("target")

dlt.apply_changes(
  target = "target",
  source = "users",
  keys = ["userId"],
  sequence_by = col("sequenceNum"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation", "sequenceNum"],
  stored_as_scd_type = "2"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create and populate the target table.
# MAGIC CREATE OR REFRESH STREAMING TABLE target;
# MAGIC
# MAGIC APPLY CHANGES INTO
# MAGIC   live.target
# MAGIC FROM
# MAGIC   stream(cdc_data.users)
# MAGIC KEYS
# MAGIC   (userId)
# MAGIC APPLY AS DELETE WHEN
# MAGIC   operation = "DELETE"
# MAGIC SEQUENCE BY
# MAGIC   sequenceNum
# MAGIC COLUMNS * EXCEPT
# MAGIC   (operation, sequenceNum)
# MAGIC STORED AS
# MAGIC   SCD TYPE 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS cdc_data;
# MAGIC
# MAGIC CREATE TABLE
# MAGIC   cdc_data.users
# MAGIC AS SELECT
# MAGIC   col1 AS userId,
# MAGIC   col2 AS name,
# MAGIC   col3 AS city,
# MAGIC   col4 AS operation,
# MAGIC   col5 AS sequenceNum
# MAGIC FROM (
# MAGIC   VALUES
# MAGIC   -- Initial load.
# MAGIC   (124, "Raul",     "Oaxaca",      "INSERT", 1),
# MAGIC   (123, "Isabel",   "Monterrey",   "INSERT", 1),
# MAGIC   -- New users.
# MAGIC   (125, "Mercedes", "Tijuana",     "INSERT", 2),
# MAGIC   (126, "Lily",     "Cancun",      "INSERT", 2),
# MAGIC   -- Isabel is removed from the system and Mercedes moved to Guadalajara.
# MAGIC   (123, null,       null,          "DELETE", 6),
# MAGIC   (125, "Mercedes", "Guadalajara", "UPDATE", 6),
# MAGIC   -- This batch of updates arrived out of order. The above batch at sequenceNum 5 will be the final state.
# MAGIC   (125, "Mercedes", "Mexicali",    "UPDATE", 5),
# MAGIC   (123, "Isabel",   "Chihuahua",   "UPDATE", 5)
# MAGIC   -- Uncomment to test TRUNCATE.
# MAGIC   -- ,(null, null,      null,          "TRUNCATE", 3)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO my_streaming_table (id, name, __START_AT, __END_AT) VALUES (123, 'John Doe', 5, NULL);
