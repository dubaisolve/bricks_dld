# Databricks notebook source
# List the contents of the directory
dbutils.fs.ls("dbfs:/user/northstreem_gmail_com/copy-into-demo")

# COMMAND ----------

# MAGIC %sql
# MAGIC use CATALOG `hive_metastore`

# COMMAND ----------

# Set parameters for isolation in workspace and reset demo

username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
database = f"copyinto_{username}_db"
source = f"dbfs:/user/{username}/copy-into-demo"

spark.sql(f"SET c.username='{username}'")
spark.sql(f"SET c.database={database}")
spark.sql(f"SET c.source='{source}'")

spark.sql("DROP DATABASE IF EXISTS ${c.database} CASCADE")
spark.sql("CREATE DATABASE ${c.database}")
spark.sql("USE ${c.database}")

dbutils.fs.rm(source, True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table user_ping_raw;
# MAGIC drop table user_ids;
# MAGIC drop table user_ping_target;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Configure random data generator
# MAGIC
# MAGIC CREATE TABLE user_ping_raw
# MAGIC (user_id STRING, ping INTEGER, time TIMESTAMP)
# MAGIC USING json
# MAGIC LOCATION ${c.source};
# MAGIC
# MAGIC CREATE TABLE user_ids (user_id STRING);
# MAGIC
# MAGIC INSERT INTO user_ids VALUES
# MAGIC ("potato_luver"),
# MAGIC ("beanbag_lyfe"),
# MAGIC ("default_username"),
# MAGIC ("the_king"),
# MAGIC ("n00b"),
# MAGIC ("frodo"),
# MAGIC ("data_the_kid"),
# MAGIC ("el_matador"),
# MAGIC ("the_wiz");
# MAGIC
# MAGIC CREATE FUNCTION get_ping()
# MAGIC     RETURNS INT
# MAGIC     RETURN int(rand() * 250);
# MAGIC
# MAGIC CREATE FUNCTION is_active()
# MAGIC     RETURNS BOOLEAN
# MAGIC     RETURN CASE
# MAGIC         WHEN rand() > .25 THEN true
# MAGIC         ELSE false
# MAGIC         END;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Write a new batch of data to the data source
# MAGIC
# MAGIC INSERT INTO user_ping_raw
# MAGIC SELECT *,
# MAGIC   get_ping() ping,
# MAGIC   current_timestamp() time
# MAGIC FROM (
# MAGIC   SELECT *
# MAGIC   FROM user_ids
# MAGIC   WHERE (SELECT is_active())=true
# MAGIC ) subquery;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table and load data
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS user_ping_target;
# MAGIC
# MAGIC COPY INTO user_ping_target
# MAGIC FROM ${c.source}
# MAGIC FILEFORMAT = JSON
# MAGIC FORMAT_OPTIONS ("mergeSchema" = "true")
# MAGIC COPY_OPTIONS ("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Review updated table
# MAGIC
# MAGIC SELECT * FROM user_ping_target

# COMMAND ----------

# MAGIC %md
# MAGIC # 3 runs and then 5th run --- folder in step 1 is populated from random data insert to raw table which is connected to stoage /user/copyinto/

# COMMAND ----------

# MAGIC %md
# MAGIC [FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_SUCCESS', name='_SUCCESS', size=0, modificationTime=1714601760000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_6487102831883699152', name='_committed_6487102831883699152', size=113, modificationTime=1714601760000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_7546088379579464566', name='_committed_7546088379579464566', size=113, modificationTime=1714601758000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_8446680688653377596', name='_committed_8446680688653377596', size=113, modificationTime=1714601729000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_6487102831883699152', name='_started_6487102831883699152', size=0, modificationTime=1714601760000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_7546088379579464566', name='_started_7546088379579464566', size=0, modificationTime=1714601757000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_8446680688653377596', name='_started_8446680688653377596', size=0, modificationTime=1714601728000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-6487102831883699152-0b890021-cdb9-4ce8-aa9a-bb47264fb2c4-62-1-c000.json', name='part-00000-tid-6487102831883699152-0b890021-cdb9-4ce8-aa9a-bb47264fb2c4-62-1-c000.json', size=477, modificationTime=1714601760000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-7546088379579464566-b5ff9801-bd2f-4565-8ef7-af5e7397e9ab-61-1-c000.json', name='part-00000-tid-7546088379579464566-b5ff9801-bd2f-4565-8ef7-af5e7397e9ab-61-1-c000.json', size=0, modificationTime=1714601758000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-8446680688653377596-b3c94ac0-0e9e-4b30-aa4b-8a17146dda81-39-1-c000.json', name='part-00000-tid-8446680688653377596-b3c94ac0-0e9e-4b30-aa4b-8a17146dda81-39-1-c000.json', size=417, modificationTime=1714601729000)]
# MAGIC
# MAGIC  -------------------------------------------------------------------------------------------
# MAGIC
# MAGIC  [FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_SUCCESS', name='_SUCCESS', size=0, modificationTime=1714601947000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_5907758529655672630', name='_committed_5907758529655672630', size=113, modificationTime=1714601947000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_6487102831883699152', name='_committed_6487102831883699152', size=113, modificationTime=1714601760000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_7222190125168643798', name='_committed_7222190125168643798', size=113, modificationTime=1714601944000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_7546088379579464566', name='_committed_7546088379579464566', size=113, modificationTime=1714601758000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_committed_8446680688653377596', name='_committed_8446680688653377596', size=113, modificationTime=1714601729000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_5907758529655672630', name='_started_5907758529655672630', size=0, modificationTime=1714601946000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_6487102831883699152', name='_started_6487102831883699152', size=0, modificationTime=1714601760000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_7222190125168643798', name='_started_7222190125168643798', size=0, modificationTime=1714601944000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_7546088379579464566', name='_started_7546088379579464566', size=0, modificationTime=1714601757000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/_started_8446680688653377596', name='_started_8446680688653377596', size=0, modificationTime=1714601728000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-5907758529655672630-d36e0cef-a02f-4237-aae3-a1c6a8a893c6-82-1-c000.json', name='part-00000-tid-5907758529655672630-d36e0cef-a02f-4237-aae3-a1c6a8a893c6-82-1-c000.json', size=348, modificationTime=1714601946000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-6487102831883699152-0b890021-cdb9-4ce8-aa9a-bb47264fb2c4-62-1-c000.json', name='part-00000-tid-6487102831883699152-0b890021-cdb9-4ce8-aa9a-bb47264fb2c4-62-1-c000.json', size=477, modificationTime=1714601760000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-7222190125168643798-28c4a1ea-0d8b-40df-9269-ca3ee947f230-81-1-c000.json', name='part-00000-tid-7222190125168643798-28c4a1ea-0d8b-40df-9269-ca3ee947f230-81-1-c000.json', size=488, modificationTime=1714601944000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-7546088379579464566-b5ff9801-bd2f-4565-8ef7-af5e7397e9ab-61-1-c000.json', name='part-00000-tid-7546088379579464566-b5ff9801-bd2f-4565-8ef7-af5e7397e9ab-61-1-c000.json', size=0, modificationTime=1714601758000),
# MAGIC  FileInfo(path='dbfs:/user/northstreem_gmail_com/copy-into-demo/part-00000-tid-8446680688653377596-b3c94ac0-0e9e-4b30-aa4b-8a17146dda81-39-1-c000.json', name='part-00000-tid-8446680688653377596-b3c94ac0-0e9e-4b30-aa4b-8a17146dda81-39-1-c000.json', size=417, modificationTime=1714601729000)]

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.copyinto_northstreem_gmail_com_db.user_ping_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC # Now from ADLS which is already connected via credentials in unity catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists brokers_delta;

# COMMAND ----------

# Read the CSV into a DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("abfss://dld@dldlakehouse.dfs.core.windows.net/brokers-2023-05-29.csv")

# Replace the invalid characters in the column names
new_column_names = [column.replace(' ', '_').replace('-', '_') for column in df.columns]
df = df.toDF(*new_column_names)

# Write the DataFrame to a Delta table
df.write.format("delta").mode("overwrite").saveAsTable("brokers_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brokers_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop TABLE brokers_delta

# COMMAND ----------

# Load the CSV to a DataFrame to infer the schema
df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://dld@dldlakehouse.dfs.core.windows.net/brokers-2023-05-29.csv")

df.printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE brokers_delta (
# MAGIC   `Broker Number` STRING,
# MAGIC   `Broker Name` STRING,
# MAGIC   Gender STRING,
# MAGIC   `License Start Date` TIMESTAMP,
# MAGIC   `License End Date` STRING,
# MAGIC   Website STRING,
# MAGIC   Phone STRING,
# MAGIC   Fax STRING,
# MAGIC   `Real Estate Number` STRING,
# MAGIC   `Real Estate Name` STRING
# MAGIC )
# MAGIC USING delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from brokers_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO brokers_delta
# MAGIC FROM 'abfss://dld@dldlakehouse.dfs.core.windows.net/brokers-2023-05-29.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS (
# MAGIC   'header' = 'true',
# MAGIC   'inferSchema' = 'true'
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM brokers_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO my_json_data
# MAGIC   FROM 'abfss://container@storageAccount.dfs.core.windows.net/base/path'
# MAGIC   FILEFORMAT = JSON
# MAGIC   FILES = ('f1.json', 'f2.json', 'f3.json', 'f4.json', 'f5.json')
# MAGIC
# MAGIC  -- The second execution will not copy any data since the first command already loaded the data
# MAGIC  COPY INTO my_json_data
# MAGIC    FROM 'abfss://container@storageAccount.dfs.core.windows.net/base/path'
# MAGIC    FILEFORMAT = JSON
# MAGIC    FILES = ('f1.json', 'f2.json', 'f3.json', 'f4.json', 'f5.json')

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO my_delta_table
# MAGIC   FROM (SELECT to_date(dt) dt, event as measurement, quantity::double
# MAGIC           FROM 'abfss://container@storageAccount.dfs.core.windows.net/base/path')
# MAGIC   FILEFORMAT = AVRO

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO delta.`abfss://container@storageAccount.dfs.core.windows.net/deltaTables/target`
# MAGIC   FROM (SELECT key, index, textData, 'constant_value'
# MAGIC           FROM 'abfss://container@storageAccount.dfs.core.windows.net/base/path')
# MAGIC   FILEFORMAT = CSV
# MAGIC   PATTERN = 'folder1/file_[a-g].csv'
# MAGIC   FORMAT_OPTIONS('header' = 'true')
# MAGIC
# MAGIC -- The example below loads CSV files without headers in ADLS Gen2 using COPY INTO.
# MAGIC -- By casting the data and renaming the columns, you can put the data in the schema you want
# MAGIC COPY INTO delta.`abfss://container@storageAccount.dfs.core.windows.net/deltaTables/target`
# MAGIC   FROM (SELECT _c0::bigint key, _c1::int index, _c2 textData
# MAGIC         FROM 'abfss://container@storageAccount.dfs.core.windows.net/base/path')
# MAGIC   FILEFORMAT = CSV
# MAGIC   PATTERN = 'folder1/file_[a-g].csv'

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO my_table
# MAGIC FROM '/path/to/files'
# MAGIC FILEFORMAT = <format>
# MAGIC [VALIDATE ALL]
# MAGIC FORMAT_OPTIONS ('ignoreCorruptFiles' = 'true')
