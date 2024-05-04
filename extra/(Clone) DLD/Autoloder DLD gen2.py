# Databricks notebook source
# MAGIC %sql
# MAGIC LIST '/Volumes/brick_dld/dld_raw/raw_dld'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Buildings
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC     path 'dbfs:/mnt/dld/dld_raw/raw_dld/Buildings.csv',
# MAGIC     header 'true',
# MAGIC     inferSchema 'true'
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM Buildings LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from read_files('/Volumes/brick_dld/dld_raw/raw_dld/Buildings.csv', format => 'csv') LIMIT 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from read_files('/Volumes/brick_dld/dld_raw/raw_dld/Transactions.csv', format => 'csv') LIMIT 10
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Buildings as
# MAGIC SELECT * FROM csv.`/Volumes/brick_dld/dld_raw/raw_dld/Buildings.csv`;

# COMMAND ----------

# MAGIC %sql SHOW EXTERNAL LOCATIONS;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL LOCATION my_external_location
# MAGIC URL 'abfss://dld@dldlakehouse.dfs.core.windows.net/'
# MAGIC WITH (credential = 'my_credential');

# COMMAND ----------

dbutils.fs.ls("/mnt/dld/dld_raw/raw_dld/")

# COMMAND ----------

dbutils.fs.unmount("/mnt/dld")
