# Databricks notebook source
print("Hello, Databricks!")
print("each dics !")

# COMMAND ----------

path = "/mnt/"  # Replace with your desired directory path
files = dbutils.fs.ls(path)
for file in files:
    print(file)

# COMMAND ----------

# MAGIC %sql SELECT * FROM system.information_schema.tables WHERE table_catalog = 'mycatalog' and table_type = 'MANAGED'

# COMMAND ----------

path = "/mnt/"  # Replace with your desired directory path
files = dbutils.fs.ls(path)
for file in files:
    print(file)

# COMMAND ----------

path = "/mnt/dld2"  # Replace with your desired directory path
files = dbutils.fs.ls(path)
for file in files:
    print(file)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select top 10 rows from the file located in dbfs:/mnt/dld2/Rent_Contracts.csv
# MAGIC -- Skip the 1st row since it is a header
# MAGIC -- Partition the table by "contract_start_date" column
# MAGIC -- Write the table to Parquet format, partitioned by date (day)
# MAGIC
# MAGIC SELECT * FROM csv.`dbfs:/mnt/dld2/Rent_Contracts.csv` 

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("Read and Partition Data").getOrCreate()

# Read the CSV file, assuming the first row is the header
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("dbfs:/mnt/dld2/Rent_Contracts.csv")

# We need to ensure 'contract_start_date' is in the correct date format for partitioning
# Assuming the dates are in 'yyyy-MM-dd' format, if not you might need to parse them first
df = df.withColumn("contract_start_date", df["contract_start_date"].cast("date"))

# Now, write the DataFrame back to disk as a Parquet file, partitioned by 'contract_start_date'
df.write.partitionBy("contract_start_date").format("parquet").save("dbfs:/mnt/dld2/Parquet_Rent_Contracts")


# COMMAND ----------

from pyspark.sql import SparkSession
from delta.tables import *

# Create a Spark session
spark = SparkSession.builder \
  .appName("Create Delta Table") \
  .config("spark.sql.extensions", "io.delta.dsl.DeltaSparkSessionExtension") \
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
  .getOrCreate()

# Read the Parquet files into a DataFrame
df = spark.read.format("parquet").load("dbfs:/mnt/dld2/Parquet_Rent_Contracts/")

# Write the DataFrame to a Delta table
df.write.format("delta").save("dbfs:/mnt/dld2/delta_rent_contracts")

# Create a DeltaTable object
deltaTable = DeltaTable.forPath(spark, "dbfs:/mnt/dld2/delta_rent_contracts")

# Perform operations on the Delta table using the DeltaTable API
# For example, optimize the table for better performance
deltaTable.optimize()

# Stop the Spark session
spark.stop()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the database to the one containing the Delta table
# MAGIC USE delta_rent_contracts;
# MAGIC
# MAGIC -- Query the Delta table
# MAGIC SELECT * FROM rent_contracts_table;

# COMMAND ----------

#» Importing necessary libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
#*	Define schema for the sales table 
schema = StructType([
StructField("OrderID", StringType(), True), 
StructField("Product", StringType(), True), 
StructField("Category", StringType(), True), 
StructField("Price", FloatType(), True), 
StructField("Quantity", IntegerType(), True)
])
#	Define data for the sales table 
sales_data = [
("1001", "Laptop", "Electronics", 1200.50, 2), 
("1002", "Smartphone", "Electronics", 800.75, 3), 
("1003", "Headphones", "Electronics", 99.99, 5), 
("1004", "T-shirt", "Clothing", 25.00, 4), 
("1005", "Jeans", "Clothing", 50.00, 2)
]

# Create DataFrane
sales_df = spark.createDataFrame(sales_data, schema)
# Display the datafraae
display(sales_df)

# COMMAND ----------

# Drop the temp view vw_sales
spark.catalog.dropTempView("vw_sales")

# COMMAND ----------

total_price = spark.sql("select Category, sum(price) as tot_price from {table} group by category" ,table=sales_df) 
display(total_price)
