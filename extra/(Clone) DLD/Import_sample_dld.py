# Databricks notebook source
# Load data from the CSV file
df = spark.read.format("csv").option("header", "true").load("/Workspace/Users/northstreem_gmail.com#ext#@northstreemgmail.onmicrosoft.com/Offices.csv")

# Display the first 10 rows
df.show(10)
