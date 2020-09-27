# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('sparksql').getOrCreate()

# COMMAND ----------

df = spark.read.load("dbfs:/FileStore/shared_uploads/49161@hexaware.com/users.parquet")

# COMMAND ----------

df.show()

# COMMAND ----------

df.select("name").show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/49161@hexaware.com/employees.json")

# COMMAND ----------

df1.show()

# COMMAND ----------

df1.write.parquet("people.parquet")

# COMMAND ----------

parquetFile = spark.read.parquet("people.parquet")

# COMMAND ----------

parquetFile.createOrReplaceTempView("parquetFile")

# COMMAND ----------

sal = spark.sql("SELECT name FROM parquetFile WHERE salary>=3500 AND salary<=4000")

# COMMAND ----------

sal.show()

# COMMAND ----------

from os.path import join, abspath

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

warehouse_location = abspath('spark-warehouse')

# COMMAND ----------


