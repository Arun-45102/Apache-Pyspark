# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('date').getOrCreate()

# COMMAND ----------

df = spark.read.format("csv").load("dbfs:/FileStore/shared_uploads/49161@hexaware.com/appl_stock.csv",inferSchema=True,header=True)

# COMMAND ----------

from pyspark.sql.functions import dayofmonth,hour,dayofyear,month,year,weekofyear,format_number,date_format

# COMMAND ----------

df.select(dayofmonth(df['Date'])).show()

# COMMAND ----------

df.select(month(df['Date'])).show()

# COMMAND ----------

df.select(year(df['Date'])).show()

# COMMAND ----------

newdf = df.withColumn("Year",year(df['Date']))

# COMMAND ----------

result = newdf.groupBy("Year").mean().select(["Year","avg(Close)"])

# COMMAND ----------

result.show()

# COMMAND ----------

new = result.withColumnRenamed("avg(Close)","Average Close Price")

# COMMAND ----------

new.select(['Year',format_number('Average Close Price',2).alias('Avg Close')]).show()

# COMMAND ----------


