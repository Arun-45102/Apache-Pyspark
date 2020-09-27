# Databricks notebook source
from os.path import join, abspath

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

warehouse_location = abspath('spark-warehouse')

# COMMAND ----------

spark = SparkSession.builder.appName('SQL Hive').config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")

# COMMAND ----------

spark.sql("LOAD DATA LOCAL INPATH 'dbfs:/FileStore/shared_uploads/49161@hexaware.com/kv1.txt' INTO TABLE src")

# COMMAND ----------

spark.sql("SELECT * FROM src").show()

# COMMAND ----------

spark.sql("SELECT COUNT(*) FROM src").show()

# COMMAND ----------

sqlDF = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

# COMMAND ----------

stringDS = sqlDF.rdd.map(lambda row: "Key: %d, Value: %s" % (row.key, row.value))

# COMMAND ----------

type(stringDS)

# COMMAND ----------

for record in stringDS.collect():
  print(record)

# COMMAND ----------

Record = Row("key","value")

# COMMAND ----------

recordsDF = spark.createDataFrame([Record(i, "val_" + str(i)) for i in range(1,101)])

# COMMAND ----------

recordsDF.show()

# COMMAND ----------

recordsDF.createOrReplaceTempView("records")

# COMMAND ----------

spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

# COMMAND ----------

pyspark

# COMMAND ----------


