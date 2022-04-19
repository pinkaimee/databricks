# Databricks notebook source
read_format = 'delta'
load_path = '/databricks-datasets/learning-spark-v2/people/people-10m.delta'

# COMMAND ----------

people = spark.read \
  .format(read_format) \
  .load(load_path)

# COMMAND ----------

display(people)

# COMMAND ----------

write_format = 'delta'
write_mode = 'overwrite'
partition_by = 'gender'
save_path = '/tmp/delta/people-10m'

# COMMAND ----------

people.write \
  .format(write_format) \
  .partitionBy(partition_by) \
  .mode(write_mode) \
  .save(save_path)

# COMMAND ----------

people_delta = spark.read.format(read_format).load(save_path)

# COMMAND ----------

display(people_delta)

# COMMAND ----------

table_name = 'people10m'
 
display(spark.sql("DROP TABLE IF EXISTS " + table_name))
 
display(spark.sql("CREATE TABLE " + table_name + " USING DELTA LOCATION '" + save_path + "'"))

# COMMAND ----------

display(spark.table(table_name).select('id', 'salary').orderBy('salary', ascending = False))

# COMMAND ----------

df_people = spark.table(table_name)
display(df_people.select('gender').orderBy('gender', ascending = False).groupBy('gender').count())

# COMMAND ----------

display(spark.table(table_name).select("salary").orderBy("salary", ascending = False))

# COMMAND ----------

people_delta.count()

# COMMAND ----------

display(spark.sql("SHOW PARTITIONS " + table_name))

# COMMAND ----------

dbutils.fs.ls('dbfs:/tmp/delta/people-10m/gender=M/')

# COMMAND ----------

display(spark.sql("OPTIMIZE " + table_name))

# COMMAND ----------

display(spark.sql("DESCRIBE HISTORY " + table_name))

# COMMAND ----------

display(spark.sql("DESCRIBE DETAIL " + table_name))

# COMMAND ----------

display(spark.sql("DESCRIBE FORMATTED " + table_name))

# COMMAND ----------

spark.sql("DROP TABLE " + table_name)

# COMMAND ----------

dbutils.fs.rm(save_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC  # Hello This is a Title

# COMMAND ----------

