# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renamed the column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

final_lap_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

final_df = final_lap_df.withColumnRenamed("driverId", "driver_id") \
                       .withColumnRenamed("raceId", "race_id") \
                       .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet('/mnt/formula1dlpj/processed/lap_times'))

# COMMAND ----------

dbutils.notebook.exit("success")