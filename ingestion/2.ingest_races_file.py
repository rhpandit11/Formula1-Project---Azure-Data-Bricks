# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.option("header", True).csv("dbfs:/mnt/formula1dlpj/raw/races.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding ingestion date and race_time stamp to the data frame

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = add_ingestion_date(races_df) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select only the requires column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col("raceId"), col("year"), col("round"), col("circuitId"), col("name"), 
col("ingestion_date"), col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### renamed column as requires

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_selected_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("year", "race_year") \
                                    .withColumnRenamed("circuitId", "circuit_id") \
                                    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### write the output to processed container in parquet format

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(spark.read.parquet(f'{processed_folder_path}/races'))

# COMMAND ----------

dbutils.notebook.exit("success")