# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

circuits_df = spark.read.option("header", True).csv("dbfs:/mnt/formula1dlpj/raw/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", StringType(), True),
                                     StructField("lng", StringType(), True), 
                                     StructField("alt", StringType(), True),  
                                     StructField("url", StringType(), True), 



])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select the required column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuit_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rename the column as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuit_renamed_df = circuit_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                        .withColumnRenamed("circuitRef", "circuit_ref") \
                                        .withColumnRenamed("lat", "latitude") \
                                        .withColumnRenamed("lng", "longitude") \
                                        .withColumnRenamed("alt", "altitude") \
                                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add Ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuit_final_df = add_ingestion_date(circuit_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write data to datalake as parquet

# COMMAND ----------

circuit_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits

# COMMAND ----------

dbutils.notebook.exit("success")