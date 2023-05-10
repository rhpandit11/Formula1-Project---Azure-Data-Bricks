# Databricks notebook source
# MAGIC %md
# MAGIC ### read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

constructor_schema = StructType(fields=[StructField("constructorId", IntegerType(), True),
                                        StructField("constructorRef", StringType(), True),
                                        StructField("name", StringType(), True),
                                        StructField("nationality", StringType(), True),
                                        StructField("url", StringType(), True)
])

# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_selected_df = constructor_df.select(col("constructorId"), col("constructorRef"), col("name"), col("nationality"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renamed Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_selected_df)

# COMMAND ----------

constructor_df = constructor_final_df.withColumnRenamed("constructorId", "constructor_id") \
                                              .withColumnRenamed("constructorRef", "constructor_ref") \
                                              .withColumn("data_source", lit(v_data_source))
                                                     



# COMMAND ----------

# MAGIC %md
# MAGIC ### write output to parquet file

# COMMAND ----------

constructor_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

dbutils.notebook.exit("success")