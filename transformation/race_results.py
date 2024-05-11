# Databricks notebook source
# MAGIC %md
# MAGIC ## Read all the data that is required and also do renaming of columns so to avoid any ambiguity while joing the dataframes

# COMMAND ----------

drivers_df =spark.read.parquet("/mnt/azureformula12dl/processed/drivers") \
.withColumnRenamed("number","driver_number") \
.withColumnRenamed("name","driver_name") \
.withColumnRenamed("nationality","driver_nationality")

# COMMAND ----------

constructors_df =spark.read.parquet("/mnt/azureformula12dl/processed/constructors") \
.withColumnRenamed("name","team")

# COMMAND ----------

circuits_df = spark.read.parquet("/mnt/azureformula12dl/processed/circuits") \
.withColumnRenamed("location","circuit_location")

# COMMAND ----------

races_df = spark.read.parquet("/mnt/azureformula12dl/processed/races") \
.withColumnRenamed("name","race_name") \
.withColumnRenamed("race_timestamp","race_date")

# COMMAND ----------

results_df = spark.read.parquet("/mnt/azureformula12dl/processed/results") \
.withColumnRenamed("time","race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Joins circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df,races_df.circuit_id==circuits_df.circuit_id,"inner") \
.select(races_df.race_id,races_df.race_year,races_df.race_name,races_df.race_date,circuits_df.circuit_location)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df,results_df.race_id==race_circuits_df.race_id,"inner") \
                            .join(drivers_df,results_df.driver_id==drivers_df.driver_id,"inner") \
                            .join(constructors_df,results_df.constructor_id==constructors_df.constructor_id,"inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
final_df = race_results_df.select("race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality",
                       "team","grid","fastest_lap","race_time","points") \
           .withColumn("created_date",current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year ==2020 and race_name =='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing the race results to prsentation container of ADLS

# COMMAND ----------

final_df.write \
.mode("overwrite") \
.parquet("/mnt/azureformula12dl/presentation/race_results")

# COMMAND ----------


