# Step1: To read results.json file and specifying the schema for the same

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

results_Schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", FloatType(), True),
    StructField("statusId", StringType(), True)
])

results_df = spark.read \
    .schema(results_Schema) \
    .json("/mnt/azureformula12dl/raw/results.json")
    
# Step2: Rename the columns and add a new column

from pyspark.sql.functions import current_timestamp
renamed_results_df = results_df.withColumnRenamed("resultId","result_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("positionText","position_text") \
.withColumnRenamed("positionOrder","position_order") \
.withColumnRenamed("fastestLap","fastest_lap") \
.withColumnRenamed("fastestLapTime","fastest_lap_time") \
.withColumnRenamed("fastestLapSpeed","fastest_lap_speed") \
.withColumn("ingestion_date",current_timestamp())



# Step3: Drop a column named statusId
final_results_df = renamed_results_df.drop("statusId")



# Step4: Wite the data to adls processed container also partitionBy race_id
final_results_df.write \
.mode("Overwrite") \
.partitionBy("race_id") \
.parquet("/mnt/azureformula12dl/processed/results/")



