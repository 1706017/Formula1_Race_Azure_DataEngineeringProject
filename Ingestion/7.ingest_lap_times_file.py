#Ingest pit_stops.json file

# Step1: Read the data from csv file using spark reader api


from pyspark.sql.types import StructType,StructField,IntegerType,StringType
lap_times_schema = StructType([StructField("raceId",IntegerType(),False),
                               StructField("driverId",IntegerType(),True),
                               StructField("lap",IntegerType(),True),
                               StructField("position",IntegerType(),True),
                               StructField("time",StringType(),True),
                               StructField("milliseconds",IntegerType(),True)])


lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/azureformula12dl/raw/lap_times")


# Step2: Rename columns and add a new column
from pyspark.sql.functions import current_timestamp
renamed_lap_times_df = lap_times_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("raceId","race_id") \
.withColumn("ingestion_date",current_timestamp())

# Step3: Write the data to adls processed container

renamed_lap_times_df.write \
.mode("Overwrite") \
.parquet("/mnt/azureformula12dl/processed/lap_times/")
