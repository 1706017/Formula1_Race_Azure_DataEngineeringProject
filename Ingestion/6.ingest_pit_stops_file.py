#Ingest pit_stops.json file
# Step1: Read the data from json file using spark reader api

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

pit_stops_schema = StructType([StructField("raceId",IntegerType(),False),
                               StructField("driverId",IntegerType(),True),
                               StructField("stop",StringType(),True),
                               StructField("lap",IntegerType(),True),
                               StructField("time",StringType(),True),
                               StructField("duration",StringType(),True),
                               StructField("milliseconds",IntegerType(),True)])
                               


ps_df = spark.read \
.schema(pit_stops_schema) \
.json("/mnt/azureformula12dl/raw/pit_stops.json")
# Step2: Rename columns and add a new column


from pyspark.sql.functions import current_timestamp
renamed_ps_df = ps_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("raceId","race_id") \
.withColumn("ingestion_date",current_timestamp())

# Step3: Write the data to adls processed container
renamed_ps_df.write \
.mode("Overwrite") \
.parquet("/mnt/azureformula12dl/processed/pit_stops/")
