# Ingesting Qualifying JSON file
# Step1: Read the data from Qualifying json file



from pyspark.sql.types import StructField,StructType,IntegerType,StringType

qualifying_schema = StructType([StructField("qualifyId",IntegerType(),False),
                                StructField("raceId",IntegerType(),True),
                                StructField("driverId",IntegerType(),True),
                                StructField("constructorId",IntegerType(),True),
                                StructField("number",IntegerType(),True),
                                StructField("position",IntegerType(),True),
                                StructField("q1",StringType(),True),
                                StructField("q2",StringType(),True),
                                StructField("q3",StringType(),True)])
qualifying_df = spark.read \
.schema(qualifying_schema) \
.json("/mnt/azureformula12dl/raw/qualifying/")

# Step2: Renaming Columns and Adding new Column
from pyspark.sql.functions import current_timestamp
renamed_qualify_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("constructorId","constructor_id") \
.withColumn("ingestion_date",current_timestamp())

#Step3: Write the data into datalake processed container


renamed_qualify_df.write \
.mode("Overwrite") \
.parquet("/mnt/azureformula12dl/processed/qualifying/")



