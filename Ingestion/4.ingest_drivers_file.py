# Step1: Reading Drivers.json file and specifying the schema

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

nameSchema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])

driverSchema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", nameSchema),
    StructField("dob", DateType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])

driver_df = spark.read \
.schema(driverSchema) \
.json("/mnt/azureformula12dl/raw/drivers.json")


# Step2: Drop a column named url

from pyspark.sql.functions import col
dropped_drivers_df = driver_df.drop(col("url"))

#Step3: Renaming the columns
renamed_drivers_df = dropped_drivers_df.withColumnRenamed("driverId","driver_id") \
.withColumnRenamed("driverRef","driver_ref")

# Step4: Adding a new column

from pyspark.sql.functions import current_timestamp
new_added_colum_drivers_df = renamed_drivers_df.withColumn("ingestion_date",current_timestamp())

# Step5: Adding a new column by concatinating the existing two columns
from pyspark.sql.functions import col,concat,lit
final_drivers_df = new_added_colum_drivers_df.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# Step6: Writing the drivers file into ADLS processed container

final_drivers_df.write \
.mode("Overwrite") \
.parquet("/mnt/azureformula12dl/processed/drivers")
