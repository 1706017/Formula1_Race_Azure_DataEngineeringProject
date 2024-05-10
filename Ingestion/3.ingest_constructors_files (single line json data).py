# Step1: Read Constructors.json data and defining the schema

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

constructors_schema = StructType([StructField("constructorId",IntegerType(),False),
                                  StructField("constructorRef",StringType(),True),
                                  StructField("name",StringType(),True),
                                  StructField("nationality",StringType(),True),
                                  StructField("url",StringType(),True)])


constructors_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/azureformula12dl/raw/constructors.json")

 #Step2 :Droping one column
from pyspark.sql.functions import col
dropped_constructors_df = constructors_df.drop(col("url"))



 #Step3: Renaming column names

renamed_constructor_df = dropped_constructors_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref")

# Step4: Adding new column
from pyspark.sql.functions import current_timestamp
final_constructors_df = renamed_constructor_df.withColumn("ingestion_date",current_timestamp())

# Step5: Writing data to new container in ADLS in processed container
final_constructors_df.write \
.mode("Overwrite") \
.parquet("/mnt/azureformula12dl/processed/constructors")

