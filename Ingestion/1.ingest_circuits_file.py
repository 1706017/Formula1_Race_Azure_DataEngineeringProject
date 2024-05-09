
#Ingest Cirtuits.csv file
######## Step1: Read the csv file using the spark reader api 


#One way of defining the schema for dataframe
"""schema_structure = "circuitId int,circuitRef string,name string,location string,country string,lat double,lng double,alt int,url string" """
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

#Other way of specifying the scheam of dataframe

schema_structure = StructType(fields = [
  StructField("circuitId", IntegerType(),False),  #here false means nullable=false i.e it cannot be null 
  StructField("circuitRef", StringType(),True),
  StructField("name", StringType(),True),
  StructField("location", StringType(),True),
  StructField("country", StringType(),True),
  StructField("lat", DoubleType(),True),
  StructField("lng", DoubleType(),True),
  StructField("alt", IntegerType(),True),
  StructField("url", StringType(),True)
])



circuits_df = spark.read \
.option("header",True) \
.schema(schema_structure) \
.csv("dbfs:/mnt/azureformula12dl/raw/circuits.csv")

#Selecting only the required columns

#Here it is one way of selecting the columns from Data frame 
"""selected_circuits_df = circuits_df.select('circuitId',
                                          'circuitRef','name','location',
                                          'country','lat','lng','alt')"""

#Here it is another way of selecting the columns from Dataframe and it is more better as we can do some operations on col as #well
from pyspark.sql.functions import col

selected_circuits_df_new = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country")
                                          ,col("lat"),col("lng"),col("alt"))


# To rename the columns name


renamed_circuits_df = selected_circuits_df_new.withColumnRenamed("circuitId","circuit_id") \
.withColumnRenamed("circuitRef","circuit_ref") \
.withColumnRenamed("lat","latitude") \
.withColumnRenamed("lng","longitude") \
.withColumnRenamed("alt","altitude")


# Adding a new column named ingestion_date which will show the cuurent time stamp when data is getting ingested


from pyspark.sql.functions import current_timestamp
circuits_final_df = renamed_circuits_df.withColumn("ingestion_date",current_timestamp())


# Write data to datalake in parquet format

circuits_final_df.write.mode("overwrite").parquet("/mnt/azureformula12dl/processed/circuits")

display(spark.read.parquet("/mnt/azureformula12dl/processed/circuits"))
