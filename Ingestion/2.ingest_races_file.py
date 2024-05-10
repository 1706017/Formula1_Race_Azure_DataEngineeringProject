#Ingest races.csv file
# Step1: Read the csv file using the spark reader api 

#Specifying the schema for Dataframe and then reading the data 


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType

races_df_schema = StructType([StructField("raceId", IntegerType(), False),
             StructField("year", IntegerType(), True),
             StructField("round", IntegerType(), True),
             StructField("circuitId", IntegerType(), True),
             StructField("name", StringType(),True),
             StructField("date",DateType(),True),
             StructField("time",StringType(),True),
             StructField("url",StringType(),True)]
           )
races_df = spark.read \
.option("header",True) \
.schema(races_df_schema) \
.csv("/mnt/azureformula12dl/raw/races.csv")


# Step2 : Renaming the column Name

renamed_races_df = races_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("year","race_year") \
.withColumnRenamed("circuitId","circuit_id") 


# Step3 : Selecting specific Columns from DataFrame

from pyspark.sql.functions import col
selected_races_df = renamed_races_df.select(col("race_id"),
                        col("race_year"),
                        col("round"),
                         col("circuit_id"),
                         col("name"),
                         col("date"),
                         col("time"))


# Step 4: Adding new Columns 

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, count, isnull


final_race_df = selected_races_df.withColumn("ingestion_date", current_timestamp()) \
.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

display(final_race_df)
# Step 5: Writing the data into processed ADLS container

final_race_df.write \
.mode("Overwrite") \
.partitionBy("race_year") \
.parquet("/mnt/azureformula12dl/processed/races")

