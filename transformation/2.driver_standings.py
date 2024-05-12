# Produce Driver standings
race_results = spark.read.parquet("/mnt/azureformula12dl/presentation/race_results")

from pyspark.sql.functions import sum,count,when,col
driver_standings_df = race_results \
.groupBy("race_year","driver_name","driver_nationality","team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position")==1,True)).alias("wins"))

from pyspark.sql.window import Window
from pyspark.sql.functions import rank,desc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))


final_df.write.mode("overwrite").parquet("/mnt/azureformula12dl/presentation/driver_standings")



