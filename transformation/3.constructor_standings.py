#Here it will display the rankings of constructor or teams
race_results_df = spark.read.parquet("/mnt/azureformula12dl/presentation/race_results")

from pyspark.sql.functions import col,when,count,sum

constructor_standings_df = race_results_df \
.groupBy("race_year","team") \
.agg(sum("points").alias("total_points"),count(when(col("position")==1,True)).alias("wins"))


from pyspark.sql.functions import desc,rank
from pyspark.sql.window import Window

constructor_spec = Window.partitionBy("race_year").orderBy(desc("total_points"),desc("wins"))
final_df = constructor_standings_df.withColumn("rank",rank().over(constructor_spec))


final_df.write.parquet("/mnt/azureformula12dl/presentation/constructor_standings")



