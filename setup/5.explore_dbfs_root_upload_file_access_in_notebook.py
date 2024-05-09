
#  EXPLORE DBFS ROOT (Databricks file system)
#  1.List all the folders in dbfs root
#  2.Interact with dbfs file browser
#  3.upload file to dbfs root



#to display the contents inside /
display(dbutils.fs.ls('/'))


#to display all the folders/files in dbfs root
display(dbutils.fs.ls('/FileStore'))


#to read all the files in dbfs root under /FileStore 
display(spark.read.csv("dbfs:/FileStore/circuits.csv"))
