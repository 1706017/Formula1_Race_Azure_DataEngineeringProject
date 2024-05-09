
# Explore the capabilities of dbutils secrets

# Import the necessary libraries
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Get the help for dbutils.secrets
help(spark._jvm.dbutils.secrets)

dbutils.secrets.help()


#it will list down all the secrets scope created 
dbutils.secrets.listScopes()


#it will show the key for the secret scope that we are passing here
dbutils.secrets.list(scope='formula1-scope')


dbutils.secrets.get(scope='formula1-scope',key='formula1dl-account-key')
