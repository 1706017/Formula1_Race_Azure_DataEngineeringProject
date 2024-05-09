
#  Access Azure Data lake using SAS Token by Hiding the credentials via using azure key vault + Databricks secret scope
# =========================================
# 
#  1.set the spark config for SAS token
#  2.List files from demo container
#  3.Read data from circuits.csv file



#Here we are passing the credentials into a variable via azure key vault + Secrets scope
formula1_dl_sas_token = dbutils.secrets.get(scope = 'formula1-scope',key = 'formula1-demo-sas-token')



#Here we are adding the three configuration for sas token to be used to access the datalake
spark.conf.set("fs.azure.account.auth.type.azureformula12dl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.azureformula12dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.azureformula12dl.dfs.core.windows.net",formula1_dl_sas_token)

#Here we are trying to display the data from the container demo 
display(dbutils.fs.ls("abfss://demo@azureformula12dl.dfs.core.windows.net"))

#to read the data from the container demo in adls 
display(spark.read.option("header",True).csv("abfss://demo@azureformula12dl.dfs.core.windows.net/circuits.csv"))


