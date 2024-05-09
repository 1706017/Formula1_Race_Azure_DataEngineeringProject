
#  Access Azure Data lake using Service Principle by hididng credentials via using Azure key vault + Databricks secret scope 
#  =============================================================================================================================
# 
#  1.Register Azure Service Principle
#  2.Generate secret/password for the application
#  3.Set spark config with app/client id, Directory/tenant id & Secret
#  4.Assign role storage blob contributor to the datalake



client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-id-service-principle')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id-service-principle')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-secret-service-principle')




spark.conf.set("fs.azure.account.auth.type.azureformula12dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.azureformula12dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.azureformula12dl.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.azureformula12dl.dfs.core.windows.net",client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.azureformula12dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

display(spark.read.option("header",True).csv("abfss://demo@azureformula12dl.dfs.core.windows.net/circuits.csv"))
