
# Mount Azure DataLake using Service Principle 
# ==============================================

# 1.Get client id,tenant_id,client_secret from key vault
# 2.Set spark config with app/client id, Directory/tenant id & Secret
# 3.call the file system utility mount to mount the storage 
# 4.Explore other file system utilities related to mount (list all mounts,unmount)




client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-id-service-principle')
tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id-service-principle')
client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-secret-service-principle')


configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


dbutils.fs.mount(
  source = "abfss://demo@azureformula12dl.dfs.core.windows.net/",
  mount_point = "/mnt/azureformula12dl/demo", #here we have followed naming convention as /storageaccount/container
  extra_configs = configs)

display(dbutils.fs.ls("/mnt/azureformula12dl/demo"))

display(spark.read.csv("dbfs:/mnt/azureformula12dl/demo/circuits.csv"))

display(dbutils.fs.mounts()) #here using this we can able to see all the mount points 

dbutils.fs.unmount('/mnt/azureformula12dl/demo') #Here we have unmounted the datalake from databricks 
