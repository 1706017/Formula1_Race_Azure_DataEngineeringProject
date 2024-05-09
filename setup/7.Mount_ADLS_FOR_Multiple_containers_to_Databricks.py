
# Mount Azure DataLake for Multiple Containers
#==============================================
# 1.Here we are just creating a function that we will call everytime we need to mount a container 
# and we will just pass the storage name and container name to mount the storage on databricks


def mount_adls(storage_account_name,container_name):
    #Get secrets from Keyvault
    client_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-id-service-principle')
    tenant_id = dbutils.secrets.get(scope='formula1-scope',key='formula1-tenant-id-service-principle')
    client_secret = dbutils.secrets.get(scope='formula1-scope',key='formula1-client-secret-service-principle')

    #set spark configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Mount the storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}", #here we have followed naming convention as /storageaccount/container
    extra_configs = configs)

    #to list down all the mounts once the function ran
    display(dbutils.fs.mounts())

#here we are invoking the function with storage account name and container name that will mount the containers

mount_adls('azureformula12dl','raw')  

