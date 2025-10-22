// main.bicep – orchestrates the full Step 7 architecture
param location string = 'eastus'
param storageAccountName string = 'ungcapstor01'

module storage 'storage_account.bicep' = {
  name: 'storage'
  params: {
    storageAccountName: storageAccountName
    location: location
  }
}

module vnet 'vnet.bicep' = {
  name: 'vnet'
  params: {
    vnetName: 'ungcapvnet01'
    location: location
  }
}

module keyvault 'keyvault.bicep' = {
  name: 'keyvault'
  params: {
    keyVaultName: 'ungcapkv01'
    location: location
  }
}

module databricks 'databricks.bicep' = {
  name: 'databricks'
  params: {
    databricksName: 'ungcapdbw01'
    location: location
  }
  dependsOn: [vnet]
}

module functionapp 'functionapp.bicep' = {
  name: 'functionapp'
  params: {
    functionAppName: 'ungcapfunc01'
    storageAccountName: storageAccountName
    location: location
  }
  dependsOn: [storage]
}

module monitoring 'monitoring.bicep' = {
  name: 'monitoring'
  params: {
    workspaceName: 'ungcaplog01'
    location: location
  }
  dependsOn: [storage]
}
