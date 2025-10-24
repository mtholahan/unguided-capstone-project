@description('Azure region for deployment')
param location string = resourceGroup().location

// Virtual Network
module vnet 'vnet.bicep' = {
  name: 'vnetDeployment'
  params: {
    location: location
    vnetName: 'ungcap-vnet'
  }
}

// Storage Account
module storage 'storage_account.bicep' = {
  name: 'storageDeployment'
  dependsOn: [
    vnet
  ]
  params: {
    location: location
    storageAccountName: 'ungcapstorage01'
  }
}

// Key Vault
module keyvault 'keyvault.bicep' = {
  name: 'keyvaultDeployment'
  dependsOn: [
    storage
  ]
  params: {
    location: location
    keyVaultName: 'ungcap-kv'
  }
}

// Monitoring
module monitoring 'monitoring.bicep' = {
  name: 'monitoringDeployment'
  dependsOn: [
    keyvault
  ]
  params: {
    location: location
	  workspaceName: 'ungcap-logws'
  }
}

// Databricks
module databricks 'databricks.bicep' = {
  name: 'databricksDeployment'
  dependsOn: [
    monitoring
  ]
  params: {
    workspaceName: 'ungcap-dbws'
    managedResourceGroupName: 'rg-unguidedcapstone-managed'
    skuName: 'standard'
    location: location
  }
}

// Function App
module functionapp 'functionapp.bicep' = {
  name: 'functionAppDeployment'
  dependsOn: [
    databricks
  ]
  params: {
    location: location
    appServicePlanName: 'ungcap-func-plan'
  }
}
