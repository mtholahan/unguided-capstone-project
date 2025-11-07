@description('The name of the Databricks workspace to create.')
param workspaceName string

@description('The name of the managed resource group for the Databricks workspace.')
param managedResourceGroupName string

@description('The SKU of the Databricks workspace (standard, premium, or trial).')
param skuName string

@description('The Azure region where the workspace will be deployed.')
param location string = resourceGroup().location

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: workspaceName
  location: location
  sku: {
    name: skuName
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
  }
}

output workspaceName string = databricksWorkspace.name
output workspaceId string = databricksWorkspace.id
output managedResourceGroupId string = databricksWorkspace.properties.managedResourceGroupId
