param databricksName string
param location string = resourceGroup().location
param skuName string = 'standard'

resource dbw 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksName
  location: location
  sku: {
    name: skuName
  }
  properties: {
    managedResourceGroupId: resourceGroup().id
  }
}

output databricksId string = dbw.id
