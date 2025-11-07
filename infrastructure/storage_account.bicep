param storageAccountName string
param location string = resourceGroup().location
param skuName string = 'Standard_LRS'

resource sa 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageAccountName
  location: location
  sku: {
    name: skuName
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    supportsHttpsTrafficOnly: true
  }
  tags: {
    Project: 'UnguidedCapstone'
    Step: '7'
    Environment: 'Dev'
  }
}

output storageAccountId string = sa.id
