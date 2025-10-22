param functionAppName string
param location string = resourceGroup().location
param storageAccountName string
param skuName string = 'Y1'

resource plan 'Microsoft.Web/serverfarms@2022-09-01' = {
  name: '${functionAppName}-plan'
  location: location
  sku: {
    name: skuName
    tier: 'Dynamic'
  }
}

resource func 'Microsoft.Web/sites@2022-09-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp'
  properties: {
    serverFarmId: plan.id
    siteConfig: {
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName}'
        }
      ]
    }
  }

}

output functionAppUrl string = func.properties.defaultHostName
