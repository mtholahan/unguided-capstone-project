param keyVaultName string
param location string = resourceGroup().location
param tenantId string = subscription().tenantId

resource kv 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    tenantId: tenantId
    sku: {
      family: 'A'
      name: 'standard'
    }
    accessPolicies: []
    enabledForDeployment: true
  }
}

output keyVaultUri string = kv.properties.vaultUri
