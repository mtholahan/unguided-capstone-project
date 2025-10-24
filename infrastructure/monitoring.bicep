param workspaceName string
param location string = resourceGroup().location

resource logws 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: workspaceName
  location: location
  properties: {
    retentionInDays: 30
  }
  tags: {
    Project: 'UnguidedCapstone'
    Step: '7'
    Environment: 'Dev'
  }
}

output workspaceId string = logws.id