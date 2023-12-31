param name string
param location string = resourceGroup().location
param tags object = {}

@description('Specifies whether to deploy Azure Databricks workspace with Secure Cluster Connectivity (No Public IP) enabled or not')
param disablePublicIp bool = false

@description('The pricing tier of workspace.')
param sku object = {
  name: 'trial'
}
param managedResourceGroupName string = ''
var connectorName = '${name}-connector'



resource databricks 'Microsoft.Databricks/workspaces@2018-04-01' = {
  name: name
  location: location
  sku: sku
  tags: tags
  properties: {
    managedResourceGroupId: managedResourceGroup.id
    parameters: {
      enableNoPublicIp: {
        value: disablePublicIp
      }
    }
  }
}

resource managedResourceGroup 'Microsoft.Resources/resourceGroups@2021-04-01' existing = {
  scope: subscription()
  name: managedResourceGroupName
}




// resource connector 'Microsoft.Databricks/accessConnectors@2022-04-01-preview' = {
//   name: connectorName
//   location: location
//   tags: tags
//   identity: {
//     type: 'SystemAssigned'
//   }
// }

// REMOVED AS THIS IS NOT CURRENTLY WORKING, SEE https://github.com/Azure/bicep/issues/11035
// resource existingIdentity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' existing = {
//   name: 'dbmanagedidentity'
// }

output name string = databricks.name
output workspaceurl string = databricks.properties.workspaceUrl
output workspaceid string = databricks.properties.workspaceId
output managedresourcegroup string = managedResourceGroup.name
// output connectoridentity string = connector.identity.principalId
