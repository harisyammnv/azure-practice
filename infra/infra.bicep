targetScope = 'subscription'

param location string = 'westeurope'

resource azbiceprg 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: 'azbicep-dev-west-europe-rg'
  location: location
}


