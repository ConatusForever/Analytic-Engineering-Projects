from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.keyvault import KeyVaultManagementClient
from azure.keyvault.secrets import SecretClient
from azure.mgmt.keyvault.models import VaultCreateOrUpdateParameters, Sku, AccessPolicyEntry, Permissions
import snowflakeSecrets, AzureConfigDetails, subprocess

# Authenticate use Azure CLI
creds = AzureCliCredential()
resourceclient = ResourceManagementClient(creds, AzureConfigDetails.subId)
kvMgmtClient = KeyVaultManagementClient(creds, AzureConfigDetails.subId)

# Defining key vault access policies
accessPolicies =[
    AccessPolicyEntry(
        tenant_id = AzureConfigDetails.tenantID,
        object_id = AzureConfigDetails.HLawrenceObjectId,
        permissions = Permissions(
            keys=['get','list','create','delete'],
            secrets =['get','list','set','delete'],
            certificates=['get','list','create', 'delete']
        )
    )
]

# Defining key vault params

kvParams = VaultCreateOrUpdateParameters(
    location = AzureConfigDetails.location,
    properties={
        'sku':Sku(family='A', name='standard'),
        'tenant_id': AzureConfigDetails.tenantID ,
        'access_policies':[]
    }
)


# create or update the key vault
keyVaultName = 'analyticsEngKeyVault'
kvCreation = kvMgmtClient.vaults.begin_create_or_update(
    AzureConfigDetails.resourceGroupName,
    keyVaultName,
    kvParams
)

snowflakeKeyVaultURL = f'https://{keyVaultName}.vault.azure.net'

# creating a secretClient using DefaultAzureCredential

client = SecretClient(vault_url=snowflakeKeyVaultURL, credential=creds)

# defining secrets

secrets = {
    'snowflake-account': snowflakeSecrets.AccountID,
    'snowflake-user': snowflakeSecrets.snowUsername,
    'snowflake-password': snowflakeSecrets.Password
}

# Function to set a secret

def setSecret(secretName, secretValue):
    try:
        client.set_secret(secretName, secretValue)
        print(f'Secret {secretName} set successfully.')
    except Exception as e:
        print(f'Failed to set sercret {secretName}: {e}')

for secretName, secretValue in secrets.items():
    setSecret(secretName, secretValue)