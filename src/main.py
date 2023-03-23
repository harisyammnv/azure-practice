from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import os

def get_credentials():
    # Get the credentials from the environment variables
    credentials = ClientSecretCredential(
        client_id = os.environ['AZURE_CLIENT_ID'],
        client_secret = os.environ['AZURE_CLIENT_SECRET'],
        tenant_id = os.environ['AZURE_TENANT_ID']
    )
    return credentials

def create_resource_client(credentials):
    # Create the resource client
    resource_client = ResourceManagementClient(credential=credentials, 
                                               subscription_id=os.environ['AZURE_SUBSCRIPTION_ID'])
    return resource_client

def create_datafactory_client(credentials):
    # Create the datafactory client
    datafactory_client = DataFactoryManagementClient(credential=credentials, 
                                                     subscription_id=os.environ['AZURE_SUBSCRIPTION_ID'])
    return datafactory_client

def main():
    
    # azure subscription id
    subscription_id = os.environ['AZURE_SUBSCRIPTION_ID']
    rg_name = 'azbicep-dev-west-europe-rg'
    
    df_name = 'azbicep-dev-initial-df'
    
    # get credentials
    credentials = get_credentials()
    