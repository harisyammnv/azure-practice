from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from datetime import datetime, timedelta
import time
import os


def print_item(group):
    """Print an Azure object instance."""
    print("\tName: {}".format(group.name))
    print("\tId: {}".format(group.id))
    if hasattr(group, "location"):
        print("\tLocation: {}".format(group.location))
    if hasattr(group, "tags"):
        print("\tTags: {}".format(group.tags))
    if hasattr(group, "properties"):
        print_properties(group.properties)
    print("\n")


def print_properties(props):
    """print a resource group instance properties."""
    if props and hasattr(props, "provisioning_state"):
        print("\tProperties:")
        print("\t\tProvisioning State: {}".format(props.provisioning_state))
    print("\n")


def print_activity_run_details(activity_run):
    """print activity run details."""
    print("\tActivity run details:")
    print("\t\tActivity run status: {}".format(activity_run.status))
    if activity_run.status == "Succeeded":
        print("\t\tActivity run output: {}".format(activity_run.output))


def get_credentials():
    # Get the credentials from the environment variables
    credentials = ClientSecretCredential(
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
        tenant_id=os.environ["AZURE_TENANT_ID"],
    )
    return credentials


def create_resource_client(credentials, subscription_id):
    # Create the resource client
    resource_client = ResourceManagementClient(
        credential=credentials, subscription_id=subscription_id
    )
    return resource_client


def create_datafactory_client(credentials, subscription_id):
    # Create the datafactory client
    datafactory_client = DataFactoryManagementClient(
        credential=credentials, subscription_id=subscription_id
    )
    return datafactory_client


def main():
    # azure subscription id
    subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]
    rg_name = "azbicep-dev-west-europe-rg"

    df_name = "azbicep-dev-initial-df"

    # get credentials
    credentials = get_credentials()
    resource_client = create_resource_client(credentials, subscription_id)
    adf_client = create_datafactory_client(credentials, subscription_id)

    rg_params = {"location": "westeurope"}
    df_params = {"location": "westeurope"}

    # create datafactory
    df_resource = Factory(location=df_params["location"])
    df = adf_client.factories.create_or_update(rg_name, df_name, df_resource)

    print_item(df)

    while df.provisioning_state != "Succeeded":
        time.sleep(1)
        df = adf_client.factories.get(rg_name, df_name)

    # Create an Azure Storage linked service
    ls_name = "storageLinkedService001"

    # IMPORTANT: specify the name and key of your Azure Storage account.
    storage_string = SecureString(
        value="DefaultEndpointsProtocol=https;AccountName=azbicepdevstorage;AccountKey=uY6IIR9nlti9MDTBTt16LGMcwohCff1ZWZm8ZYu1hCWHabg46UjqSE4o06/rirnZZ/uZch8ouXt9+AStWsLKCQ==;EndpointSuffix=core.windows.net"
    )

    ls_azure_storage = LinkedServiceResource(
        properties=AzureStorageLinkedService(connection_string=storage_string)
    )

    ls = adf_client.linked_services.create_or_update(
        rg_name, df_name, ls_name, ls_azure_storage
    )
    print_item(ls)

    # create a dataset

    # create a dataset for source azure blob storage
    ds_name = "ds_in"

    ds_ls = LinkedServiceReference(
        type="LinkedServiceReference", reference_name=ls_name
    )

    blob_path = "adfv2tutorial/input"
    blob_filename = "raw.txt"

    ds_azure_blob = DatasetResource(
        properties=AzureBlobDataset(
            linked_service_name=ds_ls, folder_path=blob_path, file_name=blob_filename
        )
    )
    ds = adf_client.datasets.create_or_update(rg_name, df_name, ds_name, ds_azure_blob)

    print_item(ds)

    # create a dataset for sink azure blob storage

    ds_out_name = "ds_out"
    output_blob_path = "adfv2tutorial/output"
    ds_out_az_blob = DatasetResource(
        properties=AzureBlobDataset(
            linked_service_name=ds_ls, folder_path=output_blob_path
        )
    )

    ds_out = adf_client.datasets.create_or_update(
        rg_name, df_name, ds_out_name, ds_out_az_blob
    )
    print_item(ds_out)

    # create a copy activity

    act_name = "copyBlobtoBlob"

    blob_source = BlobSource()
    blob_sink = BlobSink()

    dsin_ref = DatasetReference(type="DatasetReference", reference_name=ds_name)
    ds_out_red = DatasetReference(type="DatasetReference", reference_name=ds_out_name)

    copy_activity = CopyActivity(
        name=act_name,
        inputs=[dsin_ref],
        outputs=[ds_out_red],
        source=blob_source,
        sink=blob_sink,
    )

    # create a pipeline with copy activity

    p_name = "copyPipeline"

    parameters_for_pipeline = {}

    p_obj = PipelineResource(
        activities=[copy_activity], parameters=parameters_for_pipeline
    )
    p = adf_client.pipelines.create_or_update(rg_name, df_name, p_name, p_obj)
    print_item(p)

    # create a pipeline run
    run_response = adf_client.pipelines.create_run(
        rg_name, df_name, p_name, parameters={}
    )

    # monitor the pipeline run
    pipeline_run = adf_client.pipeline_runs.get(rg_name, df_name, run_response.run_id)

    print("\n\tPipeline run status: {}".format(pipeline_run.status))
    filter_params = RunFilterParameters(
        last_updated_after=datetime.now() - timedelta(1),
        last_updated_before=datetime.now() + timedelta(1),
    )
    query_response = adf_client.activity_runs.query_by_pipeline_run(
        rg_name, df_name, pipeline_run.run_id, filter_params
    )
    print_activity_run_details(query_response.value)


main()
