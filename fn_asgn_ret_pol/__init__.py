import azure.functions as func
import logging
import json
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
#from msrestazure.azure_active_directory import MSIAuthentication


def get_credential():
    """
    Gets Azure AD auth credentials.
    """
    return DefaultAzureCredential()
#    return MSIAuthentication()


def get_adls_gen2_service_client(credential, storage_account_name):
    return DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential)


def list_directory_contents(service_client, container, path):
    try:

        file_system_client = service_client.get_file_system_client(
            file_system=container)
        paths = file_system_client.get_paths(path=path)
        for path in paths:
            logging.info(path.name + '\n')

    except Exception as e:
        logging.info(e)


def main(req: func.HttpRequest) -> func.HttpResponse:

    folder_path = req.params.get('folderPath')
    retention_days = req.params.get('retainForDays')
    storage_account_name = "starteradls3"

    if not (folder_path or retention_days):
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            folder_path = req_body.get('folderPath')
            retention_days = req_body.get("retainForDays")

    if not (folder_path and retention_days):
        if not folder_path:
            logging.info("Folder path missing")
        if not retention_days:
            logging.info("Retention days missing")
        return func.HttpResponse("One or more of required parameters are missing", status_code=400)

    logging.info(f"Getting credentials")
    credential = get_credential()

    logging.info(f"Creating ADLS Gen2 service client")

    service_client = get_adls_gen2_service_client(
        credential, storage_account_name)

    list_directory_contents(service_client, "cdp", "Raw")

    logging.info(
        f'Applying retention of {retention_days} days to folder {folder_path}')

    return func.HttpResponse(
        json.dumps({
            "name": "bablu"
        }),
        mimetype="application/json",
        status_code=200)
    # return func.HttpResponse("Retention policy applied successfully", status_code=200)
