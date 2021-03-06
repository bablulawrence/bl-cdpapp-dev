import azure.functions as func
import logging
import os
import requests
from datetime import timedelta, date, datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


def get_credential():
    """
    Gets Azure AD auth credential.
    """
    return DefaultAzureCredential()


def get_adls_gen2_service_client(credential, storage_account_name):
    return DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential)


def apply_retention_policy(service_client, container_name, folder_path, retention_days):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=container_name)
        paths = file_system_client.get_paths(folder_path)
        status = True
        for p in paths:
            if p.is_directory:
                status = status and delete_directory(
                    file_system_client, p, retention_days)
        return status
    except Exception as e:
        logging.exception(e)
        return False


def delete_directory(file_system_client, file_path, retention_days):
    try:
        directory_client = file_system_client.get_directory_client(
            file_path.name)
        props = directory_client.get_directory_properties()
        try:
            es_date = props.metadata['engagementstartdate']
            try:
                if (datetime.now() - datetime.strptime(es_date, '%Y-%m-%d')) >= timedelta(days=retention_days):
                    directory_client.delete_directory()
            except Exception as e:
                logging.exception(e)
                return False
        except AttributeError:
            pass
        return True
    except Exception as e:
        logging.exception(e)
        return False


def main(req: func.HttpRequest) -> func.HttpResponse:

    storage_account_name = os.environ["storageAccountName"]
    container_name = os.environ["containerName"]
    folder_path = req.params.get('folderPath')
    retention_days = req.params.get('retainForDays')

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

    logging.info(f"Getting credential")
    credential = get_credential()

    logging.info(f"Creating ADLS Gen2 service client")

    service_client = get_adls_gen2_service_client(
        credential, storage_account_name)

    logging.info(
        f'Applying retention policy recursively to folder {folder_path}')

    status = apply_retention_policy(service_client, container_name,
                                    folder_path, retention_days)
    if status is True:
        return func.HttpResponse(status_code=200)
    else:
        return func.HttpResponse("Retention policy failed", status_code=500)
    # return func.HttpResponse("Retention policy applied successfully", status_code=200)
