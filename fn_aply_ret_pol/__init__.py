import azure.functions as func
import logging
import os
import requests
from datetime import timedelta, date, datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient
#from msrestazure.azure_active_directory import MSIAuthentication


def get_credential():
    """
    Gets Azure AD auth credential.
    """
    return DefaultAzureCredential()
#    return MSIAuthentication()


def get_adls_gen2_service_client(credential, storage_account_name):
    return DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential)


def apply_retention_policy(credential, storage_account_name,
                           service_client, container_name, folder_path, retention_days):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=container_name)
        paths = file_system_client.get_paths(folder_path)
        status = True
        for p in paths:
            if not p.is_directory:
                status = status and set_expiry(
                    credential, storage_account_name, container_name, p, retention_days)
        return status
    except Exception as e:
        logging.exception(e)
        return False

def set_expiry(credential, storage_account_name, container_name, file_path, retention_days):
    try:
        url = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/{file_path}?comp=expiry"
        token = credential.get_token(".default")
        date = convert_datetime_to_rfc1123(datetime.now())
        headers = {
            'x-ms-version': '2020-02-10',
            'x-ms-date': date,
            'x-ms-expiry-option': 'RelativeToCreation',
            'x-ms-expiry-time': '30000',
            'Authorization': f'Bearer {token}',
        }
        r = requests.put(url, headers=headers)
        logging.info(r)
        return True
    except Exception as e:
        logging.exception(e)
        return False


# def set_expiry(file_system_client, file_path, retention_days):
#     try:
#         file_client = file_system_client.get_file_client(file_path.name)
#         #logging.info(f"Applying retention policy on file : {file_path.name}")
#         # file_client.set_file_expiry(expiry_options="Never")
#         # file_client.set_file_expiry('RelativeToCreation', 50000)
#         file_client.set_file_expiry('Absolute',
#                                     expires_on=date.today() + timedelta(days=retention_days))
#         logging.info(file_client.get_file_properties())
#         return True
#     except Exception as e:
#         logging.info(e)
#         return False


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
