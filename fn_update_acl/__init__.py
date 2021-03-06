import azure.functions as func
import logging
from datetime import timedelta, date
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


def get_credential():
    """
    Gets Azure AD auth credentials.
    """
    return DefaultAzureCredential()


def get_adls_gen2_service_client(credential, storage_account_name):
    return DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net",
        credential=credential)


def update_acl(service_client, container_name, folder_path, aad_group_object_id):
    try:
        file_system_client = service_client.get_file_system_client(
            file_system=container_name)

        directory_client = file_system_client.get_directory_client(
            folder_path)

        directory_client.update_access_control_recursive(
            acl=f"default:group:{aad_group_object_id}:rwx")

        acl_props = directory_client.get_access_control()
        logging.info(
            f"""Permissions set for {aad_group_object_id} recursively at 
                            { folder_path}: { acl_props['permissions']}""")
        return True
    except Exception as e:
        logging.exception(e)
        return False


def main(req: func.HttpRequest) -> func.HttpResponse:

    folder_path = req.params.get('folderPath')
    aad_object_id = req.params.get('aadObjectId')
    storage_account_name = "starteradls3"
    container_name = "cdp"

    if not (folder_path or aad_object_id):
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            folder_path = req_body.get('folderPath')
            aad_object_id = req_body.get("aadObjectId")

    if not (folder_path and aad_object_id):
        if not folder_path:
            logging.info("Folder path missing")
        if not aad_object_id:
            logging.info("AAD Object Id is missing")
        return func.HttpResponse("One or more of required parameters are missing", status_code=400)

    logging.info(f"Getting credentials")
    credential = get_credential()

    logging.info(f"Creating ADLS Gen2 service client")
    service_client = get_adls_gen2_service_client(
        credential, storage_account_name)

    logging.info(
        f'Adding {aad_object_id} to ACL of {folder_path}')
    status = update_acl(service_client, container_name,
                        folder_path, aad_object_id)
    if status is True:
        return func.HttpResponse(status_code=200)
    else:
        return func.HttpResponse("ACL update failed", status_code=500)
