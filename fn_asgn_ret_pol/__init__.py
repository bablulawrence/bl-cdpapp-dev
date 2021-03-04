from azure.identity import DefaultAzureCredential
#from msrestazure.azure_active_directory import MSIAuthentication
import azure.functions as func
import logging


def get_credentials():
    """
    Gets Azure AD auth credentials.
    """
    return DefaultAzureCredential()
#    return MSIAuthentication()


def main(req: func.HttpRequest) -> func.HttpResponse:

    name = req.params.get('name')

    logging.info(
        f'Request received for applying retention policy to folder {name}')

    credentials = get_credentials()

    logging.info(f"Credentials {credentials}")

    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Retention policy is assigned to folder {name}")
    else:
        return func.HttpResponse(
            "Please pass folder name to assign retention policy",
            status_code=200
        )
