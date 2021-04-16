import azure.functions as func
import logging
import os
from datetime import timedelta, date, datetime
from azure.identity import DefaultAzureCredential
from azure.mgmt.logic import LogicManagementClient


def get_credential():
    """
    Gets Azure AD auth credential.
    """
    return DefaultAzureCredential()


def get_logic_management_client(credential, subscription_id):
    return LogicManagementClient(credential=credential,
                                 subscription_id=subscription_id)


def get_logic_app_run(logic_mgmt_client, resource_group_name, workflow_name, workflow_run_id):
    try:
        workflow_run = logic_mgmt_client.workflow_runs.get(
            resource_group_name=resource_group_name, workflow_name=workflow_name, run_name=workflow_run_id)
        return workflow_run.status
    except Exception as e:
        logging.exception(e)
        return False


def main(req: func.HttpRequest) -> func.HttpResponse:

    subscription_id = os.environ['subscriptionId']
    resource_group_name = os.environ['resourceGroupName']
    workflow_name = os.environ['workflowName']
    workflow_run_id = req.params.get('workflowRunId')

    if not (workflow_run_id):
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            workflow_run_id = req_body.get('workflowRunId')

    if not (workflow_run_id):
        logging.info("Workflow run id is missing")
        return func.HttpResponse("One or more of required parameters are missing", status_code=400)

    logging.info(f"Getting credential")
    credential = get_credential()

    logging.info(f"Creating Logic App Management Client")
    logic_mgmt_client = get_logic_management_client(credential,
                                                    subscription_id)

    logging.info(
        f'Getting Logic App run for : "{workflow_name}" - {workflow_run_id}')
    response = get_logic_app_run(
        logic_mgmt_client, resource_group_name, workflow_name, workflow_run_id)

    if response:
        return func.HttpResponse(body=response, status_code=200)
    else:
        return func.HttpResponse("Getting logic app run status failed", status_code=500)
