import logging

import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:

    name = req.params.get('name')

    logging.info(
        'Request received for applying retention policy to folder {name}')

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
