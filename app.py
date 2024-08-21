from typing import Any

import json
import logging
from caribou.endpoint.client import Client


# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Set the logging level

def caribou_cli(event: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    print(f"Received event: {event}")
    if event.get("action", None) == "run":
        workflow_id = event.get("workflow_id", None)
        argument = event.get("argument", None)
        print("Request to run workflow")

        if workflow_id:
            client = Client(workflow_id)
            print(f"Running workflow {workflow_id}")
            if argument:
                client.run(argument)
            else:
                client.run()

    return {"status": 200}