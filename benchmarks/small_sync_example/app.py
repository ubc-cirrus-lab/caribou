from typing import Any

import json
from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="small_sync_example", version="0.0.1")


@workflow.serverless_function(
    name="initial_function",
    entry_point=True,
)
def initial_function(event: dict[str, Any]) -> dict[str, Any]:
    print("Hello from first function")

    if isinstance(event, str):
        event = json.loads(event)

    execute_1 = True
    if "execute_1" in event:
        execute_1 = event["execute_1"]

    execute_2 = True
    if "execute_2" in event:
        execute_2 = event["execute_2"]

    execute_3 = True
    if "execute_3" in event:
        execute_3 = event["execute_3"]

    payload = "First Function says hello to sync function 1"

    workflow.invoke_serverless_function(sync_function, payload, execute_1)

    payload = "First Function says hello to sync function 2"

    workflow.invoke_serverless_function(sync_function, payload, execute_2)

    payload = "First Function says hello to sync function 3"

    workflow.invoke_serverless_function(sync_function, payload, execute_3)

    print("Goodbye from first function")
    return {"status": 200}


@workflow.serverless_function(name="sync_function")
def sync_function(event: dict[str, Any]) -> dict[str, Any]:
    print("Hello from sync function")
    responses: list[dict[str, Any]] = workflow.get_predecessor_data()

    for response in responses:
        print(response)

    print("Goodbye from sync function")
    return {"status": 200}
