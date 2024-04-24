from caribou.deployment.client import CaribouWorkflow
from typing import Any


def fifth(workflow: CaribouWorkflow, event: dict[str, Any], seventh_function: Any) -> dict[str, Any]:
    print("Hello from fifth function")
    responses: list[dict[str, Any]] = workflow.get_predecessor_data()

    for response in responses:
        print(response["hello"])

    workflow.invoke_serverless_function(seventh_function, {"hello": "Fifth Function says hello to seventh function"})

    print("Goodbye from fifth function")
    return {"status": 200}
