from typing import Any

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow


def fifth(workflow: MultiXServerlessWorkflow, event: dict[str, Any], seventh_function: Any) -> dict[str, Any]:
    print("Hello from fifth function")

    print(event)

    responses: list[dict[str, Any]] = workflow.get_predecessor_data()

    for response in responses:
        print(response["hello"])

    workflow.invoke_serverless_function(seventh_function, {"hello": "Fifth Function says hello to seventh function"})

    print("Goodbye from fifth function")
    return {"status": 200}
