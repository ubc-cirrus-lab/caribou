from typing import Any

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow
from src.fifth import fifth

workflow = MultiXServerlessWorkflow(name="{{ workflow_name }}", version="0.0.1")


@workflow.serverless_function(
    name="First-Function",
    entry_point=True,
    regions_and_providers={
        "allowed_regions": [
            {
                "provider": "aws",
                "region": "us-east-1",
            }
        ],
        "disallowed_regions": [
            {
                "provider": "aws",
                "region": "us-east-2",
            }
        ],
        "providers": {
            "aws": {
                "config": {
                    "timeout": 60,
                    "memory": 128,
                },
            },
        },
    },
    environment_variables=[{"key": "example_key", "value": "example_value"}],
)
def first_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    print("Hello from first function")

    payload = "First Function says hello to second function"

    workflow.invoke_serverless_function(second_function, payload)

    payload = "First Function says hello to third function"

    workflow.invoke_serverless_function(third_function, payload)

    payload = "First Function says hello to sixth function"

    workflow.invoke_serverless_function(sixth_function, payload)

    print("Goodbye from first function")
    return {"status": 200}


@workflow.serverless_function(name="Second-Function")
def second_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    print("Hello from second function")

    print(event)

    payload = {
        "hello": "Second Function says hello to fifth function",
    }

    workflow.invoke_serverless_function(fifth_function, payload)

    print("Goodbye from second function")
    return {"status": 200}


@workflow.serverless_function(name="Third-Function")
def third_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    print("Hello from third function")

    print(event)

    payload = {
        "hello": "Third Function says hello to fourth function",
    }

    workflow.invoke_serverless_function(fourth_function, payload)

    print("Goodbye from third function")
    return {"status": 200}


@workflow.serverless_function(name="Fourth-Function")
def fourth_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    print("Hello from fourth function")
    request = event["hello"]

    print(request)

    payload = {
        "hello": "Fourth Function says hello to fifth function",
    }

    workflow.invoke_serverless_function(fifth_function, payload)

    print("Goodbye from fourth function")
    return {"status": 200}


@workflow.serverless_function(name="Fifth-Function")
def fifth_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    return fifth(workflow, event, seventh_function)


@workflow.serverless_function(name="Sixth-Function")
def sixth_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    print("Hello from sixth function")

    print(event)

    payload = {
        "hello": "Sixth Function says hello to seventh function",
    }

    workflow.invoke_serverless_function(seventh_function, payload)

    print("Goodbye from sixth function")
    return {"status": 200}


@workflow.serverless_function(name="Seventh-Function")
def seventh_function(event: dict[str, Any], context: Any) -> dict[str, Any]:
    print("Hello from seventh function")
    responses: list[dict[str, Any]] = workflow.get_predecessor_data()

    for response in responses:
        print(response["hello"])

    print("Goodbye from seventh function")
    return {"status": 200}
