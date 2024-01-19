from typing import Any

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

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
def first_function(event: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "hello": "world",
    }

    workflow.invoke_serverless_function(second_function, payload)

    payload = {
        "hello": "world2",
    }

    workflow.invoke_serverless_function(third_function, payload)

    payload = {
        "hello": "world3",
    }

    workflow.invoke_serverless_function(sixth_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Second-Function")
def second_function(event: dict[str, Any]) -> dict[str, Any]:
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world4",
    }

    workflow.invoke_serverless_function(fifth_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Third-Function")
def third_function(event: dict[str, Any]) -> dict[str, Any]:
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world5",
    }

    workflow.invoke_serverless_function(fourth_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Fourth-Function")
def fourth_function(event: dict[str, Any]) -> dict[str, Any]:
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world6",
    }

    workflow.invoke_serverless_function(fifth_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Fifth-Function")
def fifth_function(event: dict[str, Any]) -> dict[str, Any]:
    responses: list[dict[str, Any]] = workflow.get_predecessor_data()

    for response in responses:
        print(response["hello"])

    return {"status": 200}


@workflow.serverless_function(name="Sixth-Function")
def sixth_function(event: dict[str, Any]) -> dict[str, Any]:
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world7",
    }

    workflow.invoke_serverless_function(seventh_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Seventh-Function")
def seventh_function(event: dict[str, Any]) -> dict[str, Any]:
    responses: list[dict[str, Any]] = workflow.get_predecessor_data()

    for response in responses:
        print(response["hello"])

    return {"status": 200}
