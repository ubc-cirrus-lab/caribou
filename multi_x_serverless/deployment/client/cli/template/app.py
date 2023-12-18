from typing import Any

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow("{{ workflow_name }}")


@workflow.serverless_function(name="First-Function", entry_point=True, timeout=60, memory=128)
def first_function(event, context):
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
def second_function(event, context):
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world4",
    }

    workflow.invoke_serverless_function(fifth_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Third-Function")
def third_function(event, context):
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world5",
    }

    workflow.invoke_serverless_function(fourth_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Fourth-Function")
def fourth_function(event, context):
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world6",
    }

    workflow.invoke_serverless_function(fifth_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Fifth-Function")
def fifth_function(event, context):
    responses = workflow.get_predecessor_data()

    for response in responses:
        print(response["hello"])

    return {"status": 200}


@workflow.serverless_function(name="Sixth-Function")
def sixth_function(event, context):
    request = event["hello"]

    print(request)

    payload = {
        "hello": "world7",
    }

    workflow.invoke_serverless_function(seventh_function, payload)

    return {"status": 200}


@workflow.serverless_function(name="Seventh-Function")
def seventh_function(event, context):
    responses: list[dict[str, Any]] = workflow.get_predecessor_data()

    for response in responses:
        print(response["hello"])

    return {"status": 200}
