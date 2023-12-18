from boto3.session import Session

from typing import Any
import time

from multi_x_serverless.deployment.client.config import Config


class AWSClient(object):
    LAMBDA_CREATE_ATTEMPTS = 30
    DELAY_TIME = 5

    def __init__(self, region: str) -> None:
        self._session = Session(region_name=region)
        self._client_cache: dict[str, Any] = {}

    def _client(self, service_name: str) -> Any:
        if service_name not in self._client_cache:
            self._client_cache[service_name] = self._session.client(service_name)
        return self._client_cache[service_name]

    def create_function(
        self,
        function_name: str,
        role_arn: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Runtime": runtime,
            "Code": {"ZipFile": zip_contents},
            "Handler": handler,
            "Role": role_arn,
            "Environment": {"Variables": environment_variables},
            "Timeout": timeout,
            "MemorySize": memory_size,
        }
        arn, state = self._create_lambda_function(kwargs)

        if state != "Active":
            self._wait_for_function_to_become_active(function_name)
        return arn

    def _create_lambda_function(self, kwargs: dict[str, Any]) -> tuple[str, str]:
        client = self._client("lambda")
        response = client.create_function(**kwargs)
        return response["FunctionArn"], response["State"]

    def _wait_for_function_to_become_active(self, function_name: str) -> None:
        client = self._client("lambda")
        for _ in range(self.LAMBDA_CREATE_ATTEMPTS):
            response = client.get_function(FunctionName=function_name)
            state = response["Configuration"]["State"]
            if state == "Active":
                return
            time.sleep(self.DELAY_TIME)
        raise RuntimeError("Lambda function %s did not become active" % function_name)
