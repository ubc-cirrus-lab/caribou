import botocore.exceptions

from multi_x_serverless.deployment.client.clients import AWSClient, Client
from multi_x_serverless.deployment.client.deploy.models.resource import Resource
from multi_x_serverless.deployment.client.enums import Endpoint


class RemoteState:
    def __init__(self, endpoint: Endpoint, region: str) -> None:
        self._endpoint = endpoint
        self._client = self.initialise_client(endpoint, region)

    @staticmethod
    def initialise_client(endpoint: Endpoint, region: str) -> Client:
        if endpoint == Endpoint.AWS:
            return AWSClient(region)
        if endpoint == Endpoint.GCP:
            raise NotImplementedError()
        raise RuntimeError(f"Unknown endpoint {endpoint}")

    def resource_exists(self, resource: Resource) -> bool:
        if self._endpoint == Endpoint.AWS:
            return self.resource_exists_aws(resource)
        if self._endpoint == Endpoint.GCP:
            return self.resource_exists_gcp(resource)
        raise RuntimeError(f"Unknown endpoint {self._endpoint}")

    def resource_exists_aws(self, resource: Resource) -> bool:
        if resource.resource_type == "iam_role":
            return self.aws_iam_role_exists(resource)
        if resource.resource_type == "function":
            return self.aws_lambda_function_exists(resource)
        raise RuntimeError(f"Unknown resource type {resource.resource_type}")

    def aws_iam_role_exists(self, resource: Resource) -> bool:
        try:
            role = self._client.get_iam_role(resource.name)
        except botocore.exceptions.ClientError:
            return False
        return role is not None

    def aws_lambda_function_exists(self, resource: Resource) -> bool:
        try:
            function = self._client.get_lambda_function(resource.name)
        except botocore.exceptions.ClientError:
            return False
        return function is not None

    def resource_exists_gcp(self, resource: Resource) -> bool:
        return False
