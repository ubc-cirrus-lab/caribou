from multi_x_serverless.deployment.client.enums import Endpoint
from multi_x_serverless.deployment.client.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.deployment.client.remote_client.remote_client import RemoteClient


class RemoteClientFactory:
    def get_remote_client(self, endpoint: str, region: str) -> RemoteClient:
        endpoint_enum = Endpoint(endpoint)
        if endpoint_enum == Endpoint.AWS:
            return AWSRemoteClient(region)
        if endpoint_enum == Endpoint.GCP:
            raise NotImplementedError()
        raise RuntimeError(f"Unknown endpoint {endpoint}")
