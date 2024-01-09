from multi_x_serverless.deployment.client.enums import Provider
from multi_x_serverless.deployment.client.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.deployment.client.remote_client.remote_client import RemoteClient


class RemoteClientFactory:
    def get_remote_client(self, provider: str, region: str) -> RemoteClient:
        provider_enum = Provider(provider)
        if provider_enum == Provider.AWS:
            return AWSRemoteClient(region)
        if provider_enum == Provider.GCP:
            raise NotImplementedError()
        raise RuntimeError(f"Unknown provider {provider}")
