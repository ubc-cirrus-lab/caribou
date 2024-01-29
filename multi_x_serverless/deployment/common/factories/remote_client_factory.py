from multi_x_serverless.common.provider import Provider
from multi_x_serverless.deployment.common.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class RemoteClientFactory:
    @staticmethod
    def get_remote_client(provider: str, region: str) -> RemoteClient:
        try:
            provider_enum = Provider(provider)
        except ValueError as e:
            raise RuntimeError(f"Unknown provider {provider}") from e
        if provider_enum == Provider.AWS:
            return AWSRemoteClient(region)
        if provider_enum == Provider.GCP:
            raise NotImplementedError()
        raise RuntimeError(f"Unknown provider {provider}")
