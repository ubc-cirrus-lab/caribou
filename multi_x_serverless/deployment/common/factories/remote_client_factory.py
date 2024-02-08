from multi_x_serverless.common.models.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient
from multi_x_serverless.common.models.remote_client.mock_remote_client import MockRemoteClient
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.provider import Provider


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
        if provider_enum in [Provider.TEST_PROVIDER1, Provider.TEST_PROVIDER2]:
            return MockRemoteClient()
        if provider_enum == Provider.INTEGRATION_TEST_PROVIDER:
            return IntegrationTestRemoteClient()
        raise RuntimeError(f"Unknown provider {provider}")
