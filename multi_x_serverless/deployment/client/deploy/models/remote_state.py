from multi_x_serverless.deployment.client.deploy.models.resource import Resource
from multi_x_serverless.deployment.client.factories.remote_client_factory import RemoteClientFactory


class RemoteState:
    def __init__(self, endpoint: str, region: str) -> None:
        self._client = RemoteClientFactory().get_remote_client(endpoint, region)

    def resource_exists(self, resource: Resource) -> bool:
        return self._client.resource_exists(resource)
