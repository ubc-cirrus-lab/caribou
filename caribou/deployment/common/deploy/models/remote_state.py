from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.deployment.common.deploy.models.resource import Resource


class RemoteState:
    def __init__(self, provider: str, region: str) -> None:
        self._client = RemoteClientFactory.get_remote_client(provider, region)

    def resource_exists(self, resource: Resource) -> bool:
        return self._client.resource_exists(resource)
