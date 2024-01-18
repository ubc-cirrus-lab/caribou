from multi_x_serverless.deployment.common.factories.remote_client_factory import RemoteClientFactory
from multi_x_serverless.deployment.common.provider import Provider
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class Endpoints:
    def __init__(self) -> None:
        # TODO (#56): Implement retrieval of deployer server and update checker regions
        self._deployment_server_region = "us-west-2"
        self._deployment_manager_client = RemoteClientFactory.get_remote_client(
            Provider.AWS.value, self._deployment_server_region
        )

        self._solver_update_checker_region = "us-west-2"
        self._solver_update_checker_client = RemoteClientFactory.get_remote_client(
            Provider.AWS.value, self._solver_update_checker_region
        )

        self._solver_routing_decision_region = "us-west-2"
        self._solver_routing_decision_client = RemoteClientFactory.get_remote_client(
            Provider.AWS.value, self._solver_routing_decision_region
        )

    def get_deployment_manager_client(self) -> RemoteClient:
        return self._deployment_manager_client

    def get_solver_update_checker_client(self) -> RemoteClient:
        return self._solver_update_checker_client

    def get_solver_workflow_placement_decision_client(self) -> RemoteClient:
        return self._solver_routing_decision_client
