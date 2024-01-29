from multi_x_serverless.deployment.common.factories.remote_client_factory import RemoteClientFactory
from multi_x_serverless.common.provider import Provider
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.constants import GLOBAL_SYSTEM_REGION


class Endpoints:
    def __init__(self) -> None:
        # TODO (#56): Implement retrieval of deployer server and update checker regions
        self._deployment_server_region = GLOBAL_SYSTEM_REGION
        self._deployment_manager_client = RemoteClientFactory.get_remote_client(
            Provider.AWS.value, self._deployment_server_region
        )

        self._solver_update_checker_region = GLOBAL_SYSTEM_REGION
        self._solver_update_checker_client = RemoteClientFactory.get_remote_client(
            Provider.AWS.value, self._solver_update_checker_region
        )

        self._solver_workflow_placement_decision_region = GLOBAL_SYSTEM_REGION
        self._solver_workflow_placement_decision_client = RemoteClientFactory.get_remote_client(
            Provider.AWS.value, self._solver_workflow_placement_decision_region
        )

        self._data_collector_region = GLOBAL_SYSTEM_REGION
        self._data_collector_client = RemoteClientFactory.get_remote_client(
            Provider.AWS.value, self._data_collector_region
        )

    def get_deployment_manager_client(self) -> RemoteClient:
        return self._deployment_manager_client

    def get_solver_update_checker_client(self) -> RemoteClient:
        return self._solver_update_checker_client

    def get_solver_workflow_placement_decision_client(self) -> RemoteClient:
        return self._solver_workflow_placement_decision_client

    def get_data_collector_client(self) -> RemoteClient:
        return self._data_collector_client
