import os

from caribou.common.constants import GLOBAL_SYSTEM_REGION, INTEGRATION_TEST_SYSTEM_REGION
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.models.remote_client.remote_client_factory import RemoteClientFactory
from caribou.common.provider import Provider
from caribou.common.utils import str_to_bool


class Endpoints:  # pylint: disable=too-many-instance-attributes
    def __init__(self) -> None:
        provider = Provider.AWS.value
        global_system_region = GLOBAL_SYSTEM_REGION

        integration_test_on = str_to_bool(os.environ.get("INTEGRATIONTEST_ON", "False"))

        if integration_test_on:
            provider = Provider.INTEGRATION_TEST_PROVIDER.value
            global_system_region = INTEGRATION_TEST_SYSTEM_REGION

        # TODO (#56): Implement retrieval of deployer server and update checker regions
        self._deployment_server_region = global_system_region
        self._deployment_resources_client = RemoteClientFactory.get_remote_client(
            provider, self._deployment_server_region
        )

        self._deployment_optimizaion_monitor_region = global_system_region
        self._deployment_manager_client = RemoteClientFactory.get_remote_client(
            provider, self._deployment_optimizaion_monitor_region
        )

        self._deployment_algorithm_workflow_placement_decision_region = global_system_region
        self._deployment_algorithm_workflow_placement_decision_client = RemoteClientFactory.get_remote_client(
            provider, self._deployment_algorithm_workflow_placement_decision_region
        )

        self._data_collector_region = global_system_region
        self._data_collector_client = RemoteClientFactory.get_remote_client(provider, self._data_collector_region)

        self._data_store_region = global_system_region
        self._data_store_client = RemoteClientFactory.get_remote_client(provider, self._data_store_region)

        self._framework_cli_remote_client = RemoteClientFactory.get_framework_cli_remote_client(global_system_region)

    def get_deployment_resources_client(self) -> RemoteClient:
        return self._deployment_resources_client

    def get_deployment_manager_client(self) -> RemoteClient:
        return self._deployment_manager_client

    def get_deployment_algorithm_workflow_placement_decision_client(self) -> RemoteClient:
        return self._deployment_algorithm_workflow_placement_decision_client

    def get_data_collector_client(self) -> RemoteClient:
        return self._data_collector_client

    def get_datastore_client(self) -> RemoteClient:
        return self._data_store_client

    def get_framework_cli_remote_client(self) -> AWSRemoteClient:
        return self._framework_cli_remote_client
