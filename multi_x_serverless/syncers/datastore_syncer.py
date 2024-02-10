from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.common.constants import (
    DEPLOYMENT_MANAGER_RESOURCE_TABLE,
)


class DatastoreSyncer:
    def __init__(self):
        self.endpoints = Endpoints()

    def sync(self):
        currently_deployed_workflows = self.endpoints.get_deployment_manager_client().get_keys(
            DEPLOYMENT_MANAGER_RESOURCE_TABLE
        )

        for workflow in currently_deployed_workflows:
            # Get deployed regions

            # For each deployed region, get the cloudsource logs

            # Summarize the logs since the last sync

            # Upload the summarized logs to the datastore

            pass
