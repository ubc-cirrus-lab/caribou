from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_INSTANCE_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.routing.solver.input.components.loader import InputLoader


class WorkflowLoader(InputLoader):
    _workflow_data: dict[str, Any]

    def __init__(self, client: RemoteClient) -> None:
        super().__init__(client, WORKFLOW_INSTANCE_TABLE)

    def setup(self, workflow_id: str) -> None:
        # self._workflow_data = self._retrieve_workflow_data(workflow_id)
        self._workflow_data = {
            "instance_1": {
                "favourite_home_region": "provider_1:region_1",
                "favourite_home_region_average_runtime": 26.0,
                "favourite_home_region_tail_runtime": 31.0,
                "projected_monthly_invocations": 12.5,
                "execution_summary": {
                    "provider_1:region_1": {"average_runtime": 26.0, "tail_runtime": 31.0},
                    "provider_1:region_2": {"average_runtime": 26.0, "tail_runtime": 31.0},
                },
                "invocation_summary": {
                    "probability_of_invocation": 0.8,
                    "average_data_transfer_size": 0.0007,
                    "transmission_summary": {
                        "provider_1:region_1": {
                            "provider_1:region_1": {"average_latency": 0.00125, "tail_latency": 0.00175},
                            "provider_1:region_2": {"average_latency": 0.125, "tail_latency": 0.155},
                        },
                        "provider_1:region_2": {
                            "provider_1:region_1": {"average_latency": 0.095, "tail_latency": 0.125}
                        },
                    },
                },
            },
            "instance_2": {
                "favourite_home_region": "provider_1:region_1",
                "favourite_home_region_average_runtime": 12.5,
                "favourite_home_region_tail_runtime": 12.5,
                "projected_monthly_invocations": 11.25,
                "execution_summary": {
                    "provider_1:region_1": {"average_runtime": 12.5, "tail_runtime": 12.5},
                    "provider_1:region_2": {"average_runtime": 12.5, "tail_runtime": 12.5},
                },
                "invocation_summary": {},
            },
        }

    def get_workflow_data(self) -> dict[str, Any]:
        return self._workflow_data

    def _retrieve_workflow_data(self, workflow_id: str) -> dict[str, Any]:
        return self._retrive_data(self._primary_table, workflow_id)
