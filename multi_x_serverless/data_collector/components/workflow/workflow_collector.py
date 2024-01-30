from typing import Any

from multi_x_serverless.common.constants import (
    WORKFLOW_AT_INSTANCE_TABLE,
    WORKFLOW_AT_REGION_TABLE,
    WORKFLOW_FROM_TO_INSTANCE_TABLE,
    WORKFLOW_FROM_TO_REGION_TABLE,
)
from multi_x_serverless.data_collector.components.data_collector import DataCollector
from multi_x_serverless.data_collector.components.workflow.workflow_exporter import WorkflowExporter
from multi_x_serverless.data_collector.components.workflow.workflow_retriever import WorkflowRetriever


class WorkflowCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "workflow_collector"

        at_region_table: str = WORKFLOW_AT_REGION_TABLE
        from_to_region_table: str = WORKFLOW_FROM_TO_REGION_TABLE

        at_instance_table: str = WORKFLOW_AT_INSTANCE_TABLE
        from_to_instance_table: str = WORKFLOW_FROM_TO_INSTANCE_TABLE

        self._data_retriever: WorkflowRetriever = WorkflowRetriever(self._data_collector_client)
        self._data_exporter: WorkflowExporter = WorkflowExporter(
            self._data_collector_client,
            at_region_table,
            from_to_region_table,
            at_instance_table,
            from_to_instance_table,
        )

    def run(self) -> None:
        # Retrieve available regions
        available_workflow_data = self._data_retriever.retrieve_available_workflows()
        # TODO (#100): Fill Data Collector Implementations

        # Do required application logic using data from retriever
        # Process said data, then return the final data into the exporters
        at_workflow_region_data: dict[str, Any] = {}
        from_to_workflow_region_data: dict[str, Any] = {}

        # For instance level data
        at_workflow_instance_data: dict[str, Any] = {}
        from_to_workflow_instance_data: dict[str, Any] = {}

        self._data_exporter.export_all_data(
            at_workflow_region_data,
            from_to_workflow_region_data,
            at_workflow_instance_data,
            from_to_workflow_instance_data,
        )

        # For workflow collector, no need to modify the time stamp of the regions