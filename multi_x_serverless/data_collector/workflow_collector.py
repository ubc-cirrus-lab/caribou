from typing import Any

from multi_x_serverless.data_collector.components.workflow.workflow_exporter import WorkflowExporter
from multi_x_serverless.data_collector.components.workflow.workflow_retriever import WorkflowRetriever
from multi_x_serverless.data_collector.data_collector import DataCollector


class WorkflowCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()

        # Perhaps this tables should be imports of the constants.py once we confirm the names
        available_region_table: str = "available_region_table"
        at_region_table: str = "workflow_collector_at_region_table"
        from_to_region_table: str = "workflow_collector_from_to_region_table"

        at_instance_table: str = "workflow_collector_at_instance_table"
        from_to_instance_table: str = "workflow_collector_from_to_instance_table"

        self._data_retriever = WorkflowRetriever()
        self._data_exporter: WorkflowExporter = WorkflowExporter(
            self._data_collector_client,
            available_region_table,
            at_region_table,
            from_to_region_table,
            at_instance_table,
            from_to_instance_table,
        )

    def run(self) -> None:
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
