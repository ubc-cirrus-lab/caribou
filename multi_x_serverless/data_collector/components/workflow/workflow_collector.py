import logging
from typing import Any

from multi_x_serverless.common.constants import WORKFLOW_INSTANCE_TABLE
from multi_x_serverless.data_collector.components.data_collector import DataCollector
from multi_x_serverless.data_collector.components.workflow.workflow_exporter import WorkflowExporter
from multi_x_serverless.data_collector.components.workflow.workflow_retriever import WorkflowRetriever

logger = logging.getLogger(__name__)


class WorkflowCollector(DataCollector):
    def __init__(self) -> None:
        super().__init__()
        self._data_collector_name: str = "workflow_collector"

        workflow_instance_table: str = WORKFLOW_INSTANCE_TABLE

        self._data_retriever: WorkflowRetriever = WorkflowRetriever(self._data_collector_client)
        self._data_exporter: WorkflowExporter = WorkflowExporter(self._data_collector_client, workflow_instance_table)

    def run(
        self,
    ) -> (
        None
    ):  # Run on all workflows -> not recommanded as it will take a lot of time, better to run on a specific workflow
        logger.error("Running on all workflows is not recommended as it will take a lot of time.")
        all_workflow_ids = self._data_retriever.retrieve_all_workflow_ids()

        for workflow_unique_id in all_workflow_ids:
            self.collect_single_workflow(workflow_unique_id)

    def run_on_workflow(self, workflow_unique_id: str) -> None:
        self.collect_single_workflow(workflow_unique_id)

    def collect_single_workflow(self, workflow_unique_id: str) -> None:  # Run on a specific workflow
        # Retrieve available regions
        self._data_retriever.retrieve_available_regions()

        workflow_summary_data: dict[str, Any] = {
            workflow_unique_id: self._data_retriever.retrieve_workflow_summary(workflow_unique_id)
        }

        self._data_exporter.export_all_data(workflow_summary_data)
