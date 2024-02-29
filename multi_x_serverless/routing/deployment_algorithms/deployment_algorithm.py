from abc import ABC, abstractmethod
from typing import List, Optional
import json

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.solver.input.input_manager import InputManager
from multi_x_serverless.routing.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.routing.formatter.formatter import Formatter
from multi_x_serverless.routing.ranker.ranker import Ranker
from multi_x_serverless.routing.models.region_indexer import RegionIndexer
from multi_x_serverless.routing.models.instance_indexer import InstanceIndexer


class DeploymentAlgorithm(ABC):
    def __init__(
        self,
        workflow_config: WorkflowConfig,
    ):
        self._workflow_config = workflow_config

        self._input_manager = InputManager(
            workflow_config=workflow_config,
        )

        self._deployment_metrics_calculator = DeploymentMetricsCalculator(
            workflow_config=workflow_config,
            input_manager=self._input_manager,
        )

        self._workflow_level_permitted_regions = self._get_workflow_level_permitted_regions()

        self._region_indexer = RegionIndexer(self._workflow_level_permitted_regions)
        self._instance_indexer = InstanceIndexer(self._workflow_config.instances)

        self._home_region_index = self._region_indexer.value_to_index(self._workflow_config.start_hops)

        self._home_deployment, self._home_deployment_metrics = self._initialise_home_deployment()

        self._ranker = Ranker(workflow_config, self._home_deployment_metrics)

        self._formatter = Formatter(self._home_deployment, self._home_deployment_metrics)

        self._endpoints = Endpoints()

    def run(self) -> None:
        deployments = self._run_algorithm()
        ranked_deployments = self._ranker.rank(deployments)
        selected_deployment = self._select_deployment(ranked_deployments)
        formatted_deployment = self._formatter.format(selected_deployment)

        self._upload_result(formatted_deployment)

    @abstractmethod
    def _run_algorithm(self) -> list[tuple[dict[int, int], dict[str, float]]]:
        raise NotImplementedError

    def _select_deployment(self, deployments: list[tuple[dict[int, int], dict[str, float]]]) -> tuple[dict[int, int], dict[str, float]]:
        return deployments[0]

    def _get_workflow_level_permitted_regions(self) -> List[str]:
        all_available_regions = self._input_manager.get_all_available_regions()
        workflow_level_permitted_regions = self._filter_regions(
            regions=all_available_regions,
            regions_and_providers=self._workflow_config.regions_and_providers,
        )
        return workflow_level_permitted_regions

    def _filter_regions_instance(self, regions: list[str], instance_index: int) -> list[str]:
        return self._filter_regions(regions, self._workflow_config.instances[instance_index]["regions_and_providers"])

    def _filter_regions(self, regions: list[str], regions_and_providers: dict) -> list[str]:
        # Take in a list of regions, then apply filters to remove regions that do not satisfy the constraints
        # First filter out regions that are not in the provider list
        provider_names = list(regions_and_providers["providers"].keys())
        regions = [region for region in regions if region.split(":")[0] in provider_names]

        # Then if the user set a allowed_regions, only permit those regions and return
        if "allowed_regions" in regions_and_providers and regions_and_providers["allowed_regions"] is not None:
            return [region for region in regions if region in regions_and_providers["allowed_regions"]]

        # Finally we filter out regions that the user doesn't want to use
        if "disallowed_regions" in regions_and_providers and regions_and_providers["disallowed_regions"] is not None:
            regions = [region for region in regions if region not in regions_and_providers["disallowed_regions"]]

        return regions

    def _upload_result(
        self,
        result: dict,
    ) -> None:
        result_json = json.dumps(result)
        self._endpoints.get_solver_workflow_placement_decision_client().set_value_in_table(
            WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE, self._workflow_config.workflow_id, result_json
        )

    def _is_hard_constraint_failed(
        self, constraints: Optional[dict], cost: float, runtime: float, carbon: float
    ) -> bool:
        if constraints is None or "hard_resource_constraints" not in constraints:
            return False
        hard_resource_constraints = constraints["hard_resource_constraints"]
        return (
            "cost" in hard_resource_constraints
            and self._ranker.is_absolute_or_relative_failed(
                cost, hard_resource_constraints["cost"], self._home_deployment_metrics["tail_cost"]
            )
            or "runtime" in hard_resource_constraints
            and self._ranker.is_absolute_or_relative_failed(
                runtime, hard_resource_constraints["runtime"], self._home_deployment_metrics["tail_runtime"]
            )
            or "carbon" in hard_resource_constraints
            and self._ranker.is_absolute_or_relative_failed(
                carbon, hard_resource_constraints["carbon"], self._home_deployment_metrics["tail_carbon"]
            )
        )

    def _initialise_home_deployment(self) -> tuple[dict[int, int], dict[str, float]]:
        home_deployment = {
            instance_index: self._home_region_index for instance_index in range(len(self._workflow_config.instances))
        }

        home_deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(home_deployment)

        return home_deployment, home_deployment_metrics