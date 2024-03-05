import json
from abc import ABC, abstractmethod

from multi_x_serverless.common.constants import WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE
from multi_x_serverless.common.models.endpoints import Endpoints
from multi_x_serverless.routing.deployment_input.input_manager import InputManager
from multi_x_serverless.routing.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)
from multi_x_serverless.routing.deployment_metrics_calculator.simple_deployment_metrics_calculator import (
    SimpleDeploymentMetricsCalculator,
)
from multi_x_serverless.routing.formatter.formatter import Formatter
from multi_x_serverless.routing.models.instance_indexer import InstanceIndexer
from multi_x_serverless.routing.models.region_indexer import RegionIndexer
from multi_x_serverless.routing.ranker.ranker import Ranker
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class DeploymentAlgorithm(ABC):  # pylint: disable=too-many-instance-attributes
    def __init__(self, workflow_config: WorkflowConfig):
        self._workflow_config = workflow_config

        self._input_manager = InputManager(workflow_config=workflow_config)

        self._workflow_level_permitted_regions = self._get_workflow_level_permitted_regions()

        self._region_indexer = RegionIndexer(self._workflow_level_permitted_regions)
        self._instance_indexer = InstanceIndexer(list(self._workflow_config.instances.values()))

        # Complete the setup of the input manager
        self._input_manager.setup(self._region_indexer, self._instance_indexer)

        self._deployment_metrics_calculator: DeploymentMetricsCalculator = SimpleDeploymentMetricsCalculator(
            workflow_config,
            self._input_manager,
            self._region_indexer,
            self._instance_indexer,
        )

        self._home_region_index = self._region_indexer.value_to_index(self._workflow_config.start_hops)

        self._number_of_instances = len(self._instance_indexer.get_value_indices().values())

        self._home_deployment, self._home_deployment_metrics = self._initialise_home_deployment()

        self._ranker = Ranker(workflow_config, self._home_deployment_metrics)

        self._formatter = Formatter(self._home_deployment, self._home_deployment_metrics)

        self._endpoints = Endpoints()

        self._per_instance_permitted_regions = [
            self._get_permitted_region_indices(self._workflow_level_permitted_regions, instance)
            for instance in range(self._number_of_instances)
        ]

    def run(self) -> None:
        deployments = self._run_algorithm()
        ranked_deployments = self._ranker.rank(deployments)
        selected_deployment = self._select_deployment(ranked_deployments)
        formatted_deployment = self._formatter.format(
            selected_deployment, self._instance_indexer.indicies_to_values(), self._region_indexer.indicies_to_values()
        )

        self._upload_result(formatted_deployment)

    @abstractmethod
    def _run_algorithm(self) -> list[tuple[list[int], dict[str, float]]]:
        raise NotImplementedError

    def _select_deployment(
        self, deployments: list[tuple[list[int], dict[str, float]]]
    ) -> tuple[list[int], dict[str, float]]:
        return deployments[0]

    def _get_workflow_level_permitted_regions(self) -> list[str]:
        # Get all regions allowed for the workflow
        all_available_regions = self._input_manager.get_all_regions()
        workflow_level_permitted_regions = self._filter_regions(
            regions=all_available_regions,
            regions_and_providers=self._workflow_config.regions_and_providers,
        )
        return workflow_level_permitted_regions

    def _filter_regions_instance(self, regions: list[str], instance_index: int) -> list[str]:
        return self._filter_regions(
            regions,
            self._workflow_config.instances[self._instance_indexer.index_to_value(instance_index)][
                "regions_and_providers"
            ],
        )

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

    def _is_hard_constraint_failed(self, metrics: dict[str, float]) -> bool:
        if (
            self._workflow_config.constraints is None
            or "hard_resource_constraints" not in self._workflow_config.constraints
        ):
            return False
        hard_resource_constraints = self._workflow_config.constraints["hard_resource_constraints"]
        return (
            "cost" in hard_resource_constraints
            and self._ranker.is_absolute_or_relative_failed(
                metrics["tail_cost"], hard_resource_constraints["cost"], self._home_deployment_metrics["tail_cost"]
            )
            or "runtime" in hard_resource_constraints
            and self._ranker.is_absolute_or_relative_failed(
                metrics["tail_runtime"],
                hard_resource_constraints["runtime"],
                self._home_deployment_metrics["tail_runtime"],
            )
            or "carbon" in hard_resource_constraints
            and self._ranker.is_absolute_or_relative_failed(
                metrics["tail_carbon"],
                hard_resource_constraints["carbon"],
                self._home_deployment_metrics["tail_carbon"],
            )
        )

    def _initialise_home_deployment(self) -> tuple[list[int], dict[str, float]]:
        home_deployment = [self._home_region_index for _ in range(self._number_of_instances)]

        home_deployment_metrics = self._deployment_metrics_calculator.calculate_deployment_metrics(home_deployment)

        return home_deployment, home_deployment_metrics

    def _get_permitted_region_indices(self, regions: list[str], instance: int) -> list[int]:
        permitted_regions: list[str] = self._filter_regions_instance(regions, instance)
        if len(permitted_regions) == 0:  # Should never happen in a valid DAG
            raise ValueError("There are no permitted regions for this instance")

        all_regions_indices = self._region_indexer.get_value_indices()
        permitted_regions_indices = [all_regions_indices[region] for region in permitted_regions]
        return permitted_regions_indices
