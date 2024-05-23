import json
import pdb
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Optional

from caribou.common.constants import (
    DEFAULT_MONITOR_COOLDOWN,
    GLOBAL_TIME_ZONE,
    TIME_FORMAT,
    WORKFLOW_PLACEMENT_SOLVER_STAGING_AREA_TABLE,
)
from caribou.common.models.endpoints import Endpoints
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.deployment_metrics_calculator import (
    DeploymentMetricsCalculator,
)
from caribou.deployment_solver.deployment_metrics_calculator.simple_deployment_metrics_calculator import (
    SimpleDeploymentMetricsCalculator,
)
from caribou.deployment_solver.formatter.formatter import Formatter
from caribou.deployment_solver.models.instance_indexer import InstanceIndexer
from caribou.deployment_solver.models.region_indexer import RegionIndexer
from caribou.deployment_solver.ranker.ranker import Ranker
from caribou.deployment_solver.workflow_config import WorkflowConfig


class DeploymentAlgorithm(ABC):  # pylint: disable=too-many-instance-attributes
    def __init__(self, workflow_config: WorkflowConfig, expiry_time_delta_seconds: int = DEFAULT_MONITOR_COOLDOWN):
        self._workflow_config = workflow_config

        self._input_manager = InputManager(workflow_config=workflow_config)

        self._workflow_level_permitted_regions = self._get_workflow_level_permitted_regions()

        self._region_indexer = RegionIndexer(self._workflow_level_permitted_regions)
        self._instance_indexer = InstanceIndexer(list(self._workflow_config.instances.values()))

        # pdb.set_trace()
        # Complete the setup of the input manager
        self._input_manager.setup(self._region_indexer, self._instance_indexer)

        self._deployment_metrics_calculator: DeploymentMetricsCalculator = SimpleDeploymentMetricsCalculator(
            workflow_config,
            self._input_manager,
            self._region_indexer,
            self._instance_indexer,
        )

        self._home_region_index = self._region_indexer.value_to_index(self._workflow_config.home_region)

        self._number_of_instances = len(self._instance_indexer.get_value_indices().values())

        self._ranker = Ranker(workflow_config, None)

        self._expiry_time_delta_seconds = expiry_time_delta_seconds

        self._formatter = Formatter()

        self._endpoints = Endpoints()

        self._per_instance_permitted_regions = [
            self._get_permitted_region_indices(self._workflow_level_permitted_regions, instance)
            for instance in range(self._number_of_instances)
        ]

    def run(self, hours_to_run: Optional[list[str]] = None) -> None:
        hour_to_run_to_result: dict[str, Any] = {
            "time_keys_to_staging_area_data": {},
        }
        if hours_to_run is None:
            hours_to_run = [None]  # type: ignore
        for hour_to_run in hours_to_run:
            self._update_data_for_new_hour(hour_to_run)
            start_time = time.time()
            deployments = self._run_algorithm()
            ranked_deployments = self._ranker.rank(deployments)
            selected_deployment = self._select_deployment(ranked_deployments)
            print(f"Solve Time: {time.time() - start_time}")
            formatted_deployment = self._formatter.format(
                selected_deployment,
                self._instance_indexer.indicies_to_values(),
                self._region_indexer.indicies_to_values(),
            )
            print(selected_deployment)
            print("------")
            if hour_to_run is None:
                # If the hour_to_run is None, we have used the daily average and thus only have one result
                # For this result to be selected at all times, we set the key to "0"
                hour_to_run = "0"
            hour_to_run_to_result["time_keys_to_staging_area_data"][hour_to_run] = formatted_deployment

        self._add_expiry_date_to_results(hour_to_run_to_result)

        self._upload_result(hour_to_run_to_result)

    def _update_data_for_new_hour(self, hour_to_run: str) -> None:
        self._input_manager.alter_carbon_setting(hour_to_run)
        (
            self._home_deployment,  # pylint: disable=attribute-defined-outside-init
            self._home_deployment_metrics,  # pylint: disable=attribute-defined-outside-init
        ) = self._initialise_home_deployment()
        self._ranker.update_home_deployment_metrics(self._home_deployment_metrics)

    def _add_expiry_date_to_results(self, hour_to_run_to_result: dict[str, Any]) -> None:
        expiry_date = datetime.now(GLOBAL_TIME_ZONE) + timedelta(seconds=self._expiry_time_delta_seconds)
        expiry_date_str = expiry_date.strftime(TIME_FORMAT)
        hour_to_run_to_result["expiry_time"] = expiry_date_str

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
            self._workflow_config.create_altered_regions_and_providers(
                self._workflow_config.instances[self._instance_indexer.index_to_value(instance_index)][
                    "regions_and_providers"
                ]
            ),
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
        self._endpoints.get_deployment_algorithm_workflow_placement_decision_client().set_value_in_table(
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
            raise ValueError(
                f"There are no permitted regions for this instance {self._instance_indexer.index_to_value(instance)}"
            )

        all_regions_indices = self._region_indexer.get_value_indices()
        permitted_regions_indices = [all_regions_indices[region] for region in permitted_regions]
        return permitted_regions_indices
