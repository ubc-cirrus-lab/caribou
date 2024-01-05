from abc import ABC, abstractmethod
from typing import Optional

import numpy as np

from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.models.region import Region
from multi_x_serverless.routing.models.dag import DAG

from multi_x_serverless.routing.data_sources.data_manager import DataManager
from multi_x_serverless.routing.calculations.calculation_manager import CalculationManager
from multi_x_serverless.routing.ranker.ranker import Ranker

class Solver(ABC):
    def __init__(self, workflow_config: WorkflowConfig):
        self._workflow_config = workflow_config

        # Set up the DAG and region source
        self._dag = self.get_dag_representation()
        self._region_source = Region(workflow_config)
        
        # Setup the data manager (Not instantiated yet)
        self._data_manager = DataManager(workflow_config, self._region_source, self._dag)

        # Setup the calculation manager
        self._calculation_manager = CalculationManager(self._data_manager)

        # Setup the ranker for final ranking of solutions
        self._ranker = Ranker(workflow_config)

    def solve(self) -> list[tuple[dict, float, float, float]]:
        filtered_regions = self._filter_regions_global(self._region_source.get_all_regions())
        self._instantiate_data_manager(filtered_regions)
        solved_results = self._solve(filtered_regions)
        ranked_results = self.rank_solved_results(solved_results)
        
        # TODO (33): Implement output formatter for solver

        return ranked_results

    @abstractmethod
    def _solve(self, regions: np.ndarray) -> list[tuple[dict, float, float, float]]:
        raise NotImplementedError

    def _filter_regions(self, regions: list, regions_and_providers: dict) -> np.ndarray:
        # Take in a list of regions, then apply filters to remove regions that do not satisfy the constraints

        # First filter out regions that are not in the provider list
        provider_names = [provider['name'] for provider in regions_and_providers['providers']]
        regions = [region for region in regions if region[0] in provider_names]

        # Then if the user set a allowed_regions, only permit those regions and return
        if "allowed_regions" in regions_and_providers and regions_and_providers["allowed_regions"] is not None:
            return np.array([region for region in regions if (region[0], region[1]) in regions_and_providers["allowed_regions"]])
        
        # Finally we filter out regions that the user doesn't want to use
        if "disallowed_regions" in regions_and_providers and regions_and_providers["disallowed_regions"] is not None:
            regions =  [region for region in regions if (region[0], region[1]) not in regions_and_providers["disallowed_regions"]]

        return np.array(regions)

    def _filter_regions_global(self, regions: np.ndarray) -> np.ndarray:
        # TODO (#21): Implement this function
        return self._filter_regions(regions, self._workflow_config['regions_and_providers'])

    def _filter_regions_instance(self, regions: np.ndarray, instance_index: str) -> np.ndarray:
        # TODO (#24): Implement this instance
        return self._filter_regions(regions, self._workflow_config.instances[instance_index]["regions_and_providers"])

    def rank_solved_results(
        self, results: list[tuple[dict, float, float, float]]
    ) -> list[tuple[dict, float, float, float]]:
        return self._ranker.rank(results)

    def _instantiate_data_manager(self, regions: np.ndarray) -> None:
        self._data_manager.setup(regions)

    def get_dag_representation(self) -> DAG:
        nodes = [
            {k: v for k, v in node.items() if k != "succeeding_instances" and k != "preceding_instances"}
            for node in self._workflow_config.instances
        ]
        dag = DAG(nodes)
        for instance in self._workflow_config.instances:
            for succeeding_instance in instance["succeeding_instances"]:
                dag.add_edge(instance["instance_name"], succeeding_instance)
        return dag

    def _fail_hard_resource_constraints(
        self, constraints: Optional[dict], cost: float, runtime: float, carbon: float
    ) -> bool:
        if constraints is None or "hard_resource_constraints" not in constraints:
            return False
        hard_resource_constraints = constraints["hard_resource_constraints"]
        return (
            "cost" in hard_resource_constraints
            and cost > hard_resource_constraints["cost"]["value"]
            or "runtime" in hard_resource_constraints
            and runtime > hard_resource_constraints["runtime"]["value"]
            or "carbon" in hard_resource_constraints
            and carbon > hard_resource_constraints["carbon"]["value"]
        )
