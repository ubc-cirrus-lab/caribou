import unittest
from unittest.mock import Mock, patch, MagicMock
import numpy as np

from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.solver.solver import Solver
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class SolverSubclass(Solver):
    def _solve(self, regions):
        pass

    def _init_home_region_transmission_costs(self, regions) -> None:
        pass

    def _instantiate_input_manager(self) -> None:
        pass


class OtherSolverSubclass(Solver):
    def __init__(self, workflow_config, all_available_regions):
        super().__init__(workflow_config, all_available_regions, None)

    def _solve(self, regions):
        pass

    def _instantiate_input_manager(self) -> None:
        pass


class TestSolver(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.instances = {
            "node1": {
                "instance_name": "node1",
                "succeeding_instances": ["node2"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
            "node2": {
                "instance_name": "node2",
                "succeeding_instances": [],
                "preceding_instances": ["node1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"provider1": None},
                },
            },
        }
        self.workflow_config.regions_and_providers = {
            "allowed_regions": None,
            "disallowed_regions": None,
            "providers": {"provider1": None, "provider2": None, "provider3": None},
        }
        self.workflow_config.workflow_id = "workflow_id"
        self.workflow_config.start_hops = "provider1:region1"
        self.solver = SolverSubclass(self.workflow_config, all_available_regions=["provider1:region1"])

    @patch.object(Solver, "_get_permitted_region_indices")
    def test_init_home_region_transmission_costs(self, mock_get_permitted_region_indices):
        solver = OtherSolverSubclass(
            self.workflow_config,
            all_available_regions=["provider1:region1", "provider2:region2", "provider3:region3"],
        )

        mock_get_permitted_region_indices.return_value = [0, 1, 2]
        mock_input_manager = Mock()
        mock_input_manager.get_transmission_cost_carbon_runtime.side_effect = [
            (1, 2, 3),  # for average
            (4, 5, 6),  # for tail
            (1, 2, 3),  # for average
            (4, 5, 6),  # for tail
            (1, 2, 3),  # for average
            (4, 5, 6),  # for tail
        ]
        regions = ["provider1:region1", "provider2:region2"]

        solver._input_manager = mock_input_manager

        solver._init_home_region_transmission_costs(regions)

        np.testing.assert_array_equal(
            solver._home_region_transmission_costs_average, np.array([[1, 1, 1], [3, 3, 3], [2, 2, 2]])
        )
        np.testing.assert_array_equal(
            solver._home_region_transmission_costs_tail, np.array([[4, 4, 4], [6, 6, 6], [5, 5, 5]])
        )

    @patch.object(DAG, "add_edge")
    def test_get_dag_representation(self, mock_add_edge):
        dag = self.solver.get_dag_representation()

        self.assertIsInstance(dag, DAG)
        self.assertEqual(len(dag.nodes), len(self.workflow_config.instances))
        mock_add_edge.assert_called_once_with("node1", "node2")

    def test_filter_regions(self):
        regions = ["provider1:region1", "provider2:region2"]
        regions_and_providers = {
            "providers": {"provider1": {}},
            "allowed_regions": ["provider1:region1"],
            "disallowed_regions": ["provider2:region2"],
        }
        filtered_regions = self.solver._filter_regions(regions, regions_and_providers)
        self.assertEqual(filtered_regions, ["provider1:region1"])

    def test_filter_regions_global(self):
        self.workflow_config.regions_and_providers = {
            "providers": {"provider1": {}},
            "allowed_regions": ["provider1:region1"],
            "disallowed_regions": ["provider2:region2"],
        }
        regions = ["provider1:region1", "provider2:region2"]
        filtered_regions = self.solver._filter_regions_global(regions)
        self.assertEqual(filtered_regions, ["provider1:region1"])

    def test_filter_regions_instance(self):
        self.workflow_config.instances = {
            "node1": {
                "regions_and_providers": {
                    "providers": {"provider1": {}},
                    "allowed_regions": ["provider1:region1"],
                    "disallowed_regions": ["provider2:region2"],
                }
            }
        }
        regions = ["provider1:region1", "provider2:region2"]
        filtered_regions = self.solver._filter_regions_instance(regions, 0)
        self.assertEqual(filtered_regions, ["provider1:region1"])

    def test_most_expensive_path(self):
        self.solver._dag.topological_sort = MagicMock(return_value=[0, 1, 2])

        edge_weights = np.array([[0, 1, 0], [0, 0, 1], [0, 0, 0]])
        node_weights = np.array([1, 2, 3])

        result = self.solver._most_expensive_path(edge_weights, node_weights)
        self.assertEqual(result, 8.0)


if __name__ == "__main__":
    unittest.main()
