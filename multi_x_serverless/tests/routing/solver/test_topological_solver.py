import unittest
from unittest.mock import Mock, patch
import numpy as np

from multi_x_serverless.routing.solver.topological_solver import TopologicalSolver
from multi_x_serverless.routing.solver_inputs.input_manager import InputManager
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.models.region import Region
import unittest
from unittest.mock import Mock, patch


class TestTopologicalSolver(unittest.TestCase):
    execution_matrices: dict[str, np.ndarray]
    transmission_matrices: dict[str, tuple[np.ndarray, np.ndarray]]

    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.constraints = None

        # Mock input manager
        self.input_manager = Mock(spec=InputManager)
        self.input_manager.get_execution_cost_carbon_runtime.side_effect = (
            lambda current_instance_index, to_region_index, using_probabilitic = False: (
                self.execution_matrices["cost"][current_instance_index][to_region_index],
                self.execution_matrices["carbon"][current_instance_index][to_region_index],
                self.execution_matrices["runtime"][current_instance_index][to_region_index],
            )
        )

        self.input_manager.get_transmission_cost_carbon_runtime.side_effect = (
            lambda previous_instance_index, current_instance_index, from_region_index, to_region_index, using_probabilitic = False: (
                (
                    self.transmission_matrices["cost"][0][previous_instance_index][current_instance_index]
                    * self.transmission_matrices["cost"][1][from_region_index][to_region_index]
                ),
                (
                    self.transmission_matrices["carbon"][0][previous_instance_index][current_instance_index]
                    * self.transmission_matrices["carbon"][1][from_region_index][to_region_index]
                ),
                (
                    self.transmission_matrices["runtime"][0][previous_instance_index][current_instance_index]
                    * self.transmission_matrices["runtime"][1][from_region_index][to_region_index]
                ),
            )
            if from_region_index is not None and previous_instance_index is not None
            else (0, 0, 0)
        )

        # Do nothing mock input manager
        self.nothing_input_manager = Mock(spec=InputManager)
        self.nothing_input_manager.get_execution_cost_carbon_runtime.return_value = (0, 0, 0)
        self.nothing_input_manager.get_transmission_cost_carbon_runtime.return_value = (0, 0, 0)

    def test_solver_simple_3_node_line(self):
        return
        """
        This is a test for a straight line of 3 nodes. Where each node go to the next node with no split or merge nodes.
        """
        self.workflow_config.home_regions = [{"provider": "p1", "region": "r1"}]
        self.workflow_config.functions = [{"f1": ["i1"]}, {"f2": ["i2"]}, {"f3": ["i3"]}]
        self.workflow_config.regions_and_providers = {"providers": {"p1": None, "p2": None}}
        self.workflow_config.instances = [
            {
                "instance_name": "i1",
                "function_name": "f1",
                "succeeding_instances": ["i2"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
            {
                "instance_name": "i2",
                "function_name": "f2",
                "succeeding_instances": ["i3"],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                }
            },
            {
                "instance_name": "i3",
                "function_name": "f3",
                "succeeding_instances": [],
                "preceding_instances": ["i2"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]

        self.workflow_config.constraints = {
            "hard_resource_constraints": {
                "cost": {
                    "type": "absolute",
                    "value": 150,
                },
                "runtime": {
                    "type": "absolute",
                    "value": 300,
                },
                "carbon": {
                    "type": "absolute",
                    "value": 300,
                },
            }
        }

        solver = TopologicalSolver(self.workflow_config)
        solver._input_manager = self.input_manager

        # Value matricies
        # First array is in format of: # (from region, to region)
        self.execution_matrices = {
            "cost": np.array(
                [
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                ]
            ),
            "carbon": np.array(
                [
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                ]
            ),
            "runtime": np.array(
                [
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                ]
            ),
        }

        # First array is in format of: # (from region, to region)
        # Second array is in format of: # (from instance, to instance)
        self.transmission_matrices = {
            "cost": (
                np.array(
                    [
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                    ]
                ),
                np.array(
                    [
                        [10, 11, 12, 13],
                        [16, 17, 18, 19],
                        [22, 23, 24, 25],
                        [28, 29, 30, 31],
                    ]
                ),
            ),
            "runtime": (
                np.array(
                    [
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                    ]
                ),
                np.array(
                    [
                        [34, 35, 36, 37],
                        [10, 11, 12, 13],
                        [40, 41, 42, 43],
                        [22, 23, 24, 25],
                    ]
                ),
            ),
            "carbon": (
                np.array(
                    [
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                    ]
                ),
                np.array(
                    [
                        [16, 17, 18, 19],
                        [22, 23, 24, 25],
                        [28, 29, 30, 31],
                        [34, 35, 36, 37],
                    ]
                ),
            ),
        }

        # Set up the instance indexer (DAG) and region indexer
        regions = [
            {"provider": "p1", "region": "r1"},
            {"provider": "p2", "region": "r2"},
        ]
        solver._region_indexer._value_indices = {("p1", "r1"): 0, ("p2", "r2"): 1}

        # solver._data_sources = data_sources
        deployments = solver._solve(regions)

        print("\nSimple straight line DAG solver results:")
        deployment_length = len(deployments)

        print("Final deployment length:", deployment_length)

        # print(deployments[0])
        print(deployments)

    def test_solver_simple_2_node_split(self):
        return
        """
        This is a test for tree like dag with 1 parent node with 2 child leaf nodes
        """
        self.workflow_config.home_regions = [{"provider": "p1", "region": "r1"}]
        self.workflow_config.functions = [{"f1": ["i1"]}, {"f2": ["i2"]}, {"f3": ["i3"]}]
        self.workflow_config.regions_and_providers = {"providers": {"p1": None, "p2": None}}
        self.workflow_config.instances = [
            {
                "instance_name": "i1",
                "function_name": "f1",
                "succeeding_instances": ["i2", "i3"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
            {
                "instance_name": "i2",
                "function_name": "f2",
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                }
            },
            {
                "instance_name": "i3",
                "function_name": "f3",
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]

        self.workflow_config.constraints = {
            "hard_resource_constraints": {
                "cost": {
                    "type": "absolute",
                    "value": 150,
                },
                "runtime": {
                    "type": "absolute",
                    "value": 300,
                },
                "carbon": {
                    "type": "absolute",
                    "value": 300,
                },
            }
        }

        solver = TopologicalSolver(self.workflow_config)
        solver._input_manager = self.input_manager

        # Value matricies
        # First array is in format of: # (from region, to region)
        self.execution_matrices = {
            "cost": np.array(
                [
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                ]
            ),
            "carbon": np.array(
                [
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                ]
            ),
            "runtime": np.array(
                [
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                ]
            ),
        }

        # First array is in format of: # (from region, to region)
        # Second array is in format of: # (from instance, to instance)
        self.transmission_matrices = {
            "cost": (
                np.array(
                    [
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                    ]
                ),
                np.array(
                    [
                        [10, 11, 12, 13],
                        [16, 17, 18, 19],
                        [22, 23, 24, 25],
                        [28, 29, 30, 31],
                    ]
                ),
            ),
            "runtime": (
                np.array(
                    [
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                    ]
                ),
                np.array(
                    [
                        [34, 35, 36, 37],
                        [10, 11, 12, 13],
                        [40, 41, 42, 43],
                        [22, 23, 24, 25],
                    ]
                ),
            ),
            "carbon": (
                np.array(
                    [
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                    ]
                ),
                np.array(
                    [
                        [16, 17, 18, 19],
                        [22, 23, 24, 25],
                        [28, 29, 30, 31],
                        [34, 35, 36, 37],
                    ]
                ),
            ),
        }

        # Set up the instance indexer (DAG) and region indexer
        regions = [
            {"provider": "p1", "region": "r1"},
            {"provider": "p2", "region": "r2"},
        ]
        solver._region_indexer._value_indices = {("p1", "r1"): 0, ("p2", "r2"): 1}

        # solver._data_sources = data_sources
        deployments = solver._solve(regions)

        print("\nSimple straight line DAG solver results:")
        deployment_length = len(deployments)

        print("Final deployment length:", deployment_length)

        # print(deployments[0])
        print(deployments)

    def test_solver_simple_3_node_split(self):
        # return 
        """
        This is a test for tree like dag with 1 parent node with 2 child leaf nodes
        """
        self.workflow_config.home_regions = [{"provider": "p2", "region": "r2"}]
        self.workflow_config.functions = [{"f1": ["i1"]}, {"f2": ["i2"]}, {"f3": ["i3"]}, {"f4": ["i4"]}]
        self.workflow_config.regions_and_providers = {"providers": {"p1": None, "p2": None}}
        self.workflow_config.instances = [
            {
                "instance_name": "i1",
                "function_name": "f1",
                "succeeding_instances": ["i2", "i3", "i4"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
            {
                "instance_name": "i2",
                "function_name": "f2",
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                }
            },
            {
                "instance_name": "i3",
                "function_name": "f3",
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
            {
                "instance_name": "i4",
                "function_name": "f4",
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]

        # self.workflow_config.constraints = {
        #     "hard_resource_constraints": {
        #         "cost": {
        #             "type": "absolute",
        #             "value": 150,
        #         },
        #         "runtime": {
        #             "type": "absolute",
        #             "value": 300,
        #         },
        #         "carbon": {
        #             "type": "absolute",
        #             "value": 300,
        #         },
        #     }
        # }

        solver = TopologicalSolver(self.workflow_config)
        solver._input_manager = self.input_manager

        # Value matricies
        # First array is in format of: # (from region, to region)
        self.execution_matrices = {
            "cost": np.array(
                [
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                ]
            ),
            "carbon": np.array(
                [
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                ]
            ),
            "runtime": np.array(
                [
                    [7, 8, 9, 1, 2, 3],
                    [4, 5, 6, 7, 8, 9],
                    [1, 2, 3, 4, 5, 6],
                    [7, 8, 9, 1, 2, 3],
                ]
            ),
        }

        # First array is in format of: # (from region, to region)
        # Second array is in format of: # (from instance, to instance)
        self.transmission_matrices = {
            "cost": (
                np.array(
                    [
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                    ]
                ),
                np.array(
                    [
                        [10, 11, 12, 13],
                        [16, 17, 18, 19],
                        [22, 23, 24, 25],
                        [28, 29, 30, 31],
                    ]
                ),
            ),
            "runtime": (
                np.array(
                    [
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                    ]
                ),
                np.array(
                    [
                        [34, 35, 36, 37],
                        [10, 11, 12, 13],
                        [40, 41, 42, 43],
                        [22, 23, 24, 25],
                    ]
                ),
            ),
            "carbon": (
                np.array(
                    [
                        [1, 2, 3, 4, 5, 6],
                        [7, 8, 9, 1, 2, 3],
                        [4, 5, 6, 7, 8, 9],
                        [1, 2, 3, 4, 5, 6],
                    ]
                ),
                np.array(
                    [
                        [16, 17, 18, 19],
                        [22, 23, 24, 25],
                        [28, 29, 30, 31],
                        [34, 35, 36, 37],
                    ]
                ),
            ),
        }

        # Set up the instance indexer (DAG) and region indexer
        regions = [
            {"provider": "p1", "region": "r1"},
            {"provider": "p2", "region": "r2"},
        ]
        solver._region_indexer._value_indices = {("p1", "r1"): 0, ("p2", "r2"): 1}

        # solver._data_sources = data_sources
        deployments = solver._solve(regions)

        print("\nSimple straight line DAG solver results:")
        deployment_length = len(deployments)

        print("Final deployment length:", deployment_length)

        # print(deployments[0])
        print(deployments)

    def test_solve_complex(self):
        return 
        self.workflow_config.home_regions = [{"provider": "p1", "region": "r1"}]
        self.workflow_config.functions = [{"f1": ["i1"]}, {"f2": ["i2"]}, {"f3": ["i3"]}]
        self.workflow_config.regions_and_providers = {"providers": {"p1": None, "p2": None}}
        self.workflow_config.instances = [
            {
                "instance_name": "i1",
                "succeeding_instances": ["i2", "i3", "i5"],
                "preceding_instances": [],
                "regions_and_providers": {  # This should be the same as start hop
                    "allowed_regions": [{"provider": "p1", "region": "r1"}],
                    "disallowed_regions": None,  # "allowed_regions" is not None, so this should be ignored
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i2",
                "succeeding_instances": ["i4"],
                "preceding_instances": ["i1"],
                "regions_and_providers": {  # No restrictions, all providers
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None, "p3": None},
                },
            },
            {
                "instance_name": "i3",
                "succeeding_instances": ["i4"],
                "preceding_instances": ["i1"],
                "regions_and_providers": {  # No restrictions, SOME providers
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p2": None, "p3": None},
                },
            },
            {
                "instance_name": "i4",
                "succeeding_instances": ["i6"],
                "preceding_instances": ["i2", "i3"],
                "regions_and_providers": {  # No restrictions, all providers
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None, "p3": None},
                },
            },
            {
                "instance_name": "i5",
                "succeeding_instances": [],
                "preceding_instances": [],
                "regions_and_providers": {  # This should be the same as start hop (As its leaf node)
                    "allowed_regions": [{"provider": "p1", "region": "r1"}],
                    "disallowed_regions": None,  # "allowed_regions" is not None, so this should be ignored
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i6",
                "succeeding_instances": [],
                "preceding_instances": [],
                "regions_and_providers": {  # This should be the same as start hop (As its leaf node)
                    "allowed_regions": [{"provider": "p1", "region": "r1"}],
                    "disallowed_regions": None,  # "allowed_regions" is not None, so this should be ignored
                    "providers": {"p1": None},
                },
            },
        ]

        solver = TopologicalSolver(self.workflow_config)
        solver._input_manager = self.nothing_input_manager

        solver._region_indexer._value_indices = {("p1", "r1"): 0, ("p1", "r2"): 1, ("p2", "r3"): 2, ("p3", "r4"): 3}
        regions = [
            {"provider": "p1", "region": "r1"},
            {"provider": "p1", "region": "r2"},
            {"provider": "p2", "region": "r3"},
            {"provider": "p3", "region": "r4"},
        ]

        # solver._data_sources = data_sources
        deployments = solver._solve(regions)

        print("\nComplex DAG solver results:")
        deployment_length = len(deployments)
        print("Final deployment length:", deployment_length)
        # print(deployments[0])
        # self.assertEqual(deployments, expected_deployments)


if __name__ == "__main__":
    unittest.main()
