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

        # Setup custom solver functions - to avoid using the actual data sources
        def custom_get_transmission_cost_carbon_runtime(
            previous_instance_index, current_instance_index, from_region_index, to_region_index
        ):
            if from_region_index is None:
                return 0, 0, 0  # So nothing was moved

            cost = (
                self.transmission_matrices["cost"][0][previous_instance_index][current_instance_index]
                * self.transmission_matrices["cost"][1][from_region_index][to_region_index]
            )
            carbon = (
                self.transmission_matrices["carbon"][0][previous_instance_index][current_instance_index]
                * self.transmission_matrices["carbon"][1][from_region_index][to_region_index]
            )
            runtime = (
                self.transmission_matrices["runtime"][0][previous_instance_index][current_instance_index]
                * self.transmission_matrices["runtime"][1][from_region_index][to_region_index]
            )
            return cost, carbon, runtime

        def custom_get_execution_cost_carbon_runtime(current_instance_index, to_region_index):
            cost = self.execution_matrices["cost"][current_instance_index][to_region_index]
            carbon = self.execution_matrices["carbon"][current_instance_index][to_region_index]
            runtime = self.execution_matrices["runtime"][current_instance_index][to_region_index]
            return cost, carbon, runtime

        self.input_manager = Mock(spec=InputManager)
        self.input_manager.get_execution_cost_carbon_runtime.side_effect = custom_get_execution_cost_carbon_runtime
        self.input_manager.get_transmission_cost_carbon_runtime.side_effect = (
            custom_get_transmission_cost_carbon_runtime
        )

    def test_solver_simple_line(self):
        """
        This is a test for a straight line of 3 nodes. Where each node go to the next node with no split or merge nodes.
        """
        self.workflow_config.functions = ["f1", "f2", "f3"]
        self.workflow_config.instances = [
            {
                "instance_name": "i1",
                "function_name": "f1",
                "succeeding_instances": ["i2"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {
                        "p1": {"config": {"timeout": 60, "memory": 128}},
                        "p2": {"config": {"timeout": 60, "memory": 128}},
                    },
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
                    "providers": {
                        "p1": {"config": {"timeout": 60, "memory": 128}},
                        "p2": {"config": {"timeout": 60, "memory": 128}},
                    },
                },
            },
            {
                "instance_name": "i3",
                "function_name": "f3",
                "succeeding_instances": [],
                "preceding_instances": ["i2"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {
                        "p1": {"config": {"timeout": 60, "memory": 128}},
                        "p2": {"config": {"timeout": 60, "memory": 128}},
                    },
                },
            },
        ]
        self.workflow_config.regions_and_providers = {
            "providers": {
                "p1": {"config": {"timeout": 60, "memory": 128}},
                "p2": {"config": {"timeout": 60, "memory": 128}},
            },
        }

        self.workflow_config.constraints = {
            "hard_resource_constraints": {
                "cost": 15,
                "runtime": 15,
                "carbon": 15,
            }
        }

#     constraints:
#   hard_resource_constraints: # None for none
#     cost:
#       type: "absolute" # Absolute value as 'absolute' (in USD) or Percentage from deployment at home regions as 'percentage' (In fractions such as 1.1)
#       value: COST_CONSTRAINT
#     runtime:
#       type: "absolute"
#       value: RUNTIME_CONSTRAINT
#     carbon:
#       type: "absolute"
#       value: CARBON_CONSTRAINT
        
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

        print("Simple straight line DAG solver results:")
        deployment_length = len(deployments)

        print("Final deployment length:", deployment_length)


        print(deployments[0])
        print(deployments)

    #     expected_deployments = [
    #         ({"function1": "region1", "function2": "region1"}, 13, 22, 4),
    #         ({"function1": "region2", "function2": "region2"}, 23, 5, 14),
    #         ({"function1": "region3", "function2": "region3"}, 6, 15, 24),
    #     ]

    #     deployments = solver._solve(regions)

    #     self.assertEqual(deployments, expected_deployments)

    # def test_solve_simple(self):
    #     self.workflow_config.functions = ["function1", "function2"]
    #     self.workflow_config.instances = [
    #         {"instance_name": "node1", "succeeding_instances": ["node2"], "preceding_instances": []},
    #         {"instance_name": "node2", "succeeding_instances": [], "preceding_instances": ["node1"]},
    #     ]
    #     solver = TopologicalSolver(self.workflow_config)
    #     regions = np.array(["region1", "region2", "region3"])
    #     data_sources = {"carbon": Mock(), "cost": Mock(), "runtime": Mock()}
    #     matrices = {
    #         "cost": (np.array([[4, 5], [7, 8], [1, 2]]), np.array([[4, 5, 6], [7, 8, 9], [1, 2, 3]])),
    #         "runtime": (np.array([[7, 8], [1, 2], [4, 5]]), np.array([[7, 8, 9], [1, 2, 3], [4, 5, 6]])),
    #         "carbon": (np.array([[1, 2], [4, 5], [7, 8]]), np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])),
    #     }

    #     for data_source, (execution_matrix, transmission_matrix) in matrices.items():
    #         data_sources[data_source].get_execution_matrix.return_value = execution_matrix
    #         data_sources[data_source].get_transmission_matrix.return_value = transmission_matrix

    #     solver._data_sources = data_sources

    #     expected_deployments = [
    #         ({"function1": "region1", "function2": "region1"}, 13, 22, 4),
    #         ({"function1": "region2", "function2": "region2"}, 23, 5, 14),
    #         ({"function1": "region3", "function2": "region3"}, 6, 15, 24),
    #     ]

    #     deployments = solver._solve(regions)

    #     self.assertEqual(deployments, expected_deployments)

    # def test_solve_complex(self):
    #     self.workflow_config.workflow_id = "1simple_line"
    #     self.workflow_config.home_regions = ([{"provider": "aws", "region": "eu-central-1"}],)
    #     self.workflow_config.functions = [{"f1": ["i1"]}, {"f2": ["i2"]}, {"f3": ["i3"]}]
    #     self.workflow_config.regions_and_providers = {
    #         "allowed_regions": None,
    #         "disallowed_regions": None,
    #         "providers": [{"name": "p1"}, {"name": "p2"}, {"name": "p3"}],
    #     }
    #     self.workflow_config.instances = [
    #         {
    #             "instance_name": "i1",
    #             "succeeding_instances": ["i2", "i3", "i5"],
    #             "preceding_instances": [],
    #             "regions_and_providers": {  # This should be the same as start hop
    #                 "allowed_regions": [{"provider": "p1", "region": "r1"}],
    #                 "disallowed_regions": None,  # "allowed_regions" is not None, so this should be ignored
    #                 "providers": [{"name": "p1"}],
    #             },
    #         },
    #         {
    #             "instance_name": "i2",
    #             "succeeding_instances": ["i4"],
    #             "preceding_instances": ["i1"],
    #             "regions_and_providers": {  # No restrictions, all providers
    #                 "allowed_regions": None,
    #                 "disallowed_regions": None,
    #                 "providers": [{"name": "p1"}, {"name": "p2"}, {"name": "p3"}],
    #             },
    #         },
    #         {
    #             "instance_name": "i3",
    #             "succeeding_instances": ["i4"],
    #             "preceding_instances": ["i1"],
    #             "regions_and_providers": {  # No restrictions, SOME providers
    #                 "allowed_regions": None,
    #                 "disallowed_regions": None,
    #                 "providers": [{"name": "p2"}, {"name": "p3"}],
    #             },
    #         },
    #         {
    #             "instance_name": "i4",
    #             "succeeding_instances": ["i6"],
    #             "preceding_instances": ["i2", "i3"],
    #             "regions_and_providers": {  # No restrictions, all providers
    #                 "allowed_regions": None,
    #                 "disallowed_regions": None,
    #                 "providers": [{"name": "p1"}, {"name": "p2"}, {"name": "p3"}],
    #             },
    #         },
    #         {
    #             "instance_name": "i5",
    #             "succeeding_instances": [],
    #             "preceding_instances": ["i1"],
    #             "preceding_instances": [],
    #             "regions_and_providers": {  # This should be the same as start hop (As its leaf node)
    #                 "allowed_regions": [{"provider": "p1", "region": "r1"}],
    #                 "disallowed_regions": None,  # "allowed_regions" is not None, so this should be ignored
    #                 "providers": [{"name": "p1"}],
    #             },
    #         },
    #         {
    #             "instance_name": "i6",
    #             "succeeding_instances": [],
    #             "preceding_instances": ["i4"],
    #             "preceding_instances": [],
    #             "regions_and_providers": {  # This should be the same as start hop (As its leaf node)
    #                 "allowed_regions": [{"provider": "p1", "region": "r1"}],
    #                 "disallowed_regions": None,  # "allowed_regions" is not None, so this should be ignored
    #                 "providers": [{"name": "p1"}],
    #             },
    #         },
    #     ]

    #     solver = TopologicalSolver(self.workflow_config)

    #     # solver._region_source = Region(self.workflow_config)
    #     # solver._region_source._value_indices = {("p1", "r1"): 0, ("p1", "r2"): 1, ("p2", "r3"): 2, ("p3", "r4"): 3}
    #     regions = [
    #         {"provider": "p1", "region": "r1"},
    #         {"provider": "p1", "region": "r2"},
    #         {"provider": "p2", "region": "r3"},
    #         {"provider": "p3", "region": "r4"},
    #     ]

    #     # data_sources = {"carbon": Mock(), "cost": Mock(), "runtime": Mock()}
    #     # matrices = {
    #     #     "cost": (
    #     #         np.array(
    #     #             [
    #     #                 [4, 5, 6, 7, 8, 9],
    #     #                 [1, 2, 3, 4, 5, 6],
    #     #                 [7, 8, 9, 1, 2, 3],
    #     #                 [4, 5, 6, 7, 8, 9],
    #     #             ]
    #     #         ),
    #     #         np.array(
    #     #             [
    #     #                 [10, 11, 12, 13],
    #     #                 [16, 17, 18, 19],
    #     #                 [22, 23, 24, 25],
    #     #                 [28, 29, 30, 31],
    #     #             ]
    #     #         ),
    #     #     ),
    #     #     "runtime": (
    #     #         np.array(
    #     #             [
    #     #                 [7, 8, 9, 1, 2, 3],
    #     #                 [4, 5, 6, 7, 8, 9],
    #     #                 [1, 2, 3, 4, 5, 6],
    #     #                 [7, 8, 9, 1, 2, 3],
    #     #             ]
    #     #         ),
    #     #         np.array(
    #     #             [
    #     #                 [34, 35, 36, 37],
    #     #                 [10, 11, 12, 13],
    #     #                 [40, 41, 42, 43],
    #     #                 [22, 23, 24, 25],
    #     #             ]
    #     #         ),
    #     #     ),
    #     #     "carbon": (
    #     #         np.array(
    #     #             [
    #     #                 [1, 2, 3, 4, 5, 6],
    #     #                 [7, 8, 9, 1, 2, 3],
    #     #                 [4, 5, 6, 7, 8, 9],
    #     #                 [1, 2, 3, 4, 5, 6],
    #     #             ]
    #     #         ),
    #     #         np.array(
    #     #             [
    #     #                 [16, 17, 18, 19],
    #     #                 [22, 23, 24, 25],
    #     #                 [28, 29, 30, 31],
    #     #                 [34, 35, 36, 37],
    #     #             ]
    #     #         ),
    #     #     ),
    #     # }

    #     # for data_source, (execution_matrix, transmission_matrix) in matrices.items():
    #     #     data_sources[data_source].get_execution_matrix.return_value = execution_matrix
    #     #     data_sources[data_source].get_transmission_matrix.return_value = transmission_matrix

    #     # solver._data_sources = data_sources
    #     deployments = solver._solve(regions)

    #     deployment_length = len(deployments)
    #     print("Final deployment length:", deployment_length)
    #     print(deployments[0])
    #     # self.assertEqual(deployments, expected_deployments)

    #     # a = {
    #     #         "provider": "aws",
    #     #         "region": "us-east-2",
    #     #     }
    #     # b = {
    #     #         "provider": "aws",
    #     #         "region": "us-east-2",
    #     #     }

    #     # print(a)
    #     # print(b)
    #     # print("identical:", a == b)

    #     # Save the data into a dictionary for data sources
    #     # test_dict = {
    #     #     **{'a': 1, 'b': 2},
    #     #     **{'c': 3, 'd': [4, 5]},
    #     #     **{'e': {'f': 6, 'g': 7}}
    #     # }
    #     # print(test_dict)


if __name__ == "__main__":
    print("Testing")
    unittest.main()
