import unittest
from unittest.mock import Mock, patch
import numpy as np

from multi_x_serverless.routing.current.solver.simple_solver import SimpleSolver
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class TestSimpleSolver(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.constraints = None

    def test_solve_simple(self):
        self.workflow_config.functions = ["function1", "function2"]
        self.workflow_config.instances = [
            {"instance_name": "node1", "succeeding_instances": ["node2"], "preceding_instances": []},
            {"instance_name": "node2", "succeeding_instances": [], "preceding_instances": ["node1"]},
        ]
        solver = SimpleSolver(self.workflow_config)
        regions = np.array(["region1", "region2", "region3"])
        data_sources = {"carbon": Mock(), "cost": Mock(), "runtime": Mock()}
        matrices = {
            "cost": (np.array([[4, 5], [7, 8], [1, 2]]), np.array([[4, 5, 6], [7, 8, 9], [1, 2, 3]])),
            "runtime": (np.array([[7, 8], [1, 2], [4, 5]]), np.array([[7, 8, 9], [1, 2, 3], [4, 5, 6]])),
            "carbon": (np.array([[1, 2], [4, 5], [7, 8]]), np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])),
        }

        for data_source, (execution_matrix, transmission_matrix) in matrices.items():
            data_sources[data_source].get_execution_matrix.return_value = execution_matrix
            data_sources[data_source].get_transmission_matrix.return_value = transmission_matrix

        solver._data_sources = data_sources

        expected_deployments = [
            ({"function1": "region1", "function2": "region1"}, 13, 22, 4),
            ({"function1": "region2", "function2": "region2"}, 23, 5, 14),
            ({"function1": "region3", "function2": "region3"}, 6, 15, 24),
        ]

        deployments = solver._solve(regions)

        self.assertEqual(deployments, expected_deployments)

    def test_solve_complex(self):
        self.workflow_config.functions = ["function1", "function2", "function3", "function4", "function5", "function6"]
        self.workflow_config.instances = [
            {"instance_name": "node1", "succeeding_instances": ["node2", "node3"], "preceding_instances": []},
            {"instance_name": "node2", "succeeding_instances": ["node4"], "preceding_instances": ["node1"]},
            {"instance_name": "node3", "succeeding_instances": ["node4"], "preceding_instances": ["node1"]},
            {"instance_name": "node4", "succeeding_instances": ["node5"], "preceding_instances": ["node2", "node3"]},
            {"instance_name": "node5", "succeeding_instances": ["node6"], "preceding_instances": ["node4"]},
            {"instance_name": "node6", "succeeding_instances": [], "preceding_instances": ["node5"]},
        ]

        solver = SimpleSolver(self.workflow_config)
        regions = np.array(["region1", "region2", "region3", "region4"])
        data_sources = {"carbon": Mock(), "cost": Mock(), "runtime": Mock()}
        matrices = {
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

        for data_source, (execution_matrix, transmission_matrix) in matrices.items():
            data_sources[data_source].get_execution_matrix.return_value = execution_matrix
            data_sources[data_source].get_transmission_matrix.return_value = transmission_matrix

        solver._data_sources = data_sources

        expected_deployments = [
            (
                {
                    "function1": "region1",
                    "function2": "region1",
                    "function3": "region1",
                    "function4": "region1",
                    "function5": "region1",
                    "function6": "region1",
                },
                99,
                234,
                117,
            ),
            (
                {
                    "function1": "region2",
                    "function2": "region2",
                    "function3": "region2",
                    "function4": "region2",
                    "function5": "region2",
                    "function6": "region2",
                },
                123,
                105,
                168,
            ),
            (
                {
                    "function1": "region3",
                    "function2": "region3",
                    "function3": "region3",
                    "function4": "region3",
                    "function5": "region3",
                    "function6": "region3",
                },
                174,
                273,
                219,
            ),
            (
                {
                    "function1": "region4",
                    "function2": "region4",
                    "function3": "region4",
                    "function4": "region4",
                    "function5": "region4",
                    "function6": "region4",
                },
                225,
                180,
                243,
            ),
        ]

        deployments = solver._solve(regions)

        self.assertEqual(deployments, expected_deployments)


if __name__ == "__main__":
    unittest.main()
