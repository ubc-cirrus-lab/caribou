import unittest
from unittest.mock import Mock, patch
import numpy as np

from multi_x_serverless.routing.current.solver.simple_solver import SimpleSolver
from multi_x_serverless.routing.current.workflow_config import WorkflowConfig


class TestSimpleSolver(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.constraints = None
        self.workflow_config.functions = ["function1", "function2"]
        self.workflow_config.instances = [
            {"instance_name": "node1", "succeeding_instances": [], "preceding_instances": []},
            {"instance_name": "node2", "succeeding_instances": [], "preceding_instances": []},
        ]
        self.solver = SimpleSolver(self.workflow_config)

    def test_solve(self):
        regions = np.array(["region1", "region2", "region3"])
        data_sources = {"carbon": Mock(), "cost": Mock(), "runtime": Mock()}
        carbon_execution_matrix = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
        cost_execution_matrix = np.array([[4, 5, 6], [7, 8, 9], [1, 2, 3]])
        runtime_execution_matrix = np.array([[7, 8, 9], [1, 2, 3], [4, 5, 6]])

        for data_source in data_sources:
            if data_source == "carbon":
                data_sources[data_source].get_execution_matrix.return_value = carbon_execution_matrix
            elif data_source == "cost":
                data_sources[data_source].get_execution_matrix.return_value = cost_execution_matrix
            elif data_source == "runtime":
                data_sources[data_source].get_execution_matrix.return_value = runtime_execution_matrix

        self.solver._data_sources = data_sources

        expected_deployments = [
            ({"function1": "region1", "function2": "region1"}, 15, 24, 6),
            ({"function1": "region2", "function2": "region2"}, 24, 6, 15),
            ({"function1": "region3", "function2": "region3"}, 6, 15, 24),
        ]

        deployments = self.solver._solve(regions)

        self.assertEqual(deployments, expected_deployments)


if __name__ == "__main__":
    unittest.main()
