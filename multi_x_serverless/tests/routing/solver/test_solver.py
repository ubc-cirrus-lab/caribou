import unittest
from unittest.mock import Mock, patch

from multi_x_serverless.routing.models.dag import DAG
from multi_x_serverless.routing.solver.solver import Solver
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class SolverSubclass(Solver):
    def _solve(self, regions):
        pass


class TestSolver(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.instances = [
            {"instance_name": "node1", "succeeding_instances": ["node2"], "preceding_instances": []},
            {"instance_name": "node2", "succeeding_instances": [], "preceding_instances": ["node1"]},
        ]
        self.workflow_config.regions_and_providers = {
            "only_regions": [
                {
                    "provider": "aws",
                    "region": "us-east-1",
                }
            ],
            "forbidden_regions": [
                {
                    "provider": "aws",
                    "region": "us-east-2",
                }
            ],
            "providers": [
                {
                    "name": "aws",
                    "config": {
                        "timeout": 60,
                        "memory": 128,
                    },
                }
            ],
        }
        self.workflow_config.workflow_id = "workflow_id"
        self.solver = SolverSubclass(self.workflow_config)

    @patch.object(DAG, "add_edge")
    def test_get_dag_representation(self, mock_add_edge):
        dag = self.solver.get_dag_representation()

        self.assertIsInstance(dag, DAG)
        self.assertEqual(len(dag.nodes), len(self.workflow_config.instances))
        mock_add_edge.assert_called_once_with("node1", "node2")


if __name__ == "__main__":
    unittest.main()
