import unittest
from unittest.mock import Mock, patch
import numpy as np

from multi_x_serverless.routing.solver.topological_solver import TopologicalSolver
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.models.region import Region

class TestTopologicalSolver(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.constraints = None

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

    def test_solve_complex(self):
        self.workflow_config.functions = ["f1", "f2", "f3", "f4", "f5", "f6"]
        self.workflow_config.regions_and_providers = {
            "allowed_regions": None,
            "disallowed_regions": None,
            "providers": [{"name": "p1"}, {"name": "p2"}, {"name": "p3"}],
        }
        self.workflow_config.instances = [
            {"instance_name": "i1", "succeeding_instances": ["i2", "i3", "i5", "i7"], "preceding_instances": [],
             "regions_and_providers": { # This should be the same as start hop
                 "allowed_regions": [["p1", "r1"]],
                 "disallowed_regions": None, # "allowed_regions" is not None, so this should be ignored
                 "providers": [{"name": "p1"}]
             }},
            {"instance_name": "i2", "succeeding_instances": ["i4"], "preceding_instances": ["i1"],
             "regions_and_providers": { # No restrictions, all providers
                "allowed_regions": None,
                "disallowed_regions": None,
                "providers": [{"name": "p1"}, {"name": "p2"}, {"name": "p3"}],
             }},
            {"instance_name": "i3", "succeeding_instances": ["i4"], "preceding_instances": ["i1"],
             "regions_and_providers": { # No restrictions, SOME providers
                "allowed_regions": None,
                "disallowed_regions": None,
                "providers": [{"name": "p2"}, {"name": "p3"}],
             }},
            {"instance_name": "i4", "succeeding_instances": ["i6"], "preceding_instances": ["i2", "i3"],
             "regions_and_providers": { # No restrictions, all providers
                "allowed_regions": None,
                "disallowed_regions": None,
                "providers": [{"name": "p1"}, {"name": "p2"}, {"name": "p3"}],
             }},
            {"instance_name": "i5", "succeeding_instances": [], "preceding_instances": ["i1"], "preceding_instances": [],
             "regions_and_providers": { # This should be the same as start hop (As its leaf node)
                 "allowed_regions": [["p1", "r1"]],
                 "disallowed_regions": None, # "allowed_regions" is not None, so this should be ignored
                 "providers": [{"name": "p1"}]
             }},
            {"instance_name": "i6", "succeeding_instances": [], "preceding_instances": ["i4"], "preceding_instances": [],
             "regions_and_providers": { # This should be the same as start hop (As its leaf node)
                 "allowed_regions": [["p1", "r1"]],
                 "disallowed_regions": None, # "allowed_regions" is not None, so this should be ignored
                 "providers": [{"name": "p1"}]
             }},
        ]

        solver = TopologicalSolver(self.workflow_config)
        solver._region_source = Region(self.workflow_config)
        solver._region_source._region_indices = {("p1", "r1"): 0, ("p1", "r2"): 1, ("p2", "r3"): 2, ("p3", "r4"): 3}
        regions = np.array([("p1", "r1"), ("p1", "r2"), ("p2", "r3"), ("p3", "r4")])
        
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
        deployments = solver._solve(regions)
        deployment_length = len(deployments)
        print("Final deployment length:", deployment_length)
        # self.assertEqual(deployments, expected_deployments)

        # a = {
        #         "provider": "aws",
        #         "region": "us-east-2",
        #     }
        # b = {
        #         "provider": "aws",
        #         "region": "us-east-2",
        #     }
        
        # print(a)
        # print(b)
        # print("identical:", a == b)

                # Save the data into a dictionary for data sources
        # test_dict = {
        #     **{'a': 1, 'b': 2},
        #     **{'c': 3, 'd': [4, 5]},
        #     **{'e': {'f': 6, 'g': 7}}
        # }
        print(test_dict)

if __name__ == "__main__":
    unittest.main()
