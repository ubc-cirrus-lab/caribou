import unittest
from unittest.mock import MagicMock, Mock

import time

import numpy as np
from multi_x_serverless.routing.distribution_solver.data_type.distribution import Distribution
from multi_x_serverless.routing.distribution_solver.data_type.sample_based_distribution import SampleBasedDistribution
from multi_x_serverless.routing.distribution_solver.input.distribution_input_manager import DistributionInputManager
from multi_x_serverless.routing.distribution_solver.distribution_bfs_coarse_grained_solver import (
    DistributionBFSFineGrainedSolver,
)
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestDistributionBFSFineGrainedSolver(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.constraints = None
        self.workflow_config.start_hops = "p1:r1"
        self.workflow_config.regions_and_providers = {"providers": {"p1": None, "p2": None}}
        self.workflow_config.instances = [
            {
                "instance_name": "i1",
                "function_name": "f1",
                "succeeding_instances": [],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]

        self._random_generated_sample_size = 1000

        self.execution_matrix = [
            [
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
            ],
            [
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
            ],
            [
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
            ],
            [
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
                SampleBasedDistribution(np.random.normal(loc=10.0, scale=1.0, size=self._random_generated_sample_size)),
            ],
        ]

        self.transmission_matrix = [
            [
                SampleBasedDistribution(
                    np.random.normal(loc=0.01, scale=0.01, size=self._random_generated_sample_size)
                ),
                SampleBasedDistribution(np.random.normal(loc=1.1, scale=1.0, size=self._random_generated_sample_size)),
            ],
            [
                SampleBasedDistribution(np.random.normal(loc=1.1, scale=1.0, size=self._random_generated_sample_size)),
                SampleBasedDistribution(
                    np.random.normal(loc=0.01, scale=0.01, size=self._random_generated_sample_size)
                ),
            ],
        ]

        # Mock input manager
        self.input_manager = Mock(spec=DistributionInputManager)
        self.input_manager.get_execution_cost_carbon_runtime_distribution.side_effect = (
            lambda current_instance_index, to_region_index: (
                (self.execution_matrix[current_instance_index][to_region_index]),
                (self.execution_matrix[current_instance_index][to_region_index]),
                (self.execution_matrix[current_instance_index][to_region_index]),
            )
        )

        self.input_manager.get_transmission_cost_carbon_runtime_distribution.side_effect = (
            lambda previous_instance_index, current_instance_index, from_region_index, to_region_index: (
                (self.transmission_matrix[from_region_index][to_region_index]),
                (self.transmission_matrix[from_region_index][to_region_index]),
                (self.transmission_matrix[from_region_index][to_region_index]),
            )
            if from_region_index is not None and previous_instance_index is not None and current_instance_index != -1.0
            else (
                SampleBasedDistribution(),
                SampleBasedDistribution(),
                SampleBasedDistribution(),
            )  # Do not consider start hop
        )
        self._all_regions = ["p1:r1", "p2:r2"]

    def test_manage_memory(self):
        # Initialize a DistributionBFSFineGrainedSolver instance here
        self.solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = {0: (None, 0.0, 0.0), 1: (None, 0.0, 0.0)}
        successor_dictionary = {0: [1], 1: []}
        prerequisites_indices = [0]
        processed_node_indices = set()
        self.solver._manage_memory(deployments, successor_dictionary, prerequisites_indices, processed_node_indices)
        self.assertNotIn(0, deployments)

    def test_acquire_prerequisite_successor_dictionaries(self):
        # Initialize a DistributionBFSFineGrainedSolver instance here
        self.solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        self.solver._dag = MagicMock()
        self.solver._dag.get_prerequisites_dict.return_value = {0: [], 1: [0], 2: [1]}
        self.solver._dag.get_preceeding_dict.return_value = {0: [1], 1: [2], 2: []}
        self.solver._dag.get_leaf_nodes.return_value = [2]
        self.solver._topological_order = [0, 1, 2]
        prerequisites_dictionary, successor_dictionary = self.solver._acquire_prerequisite_successor_dictionaries()
        self.assertEqual(prerequisites_dictionary, {0: [], 1: [0], 2: [1], -1: [2]})
        self.assertEqual(successor_dictionary, {0: [1], 1: [2], 2: [-1], -1: []})

    def test_acquire_permitted_region_indices(self):
        # Initialize a DistributionBFSFineGrainedSolver instance here
        self.solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        self.solver._topological_order = [0, 1, 2]
        self.solver._get_permitted_region_indices = MagicMock()
        self.solver._get_permitted_region_indices.return_value = [0, 1]
        regions = ["region1", "region2"]
        result = self.solver._acquire_permitted_region_indices(regions)
        self.assertEqual(result, {0, 1})

    def test_simple_1_node(self):
        """
        This is the most simple test for a single node DAG.
        """
        self.workflow_config.start_hops = "p1:r1"
        self.workflow_config.regions_and_providers = {"providers": {"p1": None, "p2": None}}
        self.workflow_config.instances = [
            {
                "instance_name": "i1",
                "function_name": "f1",
                "succeeding_instances": [],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]
        self.workflow_config.constraints = None

        self.execution_matrix = [
            [SampleBasedDistribution(np.array([2.0])), SampleBasedDistribution(np.array([2.5]))],
        ]

        self.transmission_matrix = [
            [SampleBasedDistribution(np.array([0.01])), SampleBasedDistribution(np.array([0.1]))],
            [None, SampleBasedDistribution(np.array([0.02]))],
        ]

        solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        print("For the simple 1 node test:")
        start_time = time.time()  # Start the timer
        deployments = solver._distribution_solve(self._all_regions)
        end_time = time.time()  # End the timer
        print(f"\nExecution time: {end_time - start_time} seconds")  # Print the execution time

        self._print_formatted_output(deployments)

    def test_solver_simple_2_node_join(self):
        """
        This is a simple test with 4 instances, 1 parent, and 2 nodes from that parent, and a final join node.
        """
        self.workflow_config.start_hops = "p1:r1"
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
                "succeeding_instances": ["i4"],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i3",
                "function_name": "f3",
                "succeeding_instances": ["i4"],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i4",
                "function_name": "f4",
                "succeeding_instances": [],
                "preceding_instances": ["i2", "i3"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]
        # self.workflow_config.constraints = None

        # solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        # print("For the join node test:")
        # start_time = time.time()  # Start the timer
        # deployments = solver._distribution_solve(self._all_regions)
        # end_time = time.time()  # End the timer
        # print(f"\nExecution time: {end_time - start_time} seconds")  # Print the execution time

        # self._print_formatted_output(deployments)

    def _print_formatted_output(self, deployments):
        for deployment in deployments:
            print("For deployment in region:", deployment[0])
            print("WC    :", deployment[2])
            print(
                "WC_D  :",
                (
                    deployment[1][0].get_tail_latency(True),
                    deployment[1][1].get_tail_latency(True),
                    deployment[1][2].get_tail_latency(True),
                ),
            )
            print("PC    :", deployment[3])
            print(
                "PC_D  :",
                (
                    deployment[1][0].get_average(False),
                    deployment[1][1].get_average(False),
                    deployment[1][2].get_average(False),
                ),
            )
            print()


if __name__ == "__main__":
    unittest.main()
