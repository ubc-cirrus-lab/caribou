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
from multi_x_serverless.routing.solver.input.input_manager import InputManager
from multi_x_serverless.routing.workflow_config import WorkflowConfig

from multi_x_serverless.routing.solver.coarse_grained_solver import CoarseGrainedSolver
from multi_x_serverless.routing.solver.bfs_fine_grained_solver import BFSFineGrainedSolver
from multi_x_serverless.routing.solver.stochastic_heuristic_descent_solver import StochasticHeuristicDescentSolver

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
        self._execution_matrix = [
            [(10.0, 1), (10.0, 1)],
        ]
        self._transmission_matrix = [
            [0.01, 1.1, 1],
            [1.1, 0.02, 1],
        ]
        self.distribution_execution_matrix, self.distribution_transmission_matrix = self._generate_distribution_matricies(self._execution_matrix, self._transmission_matrix)

        # Mock input manager
        self.input_manager = Mock(spec=InputManager)
        self.input_manager.get_execution_cost_carbon_runtime.side_effect = (
            lambda current_instance_index, to_region_index, consider_probabilistic_invocations=False: (
                (self._execution_matrix[current_instance_index][to_region_index][0]),
                (self._execution_matrix[current_instance_index][to_region_index][0]),
                (self._execution_matrix[current_instance_index][to_region_index][0]),
            )
        )

        self.input_manager.get_transmission_cost_carbon_runtime.side_effect = (
            lambda previous_instance_index, current_instance_index, from_region_index, to_region_index, consider_probabilistic_invocations=False: (
                (self._transmission_matrix[from_region_index][to_region_index]),
                (self._transmission_matrix[from_region_index][to_region_index]),
                (self._transmission_matrix[from_region_index][to_region_index]),
            )
            if from_region_index is not None and previous_instance_index is not None and previous_instance_index != current_instance_index
            else (0, 0, 0)  # Do not consider start hop
        )

        # Mock distribution input manager
        self.distribution_input_manager = Mock(spec=DistributionInputManager)
        self.distribution_input_manager.get_execution_cost_carbon_runtime_distribution.side_effect = (
            lambda current_instance_index, to_region_index: (
                (self.distribution_execution_matrix[current_instance_index][to_region_index]),
                (self.distribution_execution_matrix[current_instance_index][to_region_index]),
                (self.distribution_execution_matrix[current_instance_index][to_region_index]),
            )
        )

        self.distribution_input_manager.get_transmission_cost_carbon_runtime_distribution.side_effect = (
            lambda previous_instance_index, current_instance_index, from_region_index, to_region_index: (
                (self.distribution_transmission_matrix[from_region_index][to_region_index]),
                (self.distribution_transmission_matrix[from_region_index][to_region_index]),
                (self.distribution_transmission_matrix[from_region_index][to_region_index]),
            )
            if from_region_index is not None and previous_instance_index is not None and current_instance_index >= 0 and previous_instance_index >= 0
            else (
                SampleBasedDistribution(),
                SampleBasedDistribution(),
                SampleBasedDistribution(),
            )  # Do not consider start hop
        )

        self._all_regions = ["p1:r1", "p2:r2"]

    def test_manage_memory(self):
        # Initialize a DistributionBFSFineGrainedSolver instance here
        self.solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.distribution_input_manager)

        deployments = {0: (None, 0.0, 0.0), 1: (None, 0.0, 0.0)}
        successor_dictionary = {0: [1], 1: []}
        prerequisites_indices = [0]
        processed_node_indices = set()
        self.solver._manage_memory(deployments, successor_dictionary, prerequisites_indices, processed_node_indices)
        self.assertNotIn(0, deployments)

    def test_acquire_prerequisite_successor_dictionaries(self):
        # Initialize a DistributionBFSFineGrainedSolver instance here
        self.solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.distribution_input_manager)

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
        self.solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.distribution_input_manager)

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

        self.distribution_execution_matrix = [
            [SampleBasedDistribution(np.array([2.0])), SampleBasedDistribution(np.array([2.5]))],
        ]

        self.distribution_transmission_matrix = [
            [SampleBasedDistribution(np.array([0.01])), SampleBasedDistribution(np.array([0.1]))],
            [None, SampleBasedDistribution(np.array([0.02]))],
        ]

        solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.distribution_input_manager)

        deployments = solver._distribution_solve(self._all_regions)

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
                    "providers": {"p1": None, "p2": None},
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
                    "providers": {"p1": None, "p2": None},
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
        self.workflow_config.constraints = None


        self.distribution_execution_matrix = [
            [SampleBasedDistribution(np.array([1.0])), SampleBasedDistribution(np.array([1.5]))],
            [SampleBasedDistribution(np.array([2.0])), SampleBasedDistribution(np.array([2.5]))],
            [SampleBasedDistribution(np.array([3.0])), SampleBasedDistribution(np.array([3.5]))],
            [SampleBasedDistribution(np.array([4.0])), SampleBasedDistribution(np.array([4.5]))],
        ]

        self.distribution_transmission_matrix = [
            [SampleBasedDistribution(np.array([0.01])), SampleBasedDistribution(np.array([0.1]))],
            [None, SampleBasedDistribution(np.array([0.01]))],
        ]

        solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.distribution_input_manager)

        deployments = solver._distribution_solve(self._all_regions)
        # self._print_formatted_output(deployments)


    def test_solver_probabilistic_2_node_join(self):
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
                    "providers": {"p1": None, "p2": None},
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
                    "providers": {"p1": None, "p2": None},
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
        self.workflow_config.constraints = None


        self.distribution_execution_matrix = [
            [SampleBasedDistribution(np.array([1.0])), SampleBasedDistribution(np.array([1.5]))],
            [SampleBasedDistribution(np.array([0.0, 2.0])), SampleBasedDistribution(np.array([0.0, 2.5]))],
            [SampleBasedDistribution(np.array([0.0, 0.0, 0.0, 103.0])), SampleBasedDistribution(np.array([0.0, 0.0, 0.0, 103.5]))],
            [SampleBasedDistribution(np.array([4.0])), SampleBasedDistribution(np.array([4.5]))],
        ]

        self.distribution_transmission_matrix = [ # No Transmission -> Ignores complexities
            [SampleBasedDistribution(np.array([0])), SampleBasedDistribution(np.array([0]))],
            [None, SampleBasedDistribution(np.array([0]))],
        ]

        solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.distribution_input_manager)

        deployments = solver._distribution_solve(self._all_regions)

        # self._print_formatted_output(deployments)

    def test_solver_probabilistic_2_node_join_comparison(self):
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
                    "providers": {"p1": None, "p2": None},
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
                    "providers": {"p1": None, "p2": None},
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
        self.workflow_config.constraints = None

        # Execution Matrix (Runtime, Probability)
        self._execution_matrix = [
            [(1.0, 1.0), (1.5, 1.0)],
            [(2.0, 1.0), (2.5, 1.0)],
            [(3.0, 1.0), (3.5, 1.0)],
            [(4.0, 1.0), (4.5, 1.0)],
        ]
        self._transmission_matrix = [
            [0.0, 0.0],
            [0.0, 0.0],
        ]
        self.distribution_execution_matrix, self.distribution_transmission_matrix = self._generate_distribution_matricies(self._execution_matrix, self._transmission_matrix, False)

        original_solver = CoarseGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)
        original_bfs_solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)
        original_sgd_solver = StochasticHeuristicDescentSolver(self.workflow_config, self._all_regions, self.input_manager)
        solver = DistributionBFSFineGrainedSolver(self.workflow_config, self._all_regions, self.distribution_input_manager)

        limited_regions = self._all_regions
        # limited_regions = ["p1:r1"]

        print("For the complex join node test:")
        start_time = time.time()  # Start the timer
        original_solver_deployments = original_solver._solve(limited_regions)
        end_time = time.time()  # End the timer
        print(f"Original Solver Execution time: {end_time - start_time} seconds")  # Print the execution time


        start_time = time.time()  # Start the timer
        deployments = solver._distribution_solve(limited_regions)
        end_time = time.time()  # End the timer
        print(f"Distributed Solver Execution time: {end_time - start_time} seconds")  # Print the execution time

        start_time = time.time()  # Start the timer
        original_bfs_solver_deployments = original_bfs_solver._solve(limited_regions)
        end_time = time.time()  # End the timer
        print(f"Original BFS Solver Execution time: {end_time - start_time} seconds")  # Print the execution time

        start_time = time.time()  # Start the timer
        original_sgd_solver_deployments = original_sgd_solver._solve(limited_regions)
        end_time = time.time()  # End the timer
        print(f"Original SGD Solver Execution time: {end_time - start_time} seconds")  # Print the execution time

        print(original_solver_deployments)

        self._print_formatted_output(deployments)

        print(original_bfs_solver_deployments)

        print(original_sgd_solver_deployments)

    def _generate_distribution_matricies(self, execution_matrix, transmission_matrix, enable_variation = True):
        distribution_execution_matrix = []
        distribution_transmission_matrix = []

        # For execution
        for i in range(len(execution_matrix)):
            distribution_execution_matrix.append([])
            for j in range(len(execution_matrix[i])):
                execution_value = execution_matrix[i][j][0]
                execution_prob = execution_matrix[i][j][1]

                # Now we can simply get a fraction of the execution value as the std
                # This is a very simple way to generate a distribution
                std = 0
                if (enable_variation):
                    std = execution_value / 10

                if (execution_value <= 0):
                    distribution_execution_matrix[i].append(SampleBasedDistribution())
                else:
                    distribution_execution_matrix[i].append(SampleBasedDistribution(np.random.normal(loc=execution_value, scale=std, size=self._random_generated_sample_size)))

        # For transmission
        for i in range(len(transmission_matrix)):
            distribution_transmission_matrix.append([])
            for j in range(len(transmission_matrix[i])):
                transmission_value = transmission_matrix[i][j]

                # Now we can simply get a fraction of the transmission value as the std
                # This is a very simple way to generate a distribution
                std = 0
                if (enable_variation):
                    std = transmission_value / 10

                if (transmission_value <= 0):
                    distribution_transmission_matrix[i].append(SampleBasedDistribution())
                else: 
                    distribution_transmission_matrix[i].append(SampleBasedDistribution(np.random.normal(loc=transmission_value, scale=std, size=self._random_generated_sample_size)))

        return distribution_execution_matrix, distribution_transmission_matrix

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
