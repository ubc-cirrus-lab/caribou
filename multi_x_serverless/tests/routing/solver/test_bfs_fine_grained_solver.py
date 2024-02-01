import unittest
from unittest.mock import Mock

from multi_x_serverless.routing.solver.bfs_fine_grained_solver import BFSFineGrainedSolver
from multi_x_serverless.routing.solver.input.input_manager import InputManager
from multi_x_serverless.routing.workflow_config import WorkflowConfig
from multi_x_serverless.routing.models.region import Region


class TestBFSFineGrainedSolver(unittest.TestCase):
    execution_matrix: list[list[int]]
    transmission_matrix: list[list[int]]

    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.constraints = None
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}

        # Say this is the value of deploying an instance at a region (row = from, col = to) # row = instance_index, column = region_index
        # For simplicity, we just say that cost just this value, co2 is this value * 2, and rt is also just this value
        self.execution_matrix = [
            [5.0, 6.0],
            [7.0, 8.0],
            [9.0, 10.0],
            [11.0, 12.0],
            [13.0, 14.0],
            [15.0, 16.0],
        ]

        # Simplify it to 1 array as it might be easier to understand (So say cos/co2/rt have same base values)
        # previous_instance_index, current_instance_index, from_region_index, to_region_index
        # Here we only consider the from to regions
        # But we just use a factor and then simply use from to of regions here
        # Say this is the value of from a region to a region (row = from, col = to)
        # For simplicity, we just say that cost just this value, co2 is this value * 2, and rt is also just this value
        self.transmission_matrix = [
            [0, 1],
            [2, 0],
        ]

        # Mock input manager
        self.input_manager = Mock(spec=InputManager)
        self.input_manager.get_execution_cost_carbon_runtime.side_effect = (
            lambda current_instance_index, to_region_index, consider_probabilistic_invocations=False: (
                (self.execution_matrix[current_instance_index][to_region_index]),
                (self.execution_matrix[current_instance_index][to_region_index] * 2),
                (self.execution_matrix[current_instance_index][to_region_index]),
            )
        )

        self.input_manager.get_transmission_cost_carbon_runtime.side_effect = (
            lambda previous_instance_index, current_instance_index, from_region_index, to_region_index, consider_probabilistic_invocations=False: (
                (self.transmission_matrix[from_region_index][to_region_index]),
                (self.transmission_matrix[from_region_index][to_region_index] * 2),
                (self.transmission_matrix[from_region_index][to_region_index]),
            )
            if from_region_index is not None and previous_instance_index is not None
            else (0, 0, 0)  # Do not consider start hop
        )
        self._all_regions = [
            {"provider": "p1", "region": "r1"},
            {"provider": "p2", "region": "r2"},
        ]

    def test_find_common_elements(self):
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
        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, input_manager=self.input_manager)

        # Test case 1: empty list_of_sets
        result = solver._find_common_elements([])
        self.assertEqual(result, set())

        # Test case 2: non-empty list_of_sets
        result = solver._find_common_elements([{1, 2, 3}, {2, 3, 4}, {3, 4, 5}])
        self.assertEqual(result, {3})

    def test_calculate_wc_pc_cost_carbon_cl_placements(self):
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
        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        # Test case 1: empty instance_placement_data
        result = solver._calculate_wc_pc_cost_carbon_cl_placements({})
        self.assertEqual(result, (0.0, 0.0, 0.0, 0.0, {}))

        # Test case 2: non-empty instance_placement_data
        instance_placement_data = {
            1: (0, 10.0, 20.0, 5.0, 15.0),
            2: (1, 15.0, 25.0, 8.0, 18.0),
            3: (2, 20.0, 30.0, 10.0, 20.0),
        }
        result = solver._calculate_wc_pc_cost_carbon_cl_placements(instance_placement_data)
        expected_result = (45.0, 75.0, 23.0, 53.0, {1: 0, 2: 1, 3: 2})
        self.assertEqual(result, expected_result)

    def test_solver_simple_1_node(self):
        """
        This is the most simple test for a single node DAG.
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [({0: 0}, 5.0, 5.0, 10.0), ({0: 1}, 7.0, 7.0, 14.0)]  # Verified

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_simple_2_node_line(self):
        """
        This is a simple test with 2 instances, all in a straight line.
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]
        self.workflow_config.constraints = None

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0}, 12.0, 12.0, 24.0),  # Verified
            ({0: 1, 1: 0}, 16.0, 16.0, 32.0),  # Verified
            ({0: 0, 1: 1}, 14.0, 14.0, 28.0),  # Verified
            ({0: 1, 1: 1}, 15.0, 15.0, 30.0),  # Verified
        ]

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_simple_3_node_line(self):
        """
        This is a simple test with 3 instances, all in a straight line.
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
                    "providers": {"p1": None},
                },
            },
        ]
        self.workflow_config.constraints = None

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0, 2: 0}, 21.0, 21.0, 42.0),  # Verified
            ({0: 1, 1: 0, 2: 0}, 25.0, 25.0, 50.0),  # Verified
            ({0: 0, 1: 1, 2: 0}, 25.0, 25.0, 50.0),  # Verified
            ({0: 1, 1: 1, 2: 0}, 26.0, 26.0, 52.0),  # Verified
        ]

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_simple_2_node_split(self):
        """
        This is a simple test with 3 instances, 1 parent, and 2 leaf nodes
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
                },
            },
            {
                "instance_name": "i3",
                "function_name": "f3",
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None},
                },
            },
        ]
        self.workflow_config.constraints = None

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0, 2: 0}, 21.0, 14.0, 42.0),  # Verified
            ({0: 0, 1: 1, 2: 0}, 23.0, 14.0, 46.0),  # Verified
            ({0: 1, 1: 0, 2: 0}, 27.0, 18.0, 54.0),  # Verified
            ({0: 1, 1: 1, 2: 0}, 26.0, 18.0, 52.0),  # Verified
        ]

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_simple_3_node_split(self):
        """
        This is a simple test with 4 instances, 1 parent, and 3 leaf nodes
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
                    "providers": {
                        "p1": None,
                    },
                },
            },
            {
                "instance_name": "i3",
                "function_name": "f3",
                "succeeding_instances": [],
                "preceding_instances": ["i1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {
                        "p1": None,
                    },
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
        self.workflow_config.constraints = None

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0, 2: 0, 3: 0}, 32.0, 16.0, 64.0),  # Verified
            ({0: 0, 1: 0, 2: 0, 3: 1}, 34.0, 18.0, 68.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 0}, 40.0, 20.0, 80.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 1}, 39.0, 19.0, 78.0),  # Verified
        ]

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_simple_2_node_join(self):
        """
        This is a simple test with 4 instances, 1 parent, and 2 nodes from that parent, and a final join node.
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
        self.workflow_config.constraints = None

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0, 2: 0, 3: 0}, 32.0, 25.0, 64.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 0}, 38.0, 29.0, 76.0),  # Verified
            ({0: 0, 1: 0, 2: 0, 3: 1}, 35.0, 27.0, 70.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 1}, 41.0, 31.0, 82.0),  # Verified
        ]

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_complex_2_leaf(self):
        """
        This is a test with 5 instances, there are 2 leafs, one of which is a direct join node.
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
                "succeeding_instances": ["i4", "i5"],
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
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i5",
                "function_name": "f5",
                "succeeding_instances": [],
                "preceding_instances": ["i3"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]
        self.workflow_config.constraints = None

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0, 2: 0, 3: 0, 4: 0}, 45.0, 27.0, 90.0),  # Verified
            ({0: 0, 1: 0, 2: 0, 3: 0, 4: 1}, 47.0, 29.0, 94.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 0, 4: 0}, 51.0, 31.0, 102.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 0, 4: 1}, 53.0, 33.0, 106.0),  # Verified
        ]

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_complex_final_merge(self):
        """
        This is a test with 6 instances, there are 1 final end node.
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
                "succeeding_instances": ["i4", "i5"],
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
                "succeeding_instances": ["i6"],
                "preceding_instances": ["i2", "i3"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i5",
                "function_name": "f5",
                "succeeding_instances": ["i6"],
                "preceding_instances": ["i3"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i6",
                "function_name": "f6",
                "succeeding_instances": [],
                "preceding_instances": ["i4", "i5"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]
        self.workflow_config.constraints = None

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}, 60.0, 42.0, 120.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}, 66.0, 46.0, 132.0),  # Verified
            ({0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 1}, 63.0, 44.0, 126.0),  # Verified
            ({0: 1, 1: 0, 2: 0, 3: 0, 4: 0, 5: 1}, 69.0, 48.0, 138.0),  # Verified
        ]

        self.assertTrue(all(deployment in expected_deployments for deployment in deployments))

    def test_solver_hard_constraints_complex_final_merge(self):
        """
        This is a test with 6 instances, there are 1 final end node.
        This is the mofified version of the previous "test_solver_complex_final_merge" test, but with hard constraints.
        """
        self.workflow_config.start_hops = {"provider": "p1", "region": "r1"}
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
                "succeeding_instances": ["i4", "i5"],
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
                "succeeding_instances": ["i6"],
                "preceding_instances": ["i2", "i3"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i5",
                "function_name": "f5",
                "succeeding_instances": ["i6"],
                "preceding_instances": ["i3"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None},
                },
            },
            {
                "instance_name": "i6",
                "function_name": "f6",
                "succeeding_instances": [],
                "preceding_instances": ["i4", "i5"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {"p1": None, "p2": None},
                },
            },
        ]
        self.workflow_config.constraints = {
            "hard_resource_constraints": {
                "cost": {"value": 67.0},
                "runtime": {"value": 43.0},
                "carbon": {"value": 131.0},
            }
        }

        solver = BFSFineGrainedSolver(self.workflow_config, self._all_regions, self.input_manager)

        deployments = solver._solve(self._all_regions)

        # This is the expected deployments
        expected_deployments = [
            ({0: 0, 1: 0, 2: 0, 3: 0, 4: 0, 5: 0}, 60.0, 42.0, 120.0),  # Verified
        ]

        self.assertEqual(deployments, expected_deployments)


if __name__ == "__main__":
    unittest.main()
