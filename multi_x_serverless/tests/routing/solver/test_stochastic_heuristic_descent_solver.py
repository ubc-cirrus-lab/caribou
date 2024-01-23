import unittest

import numpy as np

from unittest.mock import Mock, patch
from multi_x_serverless.routing.solver.stochastic_heuristic_descent_solver import StochasticHeuristicDescentSolver
from multi_x_serverless.routing.workflow_config import WorkflowConfig


class TestStochasticHeuristicDescentSolver(unittest.TestCase):
    def setUp(self):
        self.workflow_config = Mock(spec=WorkflowConfig)
        self.workflow_config.instances = [
            {
                "instance_name": "node1",
                "succeeding_instances": ["node2"],
                "preceding_instances": [],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {
                        "aws": {
                            "config": {
                                "timeout": 60,
                                "memory": 128,
                            },
                        },
                    },
                },
            },
            {
                "instance_name": "node2",
                "succeeding_instances": [],
                "preceding_instances": ["node1"],
                "regions_and_providers": {
                    "allowed_regions": None,
                    "disallowed_regions": None,
                    "providers": {
                        "aws": {
                            "config": {
                                "timeout": 60,
                                "memory": 128,
                            },
                        },
                    },
                },
            },
        ]
        self.workflow_config.regions_and_providers = {
            "allowed_regions": None,
            "disallowed_regions": None,
            "providers": {
                "aws": {
                    "config": {
                        "timeout": 60,
                        "memory": 128,
                    },
                },
            },
        }
        self.workflow_config.workflow_id = "workflow_id"

    def test_record_successful_change(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        solver._record_successful_change(1)
        assert 1 in solver._positive_regions
        assert len(solver._positive_regions) == 1

        solver._record_successful_change(1)
        assert len(solver._positive_regions) == 1

        solver._record_successful_change(2)
        assert 2 in solver._positive_regions
        assert len(solver._positive_regions) == 2

    def test_most_expensive_path(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        solver._topological_order = [0, 1, 2]
        edge_weights = np.array([[0, 1, 0], [0, 0, 1], [0, 0, 0]])
        node_weights = np.array([1, 2, 3])
        max_cost = solver._most_expensive_path(edge_weights, node_weights)
        assert max_cost == 8

    def test_is_improvement(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        deployment = (
            {0: 0},
            (1.0, 2.0),
            (5.0, 4.0),
            (2.0, 11.0),
            (np.array([1.0]), np.array([1.0])),
            (np.array([1.0]), np.array([1.0])),
        )
        selected_instance = 0
        new_region = 1
        with patch.object(
            StochasticHeuristicDescentSolver,
            "_calculate_updated_costs_of_deployment",
            return_value=((2, 3), (6, 5), (3, 12)),
        ):
            result = solver._is_improvement(deployment, selected_instance, new_region)

            expected_result = (
                False,
                (2.0, 3.0),
                (6.0, 5.0),
                (3.0, 12.0),
                (np.array([1.0]), np.array([1.0])),
                (np.array([1.0]), np.array([1.0])),
            )
            assert result == expected_result

        with patch.object(
            StochasticHeuristicDescentSolver,
            "_calculate_updated_costs_of_deployment",
            return_value=((2, 1), (6, 5), (3, 12)),
        ):
            result = solver._is_improvement(deployment, selected_instance, new_region)

            expected_result = (
                False,
                (2.0, 1.0),
                (6.0, 5.0),
                (3.0, 12.0),
                (np.array([1.0]), np.array([1.0])),
                (np.array([1.0]), np.array([1.0])),
            )
            assert result == expected_result

        with patch.object(
            StochasticHeuristicDescentSolver,
            "_calculate_updated_costs_of_deployment",
            return_value=((2, 1), (1, 5), (3, 12)),
        ):
            result = solver._is_improvement(deployment, selected_instance, new_region)

            expected_result = (
                True,
                (2.0, 1.0),
                (1.0, 5.0),
                (3.0, 12.0),
                (np.array([1.0]), np.array([1.0])),
                (np.array([1.0]), np.array([1.0])),
            )
            assert result == expected_result

    def test_calculate_updated_costs_of_deployment(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        solver._adjacency_indexes = (np.array([0, 1, 0]), np.array([1, 0, 0]))

        previous_deployment = (
            {0: 0, 1: 1, 2: 2},
            (1.0, 1.0),
            (1.0, 1.0),
            (1.0, 1.0),
            (
                np.ones((3, 3)),
                np.ones((3, 3)),
            ),
            (
                np.ones((3, 3, 3)),
                np.ones((3, 3, 3)),
            ),
        )
        selected_instance = 0
        new_region = 1
        new_average_node_weights = previous_deployment[4][0].copy()
        new_tail_node_weights = previous_deployment[4][1].copy()
        new_average_edge_weights = previous_deployment[5][0].copy()
        new_tail_edge_weights = previous_deployment[5][1].copy()

        with patch.object(solver._input_manager, "get_execution_cost_carbon_runtime", return_value=(2.0, 3.0, 4.0)):
            with patch.object(
                solver._input_manager, "get_transmission_cost_carbon_runtime", return_value=(5.0, 6.0, 7.0)
            ):
                with patch.object(
                    solver, "_calculate_cost_of_deployment", return_value=((8.0, 9.0), (10.0, 11.0), (12.0, 13.0))
                ):
                    result = solver._calculate_updated_costs_of_deployment(
                        previous_deployment,
                        selected_instance,
                        new_region,
                        new_average_node_weights,
                        new_tail_node_weights,
                        new_average_edge_weights,
                        new_tail_edge_weights,
                    )

        expected_result = (
            (8.0, 9.0),
            (10.0, 11.0),
            (12.0, 13.0),
        )
        assert result == expected_result
        assert np.all(new_average_node_weights[0] == np.array([2.0, 1.0, 1.0]))
        assert np.all(new_tail_node_weights[0] == np.array([2.0, 1.0, 1.0]))
        assert np.all(new_average_edge_weights[0] == np.array([[5.0, 5.0, 1.0], [5.0, 1.0, 1.0], [1.0, 1.0, 1.0]]))
        assert np.all(new_tail_edge_weights[0] == np.array([[5.0, 5.0, 1.0], [5.0, 1.0, 1.0], [1.0, 1.0, 1.0]]))

        assert np.all(new_average_node_weights[1] == np.array([4.0, 1.0, 1.0]))
        assert np.all(new_tail_node_weights[1] == np.array([4.0, 1.0, 1.0]))
        assert np.all(new_average_edge_weights[1] == np.array([[7.0, 7.0, 1.0], [7.0, 1.0, 1.0], [1.0, 1.0, 1.0]]))
        assert np.all(new_tail_edge_weights[1] == np.array([[7.0, 7.0, 1.0], [7.0, 1.0, 1.0], [1.0, 1.0, 1.0]]))

        assert np.all(new_average_node_weights[2] == np.array([3.0, 1.0, 1.0]))
        assert np.all(new_tail_node_weights[2] == np.array([3.0, 1.0, 1.0]))
        assert np.all(new_average_edge_weights[2] == np.array([[6.0, 6.0, 1.0], [6.0, 1.0, 1.0], [1.0, 1.0, 1.0]]))
        assert np.all(new_tail_edge_weights[2] == np.array([[6.0, 6.0, 1.0], [6.0, 1.0, 1.0], [1.0, 1.0, 1.0]]))

    def test_select_random_instance_and_region(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        solver._bias_probability = 0.5
        solver._positive_regions = [1]
        solver._region_indexer = Mock()
        solver._region_indexer.get_value_indices.return_value = {
            ("provider1", "region1"): 0,
            ("provider2", "region2"): 1,
        }

        previous_deployment = {0: 0, 1: 1}
        regions = [{"provider": "provider1", "region": "region1"}, {"provider": "provider2", "region": "region2"}]

        with patch("random.choice", side_effect=[0, 1]):
            with patch("random.random", return_value=0.4):
                with patch.object(solver, "_filter_regions_instance", return_value=regions):
                    instance, new_region = solver.select_random_instance_and_region(previous_deployment, regions)

        assert instance == 0
        assert new_region == 1

        with patch("random.choice", side_effect=[0, 1]):
            with patch("random.random", return_value=0.6):
                with patch.object(solver, "_filter_regions_instance", return_value=regions):
                    instance, new_region = solver.select_random_instance_and_region(previous_deployment, regions)

        assert instance == 0
        assert new_region == 0 or new_region == 1

    def test_calculate_cost_of_deployment_case(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        node_weights = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
        edge_weights = np.array(
            [
                [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]],
                [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 16.0, 12.0]],
                [[1.0, 2.0, 3.0], [4.0, 5.0, 4.0], [7.0, 34.0, 1.0]],
            ]
        )

        with patch.object(solver, "_most_expensive_path", return_value=150.0):
            cost, runtime, carbon = solver._calculate_cost_of_deployment_case(node_weights, edge_weights)

        assert cost == 51.0
        assert runtime == 150.0
        assert carbon == 85.0

    def test_calculate_cost_of_deployment(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        average_node_weights = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
        tail_node_weights = np.array([[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]])
        average_edge_weights = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [7.0, 8.0, 9.0]])
        tail_edge_weights = np.array([[10.0, 20.0, 30.0], [40.0, 50.0, 60.0], [70.0, 80.0, 90.0]])

        with patch.object(
            solver, "_calculate_cost_of_deployment_case", side_effect=[(66.0, 150.0, 366.0), (660.0, 1500.0, 3660.0)]
        ):
            (
                (average_cost, tail_cost),
                (average_runtime, tail_runtime),
                (average_carbon, tail_carbon),
            ) = solver._calculate_cost_of_deployment(
                average_node_weights, tail_node_weights, average_edge_weights, tail_edge_weights
            )

        assert average_cost == 66.0
        assert average_runtime == 150.0
        assert average_carbon == 366.0

        assert tail_cost == 660.0
        assert tail_runtime == 1500.0
        assert tail_carbon == 3660.0

    def test_init_deployment(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config, [{"provider": "p1", "region": "r1"}])

        solver._workflow_config = Mock()
        solver._workflow_config.start_hops = {"provider": "p1", "region": "r1"}
        solver._workflow_config.instances = [{"instance_name": "instance1"}, {"instance_name": "instance2"}]
        solver._region_indexer = Mock()
        solver._region_indexer.value_to_index.side_effect = [0, 1]
        solver._topological_order = ["instance1", "instance2"]
        solver._dag = Mock()
        solver._dag.value_to_index.side_effect = [0, 1]
        solver._input_manager = Mock()
        solver._input_manager.get_execution_cost_carbon_runtime.side_effect = [
            (1.0, 2.0, 3.0),
            (4.0, 5.0, 6.0),
            (7.0, 8.0, 9.0),
            (10.0, 11.0, 12.0),
            (13.0, 14.0, 15.0),
            (16.0, 17.0, 18.0),
        ]
        solver._input_manager.get_transmission_cost_carbon_runtime.side_effect = [
            (13.0, 14.0, 15.0),
            (16.0, 17.0, 18.0),
            (19.0, 20.0, 21.0),
            (22.0, 23.0, 24.0),
        ]
        solver._adjacency_indexes = ([0, 1], [1, 0])
        with patch.object(
            solver, "_calculate_cost_of_deployment", return_value=((19.0, 20.0), (21.0, 22.0), (23.0, 24.0))
        ):
            result = solver._init_deployment()

        assert result[0] == {0: 0, 1: 0}
        assert result[1] == (19.0, 20.0)
        assert result[2] == (21.0, 22.0)
        assert result[3] == (23.0, 24.0)
        assert np.array_equal(result[4][0], np.array([[4.0, 10.0], [6.0, 12.0], [5.0, 11.0]]))
        assert np.array_equal(result[4][1], np.array([[1.0, 7.0], [3.0, 9.0], [2.0, 8.0]]))
        assert np.array_equal(
            result[5][0], np.array([[[0.0, 16.0], [22.0, 0.0]], [[0.0, 18.0], [24.0, 0.0]], [[0.0, 17.0], [23.0, 0.0]]])
        )
        assert np.array_equal(
            result[5][1], np.array([[[0.0, 13.0], [19.0, 0.0]], [[0.0, 15.0], [21.0, 0.0]], [[0.0, 14.0], [20.0, 0.0]]])
        )

    def test_solve(self):
        solver = StochasticHeuristicDescentSolver(self.workflow_config)
        solver._max_iterations = 5
        solver._learning_rate = 0.01
        solver._workflow_config = Mock()
        solver._workflow_config.constraints = {"constraint1": "value1"}
        solver._topological_order = ["instance1", "instance2"]
        solver._adjacency_indexes = ([0, 1], [1, 0])
        solver._permitted_region_indices_cache = {0: [0, 1], 1: [0, 1]}
        solver._positive_regions = set()
        solver._bias_probability = 0.5
        with patch.object(
            solver,
            "_init_deployment",
            return_value=(
                {0: 0, 1: 0},
                (1.0, 2.0),
                (3.0, 4.0),
                (5.0, 6.0),
                (np.zeros((3, 2)), np.zeros((3, 2))),
                (np.zeros((3, 2, 2)), np.zeros((3, 2, 2))),
            ),
        ):
            with patch.object(solver, "_fail_hard_resource_constraints", return_value=False):
                with patch.object(
                    solver, "select_random_instance_and_region", side_effect=[(0, 1), (1, 0), (0, 1), (1, 0), (0, 1)]
                ):
                    with patch.object(
                        solver,
                        "_is_improvement",
                        return_value=(
                            True,
                            (7.0, 8.0),
                            (9.0, 10.0),
                            (11.0, 12.0),
                            (np.ones((3, 2)), np.ones((3, 2))),
                            (np.ones((3, 2, 2)), np.ones((3, 2, 2))),
                        ),
                    ):
                        with patch.object(solver, "_record_successful_change"):
                            result = solver._solve([{"region1": "value1"}, {"region2": "value2"}])

        assert result == [({0: 0, 1: 0}, 1.0, 3.0, 5.0), ({0: 1, 1: 0}, 7.0, 9.0, 11.0)]


if __name__ == "__main__":
    unittest.main()
