import numpy as np
from unittest import TestCase
from unittest.mock import MagicMock, patch
from multi_x_serverless.global_routing.internal.solver.chalicelib.regions import get_regions, filter_regions
from multi_x_serverless.global_routing.internal.solver.chalicelib.cost import get_cost_matrix, get_egress_cost_matrix
import networkx as nx
import random


class TestSolver(TestCase):
    @patch("boto3.client")
    def test_get_regions(self, mock_client):
        # Mock the DynamoDB client
        mock_response = {
            "Items": [
                {"region_code": {"S": "us-west-2"}, "provider": {"S": "AWS"}},
                {"region_code": {"S": "eu-west-1"}, "provider": {"S": "Azure"}},
                # Add more mock items as needed
            ]
        }
        mock_client.return_value.scan.return_value = mock_response

        # Call the get_regions function
        regions = get_regions()

        # Assert the expected results
        expected_regions = np.array([("us-west-2", "AWS"), ("eu-west-1", "Azure")])
        np.testing.assert_array_equal(regions, expected_regions)

    def test_filter_regions(self):
        # Create a mock regions array
        regions = np.array([("us-west-2", "AWS"), ("eu-west-1", "Azure")])

        workflow_description = {"filtered_regions": ["Azure:eu-west-1"]}

        # Call the filter_regions function
        filtered_regions = filter_regions(regions, workflow_description)

        # Assert the expected results
        expected_filtered_regions = np.array([("us-west-2", "AWS")])
        np.testing.assert_array_equal(filtered_regions, expected_filtered_regions)

    def test_find_viable_deployment_options(self):
        # Mock the other functions of Solver
        get_cost_matrix = MagicMock(
            return_value=np.array(
                [
                    [lambda x, y, z: 1, lambda x, y, z: 2],
                    [lambda x, y, z: 3, lambda x, y, z: 4],
                    [lambda x, y, z: 5, lambda x, y, z: 6],
                ]
            )
        )
        get_egress_cost_matrix = MagicMock(
            return_value=np.array(
                [
                    [1.5, 0.6],
                    [0.7, 0.4],
                ]
            )
        )
        get_runtime_array = MagicMock(return_value=np.array([lambda x, y, z: 1.2, lambda x, y, z: 0.4]))
        get_latency_matrix = MagicMock(
            return_value=np.array(
                [
                    [1.8, 0.2],
                    [0.4, 1.4],
                ]
            )
        )
        get_execution_carbon_matrix = MagicMock(
            return_value=np.array(
                [
                    [lambda x, y, z: 1.5, lambda x, y, z: 0.6],
                    [lambda x, y, z: 0.5, lambda x, y, z: 1.8],
                    [lambda x, y, z: 0.9, lambda x, y, z: 0.2],
                ]
            )
        )
        get_transmission_carbon_matrix = MagicMock(
            return_value=np.array(
                [
                    [0.5, 0.6],
                    [0.7, 0.8],
                ]
            )
        )
        dag = nx.DiGraph()
        dag.add_node("A")
        dag.add_node("B")
        dag.add_node("C")

        dag.add_edge("A", "B")
        dag.add_edge("B", "C")
        dag.add_edge("A", "C")
        build_dag = MagicMock(return_value=dag)

        regions = np.array([("us-west-2", "AWS"), ("eu-west-1", "Azure")])

        cost_constraint = 450
        runtime_constraint = 500
        carbon_constraint = 600

        workflow_description = {
            "functions": [
                {"name": "A", "some_other_attribute": "some_other_value"},
                {"name": "B", "some_other_attribute": "some_other_value"},
                {"name": "C", "some_other_attribute": "some_other_value"},
            ],
            "start_hop": "us-west-2",
            "constraints": {
                "cost": cost_constraint,
                "runtime": runtime_constraint,
                "carbon": carbon_constraint,
            },
        }

        function_runtime_measurements = {
            "A": 1.2,
            "B": 0.9,
            "C": 0.5,
        }

        function_data_transfer_size_measurements = {
            "A": 100,
            "B": 200,
            "C": 200,
        }

        # Call the find_viable_deployment_options function
        viable_options = find_viable_deployment_options(
            regions, function_runtime_measurements, function_data_transfer_size_measurements, workflow_description
        )

        # Assert the expected results
        assert len(viable_options) == 2
        for option in viable_options:
            assert option[1] <= cost_constraint
            assert option[2] <= runtime_constraint
            assert option[3] <= carbon_constraint

    def test_find_viable_deployment_options_large_dag_many_regions(self):
        # Create an instance of the Solver class
        solver = Solver()

        number_of_regions = 10
        number_of_functions = 5

        # Mock the other functions of Solver
        solver.get_cost_matrix = MagicMock(
            return_value=np.array(
                [
                    [lambda x, y, z: random.randint(1, 10) for _ in range(number_of_regions)]
                    for _ in range(number_of_functions)
                ]
            )
        )

        solver.get_egress_cost_matrix = MagicMock(
            return_value=np.array(
                [[random.randint(1, 10) for _ in range(number_of_regions)] for _ in range(number_of_regions)]
            )
        )

        solver.get_runtime_array = MagicMock(
            return_value=np.array([lambda x, y, z: random.randint(1, 10) for _ in range(number_of_regions)])
        )

        solver.get_latency_matrix = MagicMock(
            return_value=np.array(
                [[random.randint(1, 10) for _ in range(number_of_regions)] for _ in range(number_of_regions)]
            )
        )

        solver.get_execution_carbon_matrix = MagicMock(
            return_value=np.array(
                [
                    [lambda x, y, z: random.randint(1, 10) for _ in range(number_of_regions)]
                    for _ in range(number_of_functions)
                ]
            )
        )

        solver.get_transmission_carbon_matrix = MagicMock(
            return_value=np.array(
                [[random.randint(1, 10) for _ in range(number_of_regions)] for _ in range(number_of_regions)]
            )
        )

        dag = nx.DiGraph()
        for i in range(number_of_functions):
            dag.add_node(i)

        for node in range(number_of_functions - 1):
            while True:
                next_node = random.choice(range(number_of_functions))
                if next_node != node and not nx.has_path(dag, next_node, node):
                    break
            dag.add_edge(node, next_node)

        solver.build_dag = MagicMock(return_value=dag)

        regions = np.array([(f"region_{i}", f"provider_{i}") for i in range(number_of_regions)])

        cost_constraint = 10000000000
        runtime_constraint = 10000000000
        carbon_constraint = 10000000000

        workflow_description = {
            "functions": [{"name": i, "some_other_attribute": "some_other_value"} for i in range(number_of_functions)],
            "start_hop": "region_0",
            "constraints": {
                "cost": cost_constraint,
                "runtime": runtime_constraint,
                "carbon": carbon_constraint,
            },
        }

        function_runtime_measurements = {i: random.randint(1, 10) for i in range(number_of_functions)}
        function_data_transfer_size_measurements = {i: random.randint(1, 10) for i in range(number_of_functions)}

        # Call the find_viable_deployment_options function
        viable_options = solver.find_viable_deployment_options(
            regions, function_runtime_measurements, function_data_transfer_size_measurements, workflow_description
        )

        print(len(viable_options))

        # Assert the expected results
        for option in viable_options:
            assert option[1] <= cost_constraint
            assert option[2] <= runtime_constraint
            assert option[3] <= carbon_constraint
