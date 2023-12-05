from unittest.mock import patch
from multi_x_serverless.global_routing.internal.solver.solver import find_viable_deployment_options
import networkx as nx
import random

NUMBER_OF_REGIONS = 5
NUMBER_OF_FUNCTIONS = 3


@patch("multi_x_serverless.global_routing.internal.solver.solver.get_dag")
@patch("multi_x_serverless.global_routing.internal.solver.solver.get_transmission_carbon_matrix")
@patch("multi_x_serverless.global_routing.internal.solver.solver.get_execution_carbon_matrix")
@patch("multi_x_serverless.global_routing.internal.solver.solver.get_latency_matrix")
@patch("multi_x_serverless.global_routing.internal.solver.solver.get_runtime_array")
@patch("multi_x_serverless.global_routing.internal.solver.solver.get_egress_cost_matrix")
@patch("multi_x_serverless.global_routing.internal.solver.solver.get_cost_matrix")
def test_find_viable_deployment_options_large_dag_many_regions(
    get_cost_matrix,
    get_egress_cost_matrix,
    get_runtime_array,
    get_latency_matrix,
    get_execution_carbon_matrix,
    get_transmission_carbon_matrix,
    get_dag,
):
    # Mock the other functions of Solver
    get_cost_matrix.return_value = [
        [lambda x, y: random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_FUNCTIONS)
    ]

    get_egress_cost_matrix.return_value = [
        [lambda x: random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_REGIONS)
    ]

    get_runtime_array.return_value = [lambda x, y: random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)]

    get_latency_matrix.return_value = [
        [random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_REGIONS)
    ]

    get_execution_carbon_matrix.return_value = [
        [lambda x, y: random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_FUNCTIONS)
    ]

    get_transmission_carbon_matrix.return_value = [
        [random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_REGIONS)
    ]

    regions = [(f"region_{i}", f"provider_{i}") for i in range(NUMBER_OF_REGIONS)]

    dag = nx.DiGraph()
    for i in range(NUMBER_OF_FUNCTIONS):
        dag.add_node(i)

    for node in range(NUMBER_OF_FUNCTIONS - 1):
        while True:
            next_node = random.choice(range(NUMBER_OF_FUNCTIONS))
            if next_node != node and not nx.has_path(dag, next_node, node):
                break
        dag.add_edge(node, next_node)

    get_dag.return_value = dag

    cost_constraint = 10000000000
    runtime_constraint = 10000000000
    carbon_constraint = 10000000000

    workflow_description = {
        "functions": [{"name": i, "some_other_attribute": "some_other_value"} for i in range(NUMBER_OF_FUNCTIONS)],
        "start_hop": "region_0",
        "constraints": {
            "cost": cost_constraint,
            "runtime": runtime_constraint,
            "carbon": carbon_constraint,
        },
    }

    function_runtime_measurements = {i: random.randint(1, 10) for i in range(NUMBER_OF_FUNCTIONS)}
    function_data_transfer_size_measurements = {i: random.randint(1, 10) for i in range(NUMBER_OF_FUNCTIONS)}

    # Call the find_viable_deployment_options function
    viable_options = find_viable_deployment_options(
        regions, function_runtime_measurements, function_data_transfer_size_measurements, workflow_description
    )

    print(viable_options)
    assert False
    # Assert the expected results
    for option in viable_options:
        assert option[1] <= cost_constraint
        assert option[2] <= runtime_constraint
        assert option[3] <= carbon_constraint
