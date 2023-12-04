from unittest.mock import MagicMock
import numpy as np
import random
import networkx as nx
from multi_x_serverless.global_routing.internal.solver import Solver


solver = Solver()

number_of_regions = 10
number_of_functions = 5

# Mock the other functions of Solver
solver.get_cost_matrix = MagicMock(
    return_value=np.array(
        [[lambda x, y, z: random.randint(1, 10) for _ in range(number_of_regions)] for _ in range(number_of_functions)]
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
    return_value=np.array([[random.randint(1, 10) for _ in range(number_of_regions)] for _ in range(number_of_regions)])
)

solver.get_execution_carbon_matrix = MagicMock(
    return_value=np.array(
        [[lambda x, y, z: random.randint(1, 10) for _ in range(number_of_regions)] for _ in range(number_of_functions)]
    )
)

solver.get_transmission_carbon_matrix = MagicMock(
    return_value=np.array([[random.randint(1, 10) for _ in range(number_of_regions)] for _ in range(number_of_regions)])
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

cost_constraint = 300
runtime_constraint = 300
carbon_constraint = 300

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