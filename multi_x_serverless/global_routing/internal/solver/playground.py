import random
from unittest.mock import MagicMock

import networkx as nx
import numpy as np

from multi_x_serverless.global_routing.internal.solver.solver import find_viable_deployment_options

NUMBER_OF_REGIONS = 10
NUMBER_OF_FUNCTIONS = 5

# Mock the other functions of Solver
get_cost_matrix = MagicMock(
    return_value=np.array(
        [[lambda x, y, z: random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_FUNCTIONS)]
    )
)

get_egress_cost_matrix = MagicMock(
    return_value=np.array([[random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_REGIONS)])
)

get_runtime_array = MagicMock(
    return_value=np.array([lambda x, y, z: random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)])
)

get_latency_matrix = MagicMock(
    return_value=np.array([[random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_REGIONS)])
)

get_execution_carbon_matrix = MagicMock(
    return_value=np.array(
        [[lambda x, y, z: random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_FUNCTIONS)]
    )
)

get_transmission_carbon_matrix = MagicMock(
    return_value=np.array([[random.randint(1, 10) for _ in range(NUMBER_OF_REGIONS)] for _ in range(NUMBER_OF_REGIONS)])
)

dag = nx.DiGraph()
for i in range(NUMBER_OF_FUNCTIONS):
    dag.add_node(i)

for node in range(NUMBER_OF_FUNCTIONS - 1):
    while True:
        next_node = random.choice(range(NUMBER_OF_FUNCTIONS))
        if next_node != node and not nx.has_path(dag, next_node, node):
            break
    dag.add_edge(node, next_node)

build_dag = MagicMock(return_value=dag)

regions = np.array([(f"region_{i}", f"provider_{i}") for i in range(NUMBER_OF_REGIONS)])

COST_CONSTRAINT = 300
RUNTIME_CONSTRAINT = 300
CARBON_CONSTRAINT = 300

workflow_description = {
    "functions": [{"name": i, "some_other_attribute": "some_other_value"} for i in range(NUMBER_OF_FUNCTIONS)],
    "start_hop": "region_0",
    "constraints": {
        "cost": COST_CONSTRAINT,
        "runtime": RUNTIME_CONSTRAINT,
        "carbon": CARBON_CONSTRAINT,
    },
}

function_runtime_measurements = {i: random.randint(1, 10) for i in range(NUMBER_OF_FUNCTIONS)}
function_data_transfer_size_measurements = {i: random.randint(1, 10) for i in range(NUMBER_OF_FUNCTIONS)}

# Call the find_viable_deployment_options function
viable_options = find_viable_deployment_options(
    regions, function_runtime_measurements, function_data_transfer_size_measurements, workflow_description
)

print(len(viable_options))

# Assert the expected results
for option in viable_options:
    assert option[1] <= COST_CONSTRAINT
    assert option[2] <= RUNTIME_CONSTRAINT
    assert option[3] <= CARBON_CONSTRAINT
