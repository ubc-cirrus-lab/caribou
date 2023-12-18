import random
from unittest.mock import MagicMock, patch

import networkx as nx
import numpy as np

from multi_x_serverless.global_routing.internal.solver.chalicelib.regions import get_regions
from multi_x_serverless.global_routing.internal.solver.chalicelib.utils import DEFAULT_REGION, OPT_IN_REGIONS
from multi_x_serverless.global_routing.internal.solver.solver import find_viable_deployment_options

# NUMBER_OF_REGIONS = 10
# NUMBER_OF_FUNCTIONS = 5
NUMBER_OF_REGIONS = 5
NUMBER_OF_FUNCTIONS = 2

COST_CONSTRAINT = 100000
RUNTIME_CONSTRAINT = 100000
CARBON_CONSTRAINT = 100000

workflow_description = {
    "functions": [
        {
            "name": i,
            "resource_request": {
                "vCPU": 1,
                "memory": 128,
                "architecture": "x86_64",
            },
            "estimated_invocations_per_month": 100000,
        }
        for i in range(NUMBER_OF_FUNCTIONS)
    ],
    "start_hop": DEFAULT_REGION,
    "constraints": {
        "cost": COST_CONSTRAINT,
        "runtime": RUNTIME_CONSTRAINT,
        "carbon": CARBON_CONSTRAINT,
    },
}

function_runtime_measurements = {i: [random.randint(1, 10)] for i in range(NUMBER_OF_FUNCTIONS)}
function_data_transfer_size_measurements = {i: random.randint(1, 10) for i in range(NUMBER_OF_FUNCTIONS)}


print("Starting to find viable deployment options")


# Call the find_viable_deployment_options function
@patch("multi_x_serverless.global_routing.internal.solver.solver.get_dag")
def test_find_viable_deployment_options(mock_get_dag: MagicMock) -> None:
    dag = nx.DiGraph()
    for i in range(NUMBER_OF_FUNCTIONS):
        dag.add_node(i)

    for node in range(NUMBER_OF_FUNCTIONS - 1):
        while True:
            next_node = random.choice(range(NUMBER_OF_FUNCTIONS))
            if next_node != node and not nx.has_path(dag, next_node, node):
                break
        dag.add_edge(node, next_node)

    mock_get_dag.return_value = dag

    regions = get_regions()
    regions = [region for region in regions if region[0] not in OPT_IN_REGIONS]
    regions = regions[:NUMBER_OF_REGIONS]
    workflow_description["start_hop"] = regions[0][0]

    viable_options = find_viable_deployment_options(
        regions, function_runtime_measurements, function_data_transfer_size_measurements, workflow_description
    )

    # Assert the expected results
    for option in viable_options:
        assert option[1] <= COST_CONSTRAINT
        assert option[2] <= RUNTIME_CONSTRAINT
        assert option[3] <= CARBON_CONSTRAINT


test_find_viable_deployment_options()
