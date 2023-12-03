import boto3
import numpy as np
import networkx as nx

AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
DEFAULT_REGION = "us-west-2"


def get_regions() -> np.ndarray:
    client = boto3.client(
        "dynamodb",
        region_name=DEFAULT_REGION,
    )

    response = client.scan(
        TableName=AWS_DATACENTER_INFO_TABLE_NAME,
    )

    results = np.array([item["region_code"]["S"] for item in response["Items"]])
    return results


def filter_regions(regions: np.ndarray, workflow_description: dict) -> np.ndarray:
    if "filtered_regions" in workflow_description:
        return np.array([region for region in regions if region not in workflow_description["filtered_regions"]])
    else:
        return regions


def get_cost_for_region_function(region: str, function: dict) -> callable:
    return lambda function, function_runtime_measurements: 0.0


def get_cost_matrix(regions: np.ndarray, functions: np.ndarray) -> np.ndarray:
    cost_matrix = np.zeros((len(functions), len(regions)))
    for i in range(len(functions)):
        for j in range(len(regions)):
            cost_matrix[i][j] = get_cost_for_region_function(regions[j], functions[i])


def get_runtime_for_region_function(region: str, destination_region: str) -> callable:
    return lambda function, function_runtime_measurements: 0.0


def get_runtime_array(regions: np.ndarray) -> np.ndarray:
    latency_matrix = np.zeros(len(regions))
    for i in range(len(regions)):
        latency_matrix[i] = get_runtime_coefficient_for_region(regions[i])


def get_latency_coefficient_for_region(region: str, destination_region: str) -> float:
    return 0.0


def get_latency_matrix(regions: np.ndarray) -> np.ndarray:
    latency_matrix = np.zeros((len(regions), len(regions)))
    for i in range(len(regions)):
        for j in range(i, len(regions)):
            latency_matrix[i][j] = get_latency_coefficient_for_region(regions[i], regions[j])
            latency_matrix[j][i] = latency_matrix[i][j]
    return latency_matrix


def get_execution_carbon_for_region_function(region: str, function: dict) -> callable:
    return lambda function, function_runtime_measurements: 0.0


def get_execution_carbon_matrix(regions: np.ndarray, functions: np.ndarray) -> np.ndarray:
    execution_carbon_matrix = np.zeros((len(functions), len(regions)))
    for i in range(len(functions)):
        for j in range(len(regions)):
            execution_carbon_matrix[i][j] = get_execution_carbon_for_region_function(regions[j], functions[i])


def get_transmission_carbon_coefficient_for_region_and_destination_region(
    region: str, destination_region: str
) -> callable:
    return 0.0


def get_transmission_carbon_matrix(regions: np.ndarray) -> np.ndarray:
    transmission_carbon_matrix = np.zeros((len(regions), len(regions)))
    for i in range(len(regions)):
        for j in range(i, len(regions)):
            transmission_carbon_matrix[i][j] = get_transmission_carbon_coefficient_for_region_and_destination_region(
                regions[i], regions[j]
            )
            transmission_carbon_matrix[j][i] = transmission_carbon_matrix[i][j]


def get_egress_cost(region: str, destination_region: str, transmission_size: float) -> float:
    return 0.0


def build_dag(workflow_description: dict) -> tuple[nx.DiGraph, dict]:
    dag = nx.DiGraph()
    function_name_to_spec = {}
    for function in workflow_description["functions"]:
        dag.add_node(function["name"])
        function_name_to_spec[function["name"]] = function
        for next_function in function["next_functions"]:
            dag.add_edge(function["name"], next_function["name"])
    return dag, function_name_to_spec


def find_viable_deployment_options(
    regions: np.ndarray,
    function_runtime_measurements: dict,
    function_data_transfer_size_measurements: dict,
    workflow_description: dict,
) -> np.ndarray:
    # We are going to go through
    # 1. Get the cost matrix
    # 2. Get the latency matrix
    # 3. Get the execution carbon matrix
    # 4. Get the transmission carbon matrix

    # Then we are going to go start building the viable deployment options starting from the first function and the first region and then going through the DAG of the workflow
    # Every time we reach the last function we add it to the list of viable deployment options
    # We also keep track of the cost, runtime and carbon of the deployment option
    # Every time any of these metrics is breaking the constraints given in the workflow description we stop building the deployment option and move on to the next one

    region_to_index = {region: i for i, region in enumerate(regions)}
    function_to_spec = {function["name"]: function for function in workflow_description["functions"]}

    dag, function_name_to_spec = build_dag(workflow_description)

    initial_start_hop_region = workflow_description["start_hop"]

    successors_of_first_hop = [node for node, in_degree in dag.in_degree() if in_degree == 0]

    dag.add_node(initial_start_hop_region)
    for initial_node in successors_of_first_hop:
        dag.add_edge(initial_start_hop_region, initial_node)

    sorted_functions = nx.topological_sort(dag)

    cost_matrix: np.ndarray = get_cost_matrix(regions, sorted_functions)
    latency_matrix: np.ndarray = get_latency_matrix(regions)
    runtime_array: np.ndarray = get_runtime_array(regions)
    execution_carbon_matrix: np.ndarray = get_execution_carbon_matrix(regions, sorted_functions)
    transmission_carbon_matrix: np.ndarray = get_transmission_carbon_matrix(regions)

    deployment_options = np.array([({initial_start_hop_region: initial_start_hop_region}, 0, 0, 0)])

    for i, function in enumerate(sorted_functions[1:]):
        new_deployment_options = []
        current_index = i + 1
        for region in regions:
            cost_of_function_in_region = cost_matrix[current_index][region_to_index[region]](
                function_to_spec[function], function_runtime_measurements[function]
            )
            runtime_of_function_in_region = runtime_array[region_to_index[region]](
                function_to_spec[function], function_runtime_measurements[function]
            )
            execution_carbon_of_function_in_region = execution_carbon_matrix[current_index][region_to_index[region]](
                function_to_spec[function], function_runtime_measurements[function]
            )

            for deployment_option in deployment_options:
                new_transmission_carbon = 0.0
                new_transmission_cost = 0.0
                new_transmission_latency = 0.0

                for predecessor in dag.predecessors(function):
                    new_transmission_carbon += (
                        transmission_carbon_matrix[region_to_index[deployment_option[0][predecessor]]][
                            region_to_index[region]
                        ]
                        * function_data_transfer_size_measurements[function]
                    )
                    new_transmission_cost += get_egress_cost(
                        deployment_option[0][predecessor], region, function_data_transfer_size_measurements[function]
                    )
                    new_transmission_latency += latency_matrix[region_to_index[deployment_option[0][predecessor]]][
                        region_to_index[region]
                    ]

                if (
                    deployment_option[1] + cost_of_function_in_region + new_transmission_cost
                    > workflow_description["constraints"]["cost"]
                    or deployment_option[2] + runtime_of_function_in_region + new_transmission_latency
                    > workflow_description["constraints"]["runtime"]
                    or deployment_option[3] + execution_carbon_of_function_in_region + new_transmission_carbon
                    > workflow_description["constraints"]["carbon"]
                ):
                    continue

                new_deployment_option = (
                    deployment_option[0].copy(),
                    deployment_option[1] + cost_of_function_in_region,
                    deployment_option[2] + runtime_of_function_in_region,
                    deployment_option[3] + execution_carbon_of_function_in_region,
                )

                new_deployment_option[0][function] = region
                new_deployment_options.append(new_deployment_option)

            new_deployment_options = [
                deployment_option
                for deployment_option in new_deployment_options
                if len(deployment_option[0]) == current_index + 1
            ]

        deployment_options = np.array(new_deployment_options)


def sort_deployment_options(viable_deployment_options: np.ndarray, workflow_description: dict) -> np.ndarray:
    # TODO: Implement logic to sort the viable deployment options based on the objectives given in the workflow description
    pass


def main(
    workflow_description: dict,
    function_runtime_measurements: dict,
    function_data_transfer_size_measurements: dict,
    select_deplyoment_number: int = 0,
):
    # To be called with a workflow description of n functions and a list of n integers representing the runtime of each function
    # The m regions are retrieved from the DynamoDB table "multi-x-serverless-datacenter-info"
    # For more details, see the README.md file as well as the workflow description schema here:
    # multi_x_serverless/config/global_routing/workflow_description.yaml

    # First get all regions
    regions: np.ndarray = get_regions()

    # Filter out regions that are not viable due to the workflow description
    viable_regions: np.ndarray = filter_regions(regions, workflow_description)

    # Find the viable deployment options for the workflow
    # A viable deployment option is a tuple of the form (list of regions, cost, runtime, carbon)
    viable_deployment_options: np.ndarray = find_viable_deployment_options(
        viable_regions,
        function_runtime_measurements,
        function_data_transfer_size_measurements,
        workflow_description,
    )

    # Sort the viable deployment options according to the objectives given in the workflow description
    sorted_deployment_options: np.ndarray = sort_deployment_options(viable_deployment_options, workflow_description)

    # Return the best deployment option
    # TODO (vGsteiger): We
    return sorted_deployment_options[select_deplyoment_number]
