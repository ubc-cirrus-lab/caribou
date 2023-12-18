import networkx as nx
from tqdm import tqdm

from .chalicelib.carbon import get_execution_carbon_matrix, get_transmission_carbon_matrix
from .chalicelib.cost import get_cost_matrix, get_egress_cost_matrix
from .chalicelib.regions import filter_regions, get_regions
from .chalicelib.runtime import get_latency_matrix, get_runtime_array
from .chalicelib.utils import get_dag, ENERGY_CONSUMPTION_PER_GB


def find_viable_deployment_options(  # pylint: disable=too-many-locals
    regions: list[tuple[str, str]],
    function_runtime_measurements: dict[str, list[float]],
    function_data_transfer_size_measurements: dict[str, list[float]],
    workflow_description: dict,
) -> list[tuple[dict, float, float, float]]:
    # First we build the DAG and some auxiliary data structures
    region_to_index = {region[0]: i for i, region in enumerate(regions)}
    function_to_spec = {function["name"]: function for function in workflow_description["functions"]}

    dag = get_dag(workflow_description)

    # Now we can start the actual algorithm
    # Because the DAG is acyclic, we can use topological sort to get the order in which we need to process the functions
    sorted_functions = list(nx.topological_sort(dag))
    number_of_function = len(sorted_functions)

    # Retrieve all the matrices and arrays that we need to compute the viable deployment options
    cost_matrix = get_cost_matrix(regions, number_of_function)
    egress_cost_matrix = get_egress_cost_matrix(regions)
    runtime_array = get_runtime_array(regions)
    latency_matrix = get_latency_matrix(regions)
    execution_carbon_matrix = get_execution_carbon_matrix(regions, number_of_function)
    transmission_carbon_matrix = get_transmission_carbon_matrix(regions)

    # Add the start hop region to the DAG
    initial_start_hop_region = workflow_description["start_hop"]

    successors_of_first_hop = [node for node, in_degree in dag.in_degree() if in_degree == 0]

    dag.add_node(initial_start_hop_region)
    for initial_node in successors_of_first_hop:
        dag.add_edge(initial_start_hop_region, initial_node)

    # The initial deployment option is the start hop region
    deployment_options = [({initial_start_hop_region: initial_start_hop_region}, 0.0, 0.0, 0.0)]

    # print(region_to_index)

    # Now we iterate over all functions and compute the viable deployment options for each function
    # print(sorted_functions)
    # print(tqdm(sorted_functions))
    for i, function in enumerate(tqdm(sorted_functions)):
        # print('i:',i)
        # print('function:',function)
        new_deployment_options = []
        # We iterate over all regions and compute the viable deployment options for each region
        for region, _ in regions:
            # print('region:',region)
            # print('function_to_spec:',function_to_spec[function])
            # print('function_runtime_measurements:',function_runtime_measurements[function])
            # Calculate the cost, runtime and carbon of the function in the new region
            cost_of_function_in_region: float = cost_matrix[i][region_to_index[region]](
                function_to_spec[function], function_runtime_measurements[function]
            )
            runtime_of_function_in_region: float = runtime_array[region_to_index[region]](
                function_to_spec[function], function_runtime_measurements[function]
            )
            # print('region:', region)
            # print('i, execution_carbon_matrix:', i, execution_carbon_matrix)
            # print('execution_carbon_matrix[i]:', len(execution_carbon_matrix[i]), execution_carbon_matrix[i])
            # print('region_to_index[region]:', region_to_index[region])
            # print('function_to_spec[function]:', function_to_spec[function])
            # print('function_runtime_measurements[function]:', function_runtime_measurements[function])
            execution_carbon_of_function_in_region: float = execution_carbon_matrix[i][region_to_index[region]](
                function_to_spec[function], function_runtime_measurements[function]
            )

            # We iterate over all viable deployment options for the previous function and
            # compute the viable deployment options for the current function.
            # This is to calculate the cost, runtime and carbon of the transmission of the
            # function from the previous region to the current region.
            for deployment_option in deployment_options:
                new_transmission_carbon: float = 0.0
                new_transmission_cost: float = 0.0
                new_transmission_latency: float = 0.0

                for predecessor in dag.predecessors(function):
                    # print('test1:', latency_matrix[region_to_index[deployment_option[0][predecessor]]][region_to_index[region]])
                    # print('test2:', function_data_transfer_size_measurements[function])
                    transmission_latency = (
                        latency_matrix[region_to_index[deployment_option[0][predecessor]]][region_to_index[region]]
                    ) * function_data_transfer_size_measurements[function]
                    # (gCO2) = (Data Size in GB) * (Energy Consumption per GB) * (CO2 Emissions per kWh) * (Latency in hours)
                    # Where (CO2 Emissions per kWh) are calculated as a coefficient in the transmission carbon matrix
                    # Coefficient for Energy Consumption per GB: 0.001 kWh/Gb
                    new_transmission_carbon += (
                        transmission_carbon_matrix[region_to_index[deployment_option[0][predecessor]]][
                            region_to_index[region]
                        ]
                        * (function_data_transfer_size_measurements[function] / 1000)
                        * (transmission_latency / 3600000)
                        * ENERGY_CONSUMPTION_PER_GB
                    )
                    new_transmission_cost += egress_cost_matrix[region_to_index[deployment_option[0][predecessor]]][
                        region_to_index[region]
                    ](function_data_transfer_size_measurements[function])
                    new_transmission_latency += transmission_latency

                # If we violate any of the constraints, we discard the deployment option
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
                    deployment_option[1] + cost_of_function_in_region + new_transmission_cost,
                    deployment_option[2] + runtime_of_function_in_region + new_transmission_latency,
                    deployment_option[3] + execution_carbon_of_function_in_region + new_transmission_carbon,
                )

                new_deployment_option[0][function] = region
                new_deployment_options.append(new_deployment_option)

        print(new_deployment_options)

        # We only keep the viable deployment options
        new_deployment_options = [
            deployment_option
            for deployment_option in new_deployment_options
            if len(deployment_option[0].keys())
            == i + 2  # + 2 because we added the start hop region and the current function
        ]

        deployment_options = new_deployment_options

    return deployment_options


def sort_deployment_options(
    viable_deployment_options: list[tuple[dict, float, float, float]],  # pylint: disable=unused-argument
    workflow_description: dict,  # pylint: disable=unused-argument
) -> list[tuple[dict, float, float, float]]:
    # TODO: Implement logic to sort the viable deployment options
    # based on the objectives given in the workflow description
    return []


def run(
    workflow_description: dict,
    function_runtime_measurements: dict,
    function_data_transfer_size_measurements: dict,
    select_deplyoment_number: int = 0,
) -> tuple[dict, float, float, float]:
    """
    Args:
        workflow_description (dict): A dictionary representing the workflow description.
        function_runtime_measurements (dict): A dictionary containing the runtime measurements of each function in seconds.
        function_data_transfer_size_measurements (dict): A dictionary containing the data transfer size measurements of each function in MB.
        select_deployment_number (int, optional): The index of the deployment option to select. Defaults to 0.

    Returns:
        tuple[dict, float, float, float]: A tuple containing the selected deployment option, its cost, runtime, and carbon footprint.
    """
    # To be called with a workflow description of n functions and a list of n integers
    # representing the runtime of each function.
    # The m regions are retrieved from the DynamoDB table "multi-x-serverless-datacenter-info".
    # For more details, see the README.md file as well as the workflow description schema here:
    # multi_x_serverless/config/global_routing/workflow_description.yaml

    # First get all regions
    regions: list[tuple[str, str]] = get_regions()

    # Filter out regions that are not viable due to the workflow description
    viable_regions: list[tuple[str, str]] = filter_regions(regions, workflow_description)

    # Find the viable deployment options for the workflow
    # A viable deployment option is a tuple of the form (list of regions, cost, runtime, carbon)
    viable_deployment_options: list[tuple[dict, float, float, float]] = find_viable_deployment_options(
        viable_regions,
        function_runtime_measurements,
        function_data_transfer_size_measurements,
        workflow_description,
    )

    # Sort the viable deployment options according to the objectives given in the workflow description
    sorted_deployment_options: list[tuple[dict, float, float, float]] = sort_deployment_options(
        viable_deployment_options, workflow_description
    )

    # Return the best deployment option
    return sorted_deployment_options[select_deplyoment_number]
