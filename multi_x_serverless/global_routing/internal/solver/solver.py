import boto3
import networkx as nx
import numpy as np

AWS_DATACENTER_INFO_TABLE_NAME = "multi-x-serverless-datacenter-info"
DEFAULT_REGION = "us-west-2"


class Solver:
    def get_regions(self) -> np.ndarray:
        """
        Retrieves all regions from the DynamoDB table "multi-x-serverless-datacenter-info"
        :return: A numpy array of tuples of the form (region, provider)
        """
        client = boto3.client(
            "dynamodb",
            region_name=DEFAULT_REGION,
        )

        response = client.scan(
            TableName=AWS_DATACENTER_INFO_TABLE_NAME,
        )

        results = np.array([(item["region_code"]["S"], item["provider"]["S"]) for item in response["Items"]])
        return results

    def filter_regions(self, regions: np.ndarray, workflow_description: dict) -> np.ndarray:
        if "filtered_regions" in workflow_description:
            return np.array(
                [
                    region
                    for region in regions
                    if f"{region[0]}:{region[1]}" not in workflow_description["filtered_regions"]
                ]
            )

        return regions

    def get_cost_for_region_function(self, region: str, function: dict) -> callable:
        # TODO: Implement logic to retrieve the cost of the function in the given region
        return lambda function, function_runtime_measurements: 0.0

    def get_cost_matrix(self, regions: np.ndarray, functions: np.ndarray) -> np.ndarray:
        cost_matrix = np.zeros((len(functions), len(regions)))
        for i, function in enumerate(functions):
            for j, region in enumerate(regions):
                cost_matrix[i][j] = self.get_cost_for_region_function(region, function)

    def get_runtime_for_region_function(self, region: str, destination_region: str) -> callable:
        # TODO: Implement logic to retrieve the runtime of the function in the given region
        return lambda function, function_runtime_measurements: 0.0

    def get_runtime_array(self, regions: np.ndarray) -> np.ndarray:
        runtime_array = np.zeros(len(regions))
        for i, region in enumerate(regions):
            runtime_array[i] = self.get_runtime_coefficient_for_region(region)
        return runtime_array

    def get_latency_coefficient_for_region(self, region: str, destination_region: str) -> float:
        # TODO: Implement logic to retrieve the latency coefficient between the two regions
        return 0.0

    def get_latency_matrix(self, regions: np.ndarray) -> np.ndarray:
        latency_matrix = np.zeros((len(regions), len(regions)))
        for i, region1 in enumerate(regions):
            for j, region2 in enumerate(regions[i:], start=i):
                latency_matrix[i][j] = self.get_latency_coefficient_for_region(region1, region2)
                latency_matrix[j][i] = latency_matrix[i][j]
        return latency_matrix

    def get_execution_carbon_for_region_function(self, region: str, function: dict) -> callable:
        # TODO: Implement logic to retrieve the execution carbon of the function in the given region
        return lambda function, function_runtime_measurements: 0.0

    def get_execution_carbon_matrix(self, regions: np.ndarray, functions: np.ndarray) -> np.ndarray:
        execution_carbon_matrix = np.zeros((len(functions), len(regions)))
        for i, function in enumerate(functions):
            for j, region in enumerate(regions):
                execution_carbon_matrix[i][j] = self.get_execution_carbon_for_region_function(region, function)

    def get_transmission_carbon_coefficient_for_region_and_destination_region(
        self, region: str, destination_region: str
    ) -> callable:
        # TODO: Implement logic to retrieve the transmission carbon coefficient between the two regions
        return 0.0

    def get_transmission_carbon_matrix(self, regions: np.ndarray) -> np.ndarray:
        transmission_carbon_matrix = np.zeros((len(regions), len(regions)))
        for i, region1 in enumerate(regions):
            for j, region2 in enumerate(regions[i:], start=i):
                transmission_carbon_matrix[i][
                    j
                ] = self.get_transmission_carbon_coefficient_for_region_and_destination_region(region1, region2)
                transmission_carbon_matrix[j][i] = transmission_carbon_matrix[i][j]

    def get_egress_cost_coefficient_for_region_and_destination_region(
        self, region: str, destination_region: str
    ) -> float:
        # TODO: Implement logic to retrieve the egress cost coefficient between the two regions
        return 0.0

    def get_egress_cost_matrix(self, regions: np.ndarray) -> np.ndarray:
        egress_cost_matrix = np.zeros((len(regions), len(regions)))
        for i, region1 in enumerate(regions):
            for j, region2 in enumerate(regions[i:], start=i):
                egress_cost_matrix[i][j] = self.get_egress_cost_coefficient_for_region_and_destination_region(
                    region1, region2
                )
                egress_cost_matrix[j][i] = egress_cost_matrix[i][j]

    def build_dag(self, workflow_description: dict) -> tuple[nx.DiGraph, dict]:
        dag = nx.DiGraph()
        for function in workflow_description["functions"]:
            dag.add_node(function["name"])
            for next_function in function["next_functions"]:
                dag.add_edge(function["name"], next_function["name"])
        return dag

    def find_viable_deployment_options(  # pylint: disable=too-many-locals
        self,
        regions: np.ndarray,
        function_runtime_measurements: dict,
        function_data_transfer_size_measurements: dict,
        workflow_description: dict,
    ) -> np.ndarray:
        # First we build the DAG and some auxiliary data structures
        region_to_index = {region: i for i, region in enumerate(regions)}
        function_to_spec = {function["name"]: function for function in workflow_description["functions"]}

        dag = self.build_dag(workflow_description)

        # Now we can start the actual algorithm
        # We start by adding the start hop region to the DAG and connecting it to all functions that have no predecessors
        initial_start_hop_region = workflow_description["start_hop"]

        successors_of_first_hop = [node for node, in_degree in dag.in_degree() if in_degree == 0]

        dag.add_node(initial_start_hop_region)
        for initial_node in successors_of_first_hop:
            dag.add_edge(initial_start_hop_region, initial_node)

        # Because the DAG is acyclic, we can use topological sort to get the order in which we need to process the functions
        sorted_functions = list(nx.topological_sort(dag))

        # Retrieve all the matrices and arrays that we need to compute the viable deployment options
        cost_matrix: np.ndarray = self.get_cost_matrix(regions, sorted_functions)
        egress_cost_matrix: np.ndarray = self.get_egress_cost_matrix(regions)
        runtime_array: np.ndarray = self.get_runtime_array(regions)
        latency_matrix: np.ndarray = self.get_latency_matrix(regions)
        execution_carbon_matrix: np.ndarray = self.get_execution_carbon_matrix(regions, sorted_functions)
        transmission_carbon_matrix: np.ndarray = self.get_transmission_carbon_matrix(regions)

        # The initial deployment option is the start hop region
        deployment_options = np.array([({initial_start_hop_region: initial_start_hop_region}, 0, 0, 0)])

        # Now we iterate over all functions and compute the viable deployment options for each function
        for i, function in enumerate(sorted_functions[1:]):
            new_deployment_options = np.array([])
            current_index = i + 1  # We start at 1 because the first function is the start hop
            # We iterate over all regions and compute the viable deployment options for each region
            for region, provider in regions:
                # Calculate the cost, runtime and carbon of the function in the new region
                cost_of_function_in_region = cost_matrix[current_index][region_to_index[region]](
                    function_to_spec[function], function_runtime_measurements[function], provider
                )
                runtime_of_function_in_region = runtime_array[region_to_index[region]](
                    function_to_spec[function], function_runtime_measurements[function], provider
                )
                execution_carbon_of_function_in_region = execution_carbon_matrix[current_index][
                    region_to_index[region]
                ](function_to_spec[function], function_runtime_measurements[function], provider)

                # We iterate over all viable deployment options for the previous function and
                # compute the viable deployment options for the current function.
                # This is to calculate the cost, runtime and carbon of the transmission of the
                # function from the previous region to the current region.
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
                        new_transmission_cost += self.get_egress_cost(
                            egress_cost_matrix[region_to_index[deployment_option[0][predecessor]]][
                                region_to_index[region]
                            ]
                            * function_data_transfer_size_measurements[function]
                        )
                        new_transmission_latency += (
                            latency_matrix[region_to_index[deployment_option[0][predecessor]]][region_to_index[region]]
                            * function_data_transfer_size_measurements[function]
                        )

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
                        deployment_option[1] + cost_of_function_in_region,
                        deployment_option[2] + runtime_of_function_in_region,
                        deployment_option[3] + execution_carbon_of_function_in_region,
                    )

                    new_deployment_option[0][function] = region
                    np.append(new_deployment_options, new_deployment_option)

            # We only keep the viable deployment options
            new_deployment_options = [
                deployment_option
                for deployment_option in new_deployment_options
                if len(deployment_option[0]) == current_index + 1
            ]

            deployment_options = new_deployment_options

    def sort_deployment_options(self, viable_deployment_options: np.ndarray, workflow_description: dict) -> np.ndarray:
        # TODO: Implement logic to sort the viable deployment options
        # based on the objectives given in the workflow description
        pass

    def run(
        self,
        workflow_description: dict,
        function_runtime_measurements: dict,
        function_data_transfer_size_measurements: dict,
        select_deplyoment_number: int = 0,
    ):
        # To be called with a workflow description of n functions and a list of n integers
        # representing the runtime of each function.
        # The m regions are retrieved from the DynamoDB table "multi-x-serverless-datacenter-info".
        # For more details, see the README.md file as well as the workflow description schema here:
        # multi_x_serverless/config/global_routing/workflow_description.yaml

        # First get all regions
        regions: np.ndarray = self.get_regions()

        # Filter out regions that are not viable due to the workflow description
        viable_regions: np.ndarray = self.filter_regions(regions, workflow_description)

        # Find the viable deployment options for the workflow
        # A viable deployment option is a tuple of the form (list of regions, cost, runtime, carbon)
        viable_deployment_options: np.ndarray = self.find_viable_deployment_options(
            viable_regions,
            function_runtime_measurements,
            function_data_transfer_size_measurements,
            workflow_description,
        )

        # Sort the viable deployment options according to the objectives given in the workflow description
        sorted_deployment_options: np.ndarray = self.sort_deployment_options(
            viable_deployment_options, workflow_description
        )

        # Return the best deployment option
        # TODO (vGsteiger): We
        return sorted_deployment_options[select_deplyoment_number]
