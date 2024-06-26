from caribou.deployment_solver.deployment_input.components.calculator import InputCalculator
from caribou.deployment_solver.deployment_input.components.calculators.runtime_calculator import RuntimeCalculator
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class CostCalculator(InputCalculator):
    def __init__(
        self,
        datacenter_loader: DatacenterLoader,
        workflow_loader: WorkflowLoader,
        # runtime_calculator: RuntimeCalculator, # Possibly remove this
        consider_intra_region_transfer_for_sns: bool = False
    ) -> None:
        super().__init__()
        self._datacenter_loader: DatacenterLoader = datacenter_loader
        self._workflow_loader: WorkflowLoader = workflow_loader
        # self._runtime_calculator: RuntimeCalculator = runtime_calculator

        # Conversion ratio cache
        self._execution_conversion_ratio_cache: dict[str, tuple[float, float]] = {}
        self._transmission_conversion_ratio_cache: dict[str, float] = {}
        self._consider_intra_region_transfer_for_sns: bool = consider_intra_region_transfer_for_sns

    # def calculate_transmission_cost(
    #     self,
    #     from_region_name: str,
    #     to_region_name: str,
    #     data_transfer_size: float,
    # ) -> float:
    #     if from_region_name == to_region_name:  # No egress cost from self transmission
    #         return 0.0

    #     transmission_cost_gb = self._get_transmission_conversion_ratio(from_region_name, to_region_name)
    #     return transmission_cost_gb * data_transfer_size

    # def calculate_execution_cost(self, instance_name: str, region_name: str, execution_latency: float) -> float:
    #     cost_from_compute_s, invocation_cost = self._get_execution_conversion_ratio(instance_name, region_name)
    #     return cost_from_compute_s * execution_latency + invocation_cost

    # def _get_transmission_conversion_ratio(self, from_region_name: str, to_region_name: str) -> float:
    #     # Check if the conversion ratio is in the cache
    #     key = from_region_name + "_" + to_region_name
    #     if key in self._transmission_conversion_ratio_cache:
    #         return self._transmission_conversion_ratio_cache[key]

    #     # Get the providers of the 2 instances
    #     from_provider, _ = from_region_name.split(":")
    #     to_provider, _ = to_region_name.split(":")

    #     # Determine if the transfer is intra-provider or inter-provider
    #     intra_provider_transfer: bool = False
    #     if from_provider == to_provider:
    #         intra_provider_transfer = True

    #     # Get the cost of transmission
    #     transmission_cost_gb: float = self._datacenter_loader.get_transmission_cost(
    #         to_region_name, intra_provider_transfer
    #     )

    #     # Add the conversion ratio to the cache
    #     self._transmission_conversion_ratio_cache[key] = transmission_cost_gb
    #     return self._transmission_conversion_ratio_cache[key]

######### New functions #########
    def calculate_instance_cost(
        self,
        runtime: float,
        instance_name: str,
        current_region_name: str,
        data_output_sizes: dict[str, float],
        sns_data_output_sizes: dict[str, float],
        dynamodb_read_capacity: float,
        dynamodb_write_capacity: float,
        is_invoked: bool) -> float:
            total_cost = 0.0

            # If the function is actually invoked then
            # We must consider the cost of execution and SNS
            if is_invoked:
                # Calculate execution cost of the instance itself
                total_cost += self._calculate_execution_cost(instance_name, current_region_name, runtime)

                # Add the cost of SNS (Our current orchastration service)
                total_cost += self._calculate_sns_cost(current_region_name, sns_data_output_sizes)

            # Even if the function is not invoked, we model
            # Each node as an abstract instance to consider
            # data transfer and dynamodb costs

            # Calculate the data transfer (egress cost) of the instance
            total_cost += self._calculate_data_transfer_cost(current_region_name, data_output_sizes)
            
            # Calculate the dynamodb read/write capacity cost
            total_cost += self._calculate_dynamodb_cost(current_region_name, dynamodb_read_capacity, dynamodb_write_capacity)

            return total_cost
    
    def _calculate_dynamodb_cost(
        self,
        current_region_name: str,
        dynamodb_read_capacity: float,
        dynamodb_write_capacity: float) -> float:
            total_dynamodb_cost = 0.0

            # Get the cost of dynamodb read/write capacity
            read_cost, write_cost = self._datacenter_loader.get_dynamodb_read_write_cost(current_region_name)

            total_dynamodb_cost += dynamodb_read_capacity * read_cost
            total_dynamodb_cost += dynamodb_write_capacity * write_cost

            return total_dynamodb_cost


    def _calculate_sns_cost(
        self,
        current_region_name: str,
        sns_data_output_sizes: dict[str, float]) -> float:
            total_sns_cost = 0.0

            # If assume SNS intra region data transfer is NOT free
            # We need to additionally add all SNS data transfer
            # INSIDE the current region (As data_output_size already
            # includes data transfer outside the region and contains 
            # sns_data_output_sizes)
            if self._consider_intra_region_transfer_for_sns:
                total_data_output_size = sns_data_output_sizes.get(current_region_name, 0.0)

                # Calculate the cost of data transfer
                # This is simply the egress cost of data transfer
                # In a region
                # Get the cost of transmission
                transmission_cost_gb: float = self._datacenter_loader.get_transmission_cost(
                    current_region_name, True
                )

                total_sns_cost += total_data_output_size * transmission_cost_gb

            # TODO: LOOK INTO THIS COST, perhaps its where it is send (As what calls this node)
            invocation_cost = self._datacenter_loader.get_sns_request_cost(current_region_name)
            total_sns_cost += invocation_cost

            return total_sns_cost

    def _calculate_data_transfer_cost(
        self,
        current_region_name: str,
        data_output_sizes: dict[str, float]) -> float:
            # Calculate the amount of data output from the instance
            # This will be the data output going out of the current region
            total_data_output_size = 0.0
            for data_name, data_size in data_output_sizes.items():
                if not data_name.startswith(current_region_name):
                    total_data_output_size += data_size


            # Calculate the cost of data transfer
            # This is simply the egress cost of data transfer
            # In a region
            # Get the cost of transmission
            transmission_cost_gb: float = self._datacenter_loader.get_transmission_cost(
                current_region_name, True
            )
    
            return total_data_output_size * transmission_cost_gb


    def _calculate_execution_cost(
        self,
        instance_name: str,
        region_name: str,
        runtime: float) -> float:
            cost_from_compute_s, invocation_cost = self._get_execution_conversion_ratio(instance_name, region_name)
            return cost_from_compute_s * runtime + invocation_cost


    def _get_execution_conversion_ratio(self, instance_name: str, region_name: str) -> tuple[float, float]:
        # Check if the conversion ratio is in the cache
        key = instance_name + "_" + region_name
        if key in self._execution_conversion_ratio_cache:
            return self._execution_conversion_ratio_cache[key]

        # Get the number of vCPUs and Memory of the instance
        provider, _ = region_name.split(":")
        memory: float = self._workflow_loader.get_memory(instance_name, provider)

        ## datacenter loader data
        architecture: str = self._workflow_loader.get_architecture(instance_name, provider)
        compute_cost: float = self._datacenter_loader.get_compute_cost(region_name, architecture)
        invocation_cost: float = self._datacenter_loader.get_invocation_cost(region_name, architecture)

        # Compute cost in USD /  GB-seconds
        # Memory in MB, execution_time in seconds, vcpu in vcpu
        memory_gb: float = memory / 1024
        cost_from_compute_s: float = compute_cost * memory_gb  # IN USD / s

        # Add the conversion ratio to the cache
        self._execution_conversion_ratio_cache[key] = (cost_from_compute_s, invocation_cost)
        return self._execution_conversion_ratio_cache[key]
