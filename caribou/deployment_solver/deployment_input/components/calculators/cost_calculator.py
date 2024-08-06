import math
from typing import Optional

from caribou.common.constants import GLOBAL_SYSTEM_REGION
from caribou.deployment_solver.deployment_input.components.calculator import InputCalculator
from caribou.deployment_solver.deployment_input.components.loaders.datacenter_loader import DatacenterLoader
from caribou.deployment_solver.deployment_input.components.loaders.workflow_loader import WorkflowLoader


class CostCalculator(InputCalculator):
    def __init__(
        self,
        datacenter_loader: DatacenterLoader,
        workflow_loader: WorkflowLoader,
        consider_intra_region_transfer_for_sns: bool = False,
    ) -> None:
        super().__init__()
        self._datacenter_loader: DatacenterLoader = datacenter_loader
        self._workflow_loader: WorkflowLoader = workflow_loader

        # Conversion ratio cache
        self._execution_conversion_ratio_cache: dict[str, tuple[float, float]] = {}
        self._transmission_conversion_ratio_cache: dict[str, float] = {}
        self._consider_intra_region_transfer_for_sns: bool = consider_intra_region_transfer_for_sns

    def calculate_virtual_start_instance_cost(
        self,
        data_output_sizes: dict[Optional[str], float],  # pylint: disable=unused-argument
        sns_data_call_and_output_sizes: dict[Optional[str], list[float]],
        dynamodb_read_capacity: float,
        dynamodb_write_capacity: float,
    ) -> float:
        total_cost = 0.0

        # We model the virtual start hop cost where the current region is the SYSTEM Region
        # As it pulls wpd data from the system region.
        current_region_name = f"aws:{GLOBAL_SYSTEM_REGION}"

        # Add the cost of SNS (Our current orchastration service)
        # Here we say that current region is None, such that we never
        # incur additional cost from intra-region transfer of SNS as it is not
        # ever going to be intra-region with any functions
        total_cost += self._calculate_sns_cost(None, sns_data_call_and_output_sizes)

        # We do not ever incur egress cost, as we assume client request is not
        # from a AWS or other cloud provider region and thus egrees cost is 0.0

        # Calculate the dynamodb read/write capacity cost
        total_cost += self._calculate_dynamodb_cost(
            current_region_name, dynamodb_read_capacity, dynamodb_write_capacity
        )

        return total_cost

    def calculate_instance_cost(
        self,
        execution_time: float,
        instance_name: str,
        current_region_name: str,
        data_output_sizes: dict[Optional[str], float],
        sns_data_call_and_output_sizes: dict[Optional[str], list[float]],
        dynamodb_read_capacity: float,
        dynamodb_write_capacity: float,
        is_invoked: bool,
    ) -> float:
        total_cost = 0.0

        # If the function is actually invoked then
        # We must consider the cost of execution and SNS
        if is_invoked:
            # Calculate execution cost of the instance itself
            total_cost += self._calculate_execution_cost(instance_name, current_region_name, execution_time)

            # Add the cost of SNS (Our current orchastration service)
            total_cost += self._calculate_sns_cost(current_region_name, sns_data_call_and_output_sizes)

        # Even if the function is not invoked, we model
        # Each node as an abstract instance to consider
        # data transfer and dynamodb costs

        # Calculate the data transfer (egress cost) of the instance
        total_cost += self._calculate_data_transfer_cost(current_region_name, data_output_sizes)

        # Calculate the dynamodb read/write capacity cost
        total_cost += self._calculate_dynamodb_cost(
            current_region_name, dynamodb_read_capacity, dynamodb_write_capacity
        )

        return total_cost

    def _calculate_dynamodb_cost(
        self, current_region_name: str, dynamodb_read_capacity: float, dynamodb_write_capacity: float
    ) -> float:
        total_dynamodb_cost = 0.0

        # Get the cost of dynamodb read/write capacity
        read_cost, write_cost = self._datacenter_loader.get_dynamodb_read_write_cost(current_region_name)

        total_dynamodb_cost += dynamodb_read_capacity * read_cost
        total_dynamodb_cost += dynamodb_write_capacity * write_cost

        return total_dynamodb_cost

    def _calculate_sns_cost(
        self, current_region_name: Optional[str], sns_data_call_and_output_sizes: dict[Optional[str], list[float]]
    ) -> float:
        total_sns_cost = 0.0

        # If assume SNS intra region data transfer is NOT free
        # We need to additionally add all SNS data transfer
        # INSIDE the current region (As data_output_size already
        # includes data transfer outside the region and contains
        # sns_data_output_sizes)
        if self._consider_intra_region_transfer_for_sns and current_region_name:
            _, total_data_output_size = sns_data_call_and_output_sizes.get(current_region_name, (0, 0.0))

            # Calculate the cost of data transfer
            # This is simply the egress cost of data transfer
            # In a region, get the cost of transmission
            transmission_cost_gb: float = self._datacenter_loader.get_transmission_cost(current_region_name, True)

            total_sns_cost += total_data_output_size * transmission_cost_gb

        # Get the cost of SNS invocations (Request cost of destination region)
        for region_name, sns_invocation_sizes in sns_data_call_and_output_sizes.items():
            if not region_name:
                raise ValueError("Region name cannot be None")

            # Calculate the cost of invocation
            for sns_invocation_size_gb in sns_invocation_sizes:
                # According to AWS documentation, each 64KB chunk of delivered data is billed as 1 request
                # https://aws.amazon.com/sns/pricing/
                # Convert gb to kb and divide by 64 rounded up
                requests = math.ceil(sns_invocation_size_gb * 1024**2 / 64)
                total_sns_cost += self._datacenter_loader.get_sns_request_cost(region_name) * requests

        return total_sns_cost

    def _calculate_data_transfer_cost(
        self, current_region_name: str, data_output_sizes: dict[Optional[str], float]
    ) -> float:
        # Calculate the amount of data output from the instance
        # This will be the data output going out of the current region
        total_data_output_size = 0.0
        for region_name, data_size in data_output_sizes.items():
            if not region_name or not region_name.startswith(current_region_name):
                total_data_output_size += data_size

        # Calculate the cost of data transfer
        # This is simply the egress cost of data transfer
        # In a region
        # Get the cost of transmission
        transmission_cost_gb: float = self._datacenter_loader.get_transmission_cost(current_region_name, True)

        return total_data_output_size * transmission_cost_gb

    def _calculate_execution_cost(self, instance_name: str, region_name: str, execution_time: float) -> float:
        cost_from_compute_s, invocation_cost = self._get_execution_conversion_ratio(instance_name, region_name)
        return cost_from_compute_s * execution_time + invocation_cost

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
