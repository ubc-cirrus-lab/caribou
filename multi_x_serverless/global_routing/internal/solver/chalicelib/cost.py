from typing import Callable

from .shared import AWS_DATACENTER_INFO_TABLE_NAME, get_item_from_dynamodb


def get_cost_for_region_function(region_provider: tuple[str, str]) -> Callable:
    region, provider = region_provider
    # The function is a lambda function that takes the function spec and the function runtime measurement in ms
    table = ""
    if provider == "AWS":
        table = AWS_DATACENTER_INFO_TABLE_NAME
    datacenter_data = get_item_from_dynamodb({"region_code": region, "provider": provider}, table)

    def cost(
        function_spec: dict, function_runtime_measurements: list[float], datacenter_data: dict = datacenter_data
    ) -> float:
        # TODO: This might profit from caching
        if datacenter_data:
            datacenter_data = datacenter_data["data"]
            free_invocations = int(datacenter_data["free_invocations"]["N"])
            free_compute_gb_s = int(datacenter_data["free_compute_gb_s"]["N"])

            if "architecture" in function_spec["resource_request"]:
                architecture = function_spec["resource_request"]["architecture"]
            else:
                architecture = "x86_64"

            estimated_number_of_requests_per_month = function_spec["estimated_invocations_per_month"]

            invocation_cost = float(datacenter_data["invocation_cost_" + architecture]["N"])

            if estimated_number_of_requests_per_month > free_invocations:
                invocation_cost = invocation_cost * (
                    (estimated_number_of_requests_per_month - free_invocations) / 1000000
                )
            else:
                invocation_cost = 0

            estimated_memory = function_spec["resource_request"]["memory"] / 1000  # GB
            estimated_duration = (sum(function_runtime_measurements) / len(function_runtime_measurements)) / 1000  # s

            estimated_gb_seconds_per_month = (
                estimated_memory * estimated_duration * estimated_number_of_requests_per_month
            )

            compute_cost = 0.0
            if provider == "AWS":
                compute_cost = calculate_aws_compute_cost(
                    datacenter_data["compute_cost_" + architecture + "_gb_s"],
                    estimated_gb_seconds_per_month,
                    free_compute_gb_s,
                )
        else:
            print(f"Could not find data for region {region} and provider {provider}")
            invocation_cost = 0.0
            compute_cost = 0.0

        return invocation_cost + compute_cost

    return cost


def get_cost_matrix(regions: list[tuple[str, str]], number_of_functions: int) -> list[list[Callable]]:
    cost_matrix: list = []
    for i in range(number_of_functions):
        cost_matrix.append([])
        for region in regions:
            cost_matrix[i].append(get_cost_for_region_function(region))
    return cost_matrix


def calculate_aws_compute_cost(
    price_dimensions: dict, estimated_gb_seconds_per_month: float, compute_free_tier: float
) -> float:
    compute_cost = 0.0

    if estimated_gb_seconds_per_month <= compute_free_tier:
        return compute_cost

    estimated_gb_seconds_per_month -= compute_free_tier

    for price_dimension in price_dimensions.values():
        if estimated_gb_seconds_per_month <= int(price_dimension["endRange"]):
            compute_cost += float(price_dimension["pricePerUnit"]["USD"]) * estimated_gb_seconds_per_month
            break
        compute_cost += float(price_dimension["pricePerUnit"]["USD"]) * int(price_dimension["endRange"])
        estimated_gb_seconds_per_month -= int(price_dimension["endRange"])
    return compute_cost


def get_egress_cost_for_region_and_destination_region_function(
    region_provider: tuple[str, str], destination_region_provider: tuple[str, str]
) -> Callable:
    region, provider = region_provider
    destination_region, destination_provider = destination_region_provider

    if destination_provider != "AWS":
        raise RuntimeError("Other transmission costs not yet implemented")

    table = ""
    if provider == "AWS":
        table = AWS_DATACENTER_INFO_TABLE_NAME
    datacenter_data = get_item_from_dynamodb({"region_code": region, "provider": provider}, table)

    def cost(
        function_data_transmission_measurements: list[float],
        destination_region: str = destination_region,
        datacenter_data: dict = datacenter_data,
    ) -> float:
        transmission_cost_gb = float(datacenter_data["transmission_cost_gb"][destination_region])
        estimated_data_transmission = (
            sum(function_data_transmission_measurements) / len(function_data_transmission_measurements)
        ) / 1000  # s
        return transmission_cost_gb * estimated_data_transmission

    return cost


def get_egress_cost_matrix(regions: list[tuple[str, str]]) -> list[list[Callable]]:
    egress_cost_matrix: list = []
    for i, region1 in enumerate(regions):
        egress_cost_matrix.append([])
        for region2 in regions:
            egress_cost_matrix[i].append(get_egress_cost_for_region_and_destination_region_function(region1, region2))
    return egress_cost_matrix
