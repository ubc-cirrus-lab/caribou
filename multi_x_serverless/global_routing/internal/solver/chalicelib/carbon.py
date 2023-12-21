from typing import Callable

from .utils import (
    AWS,
    AWS_DATACENTER_INFO_TABLE_NAME,
    GRID_CO2_TABLE_NAME,
    TRANSMISSION_CO2_TABLE_NAME,
    get_item_from_dynamodb,
)


def get_execution_carbon_for_region_function(region_provider: tuple[str, str]) -> Callable:
    # The function is a lambda function that takes the function spec and the function runtime measurement in ms
    region, provider = region_provider
    table = ""
    if provider == AWS:
        table = AWS_DATACENTER_INFO_TABLE_NAME
    datacenter_datas = get_item_from_dynamodb({"region_code": region, "provider": provider}, table)

    if not datacenter_datas:
        datacenter_data = {}
    else:
        datacenter_data = datacenter_datas[0]

    grid_co2_datas = get_item_from_dynamodb(
        {"region_code_provider": region + ":" + provider}, GRID_CO2_TABLE_NAME, limit=1, order="desc"
    )
    if len(grid_co2_datas) > 0:
        grid_co2_data = grid_co2_datas[0]
    else:
        raise ValueError(f"Could not find data for region {region} and provider {provider}")

    def cost(
        function_spec: dict,
        function_runtime_measurements: list[int],
        grid_co2_data: dict = grid_co2_data,
        datacenter_data: dict = datacenter_data,
    ) -> float:
        if datacenter_data and grid_co2_data:
            runtime_in_hours = (
                (sum(function_runtime_measurements) / len(function_runtime_measurements)) / 1000 / 60 / 60
            )  # ms -> h

            # Average power from compute
            # Compute Watt-Hours = Average Watts * vCPU Hours
            # GCP: Median Min Watts: 0.71 Median Max Watts: 4.26
            # In terms of kW
            average_kw_compute = (0.71 + 0.5 * (4.26 - 0.71)) / 1000
            vcpu = function_spec["resource_request"]["vCPU"]
            compute_kwh = average_kw_compute * vcpu * runtime_in_hours

            # They used 0.000392 Kilowatt Hour / Gigabyte Hour (0.000392 kWh/Gbh) -> 0.000000392 kWh/Mb
            memory_kw_mb = 0.000000392
            memory = function_spec["resource_request"]["memory"]  # MB
            memory_kwh = memory_kw_mb * memory * runtime_in_hours

            cloud_provider_usage_kwh = compute_kwh + memory_kwh

            operational_emission = (
                cloud_provider_usage_kwh
                * (1 - datacenter_data["cfe"])
                * datacenter_data["pue"]
                * grid_co2_data["carbon_intensity"]
            )

            return operational_emission

        print("Could not find any data for this provider and region.")
        return 0.0

    return cost


def get_execution_carbon_matrix(regions: list[tuple[str, str]], number_of_functions: int) -> list[list[Callable]]:
    execution_carbon_matrix: list = []
    for i in range(number_of_functions):
        execution_carbon_matrix.append([])
        # print('\n\nAll regions:', regions)
        for region_provider in regions:
            execution_carbon_matrix[i].append(get_execution_carbon_for_region_function(region_provider))
            # break # Solver need to use multiple regions, this break brakes solver.

    # print('execution_carbon_matrix:', execution_carbon_matrix, '\n\n')
    return execution_carbon_matrix


def get_transmission_carbon_coefficient_for_region_and_destination_region(
    region_provider: tuple[str, str], destination_region_provider: tuple[str, str]
) -> float:
    region, provider = region_provider
    destination_region, destination_provider = destination_region_provider
    region_from_to_codes = provider + ":" + region + ":" + destination_provider + ":" + destination_region

    transmission_co2_data = get_item_from_dynamodb(
        {"region_from_to_codes": region_from_to_codes}, TRANSMISSION_CO2_TABLE_NAME, limit=1, order="desc"
    )

    if len(transmission_co2_data) == 0:
        return 100.0  # Assume a high transmission carbon if we don't have the data
    return float(transmission_co2_data[0]["carbon_intensity"])


def get_transmission_carbon_matrix(regions: list[tuple[str, str]]) -> list[list[float]]:
    transmission_carbon_matrix: list = []
    for i, region1 in enumerate(regions):
        transmission_carbon_matrix.append([])
        for region2 in regions:
            transmission_carbon_matrix[i].append(
                get_transmission_carbon_coefficient_for_region_and_destination_region(region1, region2)
            )
    return transmission_carbon_matrix
