from typing import Callable


def get_runtime_for_region_function(region_provider: tuple[str, str]) -> Callable:
    region, provider = region_provider  # pylint: disable=unused-variable
    # TODO: Implement logic to retrieve the runtime of the function in the given region
    return lambda function, function_runtime_measurements: 0.0


def get_runtime_array(regions: list[tuple[str, str]]) -> list[Callable]:
    runtime_array = []
    for region in regions:
        runtime_array.append(get_runtime_for_region_function(region))
    return runtime_array


def get_latency_coefficient_for_region(
    region_provider: tuple[str, str], destination_region_provider: tuple[str, str]
) -> float:
    region, provider = region_provider  # pylint: disable=unused-variable
    destination_region, destination_provider = destination_region_provider  # pylint: disable=unused-variable
    # TODO: Implement logic to retrieve the latency coefficient between the two regions
    return 0.0


def get_latency_matrix(regions: list[tuple[str, str]]) -> list[list[float]]:
    latency_matrix: list = []
    for i, region1 in enumerate(regions):
        latency_matrix.append([])
        for region2 in regions:
            latency_matrix[i].append(get_latency_coefficient_for_region(region1, region2))
    return latency_matrix
