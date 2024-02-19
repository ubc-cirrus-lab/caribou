from multi_x_serverless.data_collector.components.carbon.carbon_collector import CarbonCollector
from multi_x_serverless.data_collector.components.provider.provider_collector import ProviderCollector
from multi_x_serverless.data_collector.components.performance.performance_collector import PerformanceCollector
from multi_x_serverless.common.constants import (
    AVAILABLE_REGIONS_TABLE,
    PROVIDER_REGION_TABLE,
    CARBON_REGION_TABLE,
    PERFORMANCE_REGION_TABLE,
)
from multi_x_serverless.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient


def test_data_collect():
    # This test runs a data collection process of the data that we have on a region level (without a deployed workflow)
    print("Test data collection.")

    run_data_collection()


def run_data_collection():
    carbon_collector = CarbonCollector()
    provider_collector = ProviderCollector()
    performance_collector = PerformanceCollector()
    remote_client = IntegrationTestRemoteClient()

    provider_collector.run()

    available_regions = remote_client.select_all_from_table(AVAILABLE_REGIONS_TABLE)

    assert len(available_regions) == 4

    expected_available_regions = [
        "integration_test_provider:rivendell",
        "integration_test_provider:lothlorien",
        "integration_test_provider:anduin",
        "integration_test_provider:fangorn",
    ]

    for region in available_regions:
        assert region[0] in expected_available_regions

    provider_region_data = remote_client.select_all_from_table(PROVIDER_REGION_TABLE)

    assert len(provider_region_data) == 4

    expected_provider_region_data = [
        (
            "integration_test_provider:rivendell",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.4e-07, "x86_64": 2.3e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56138e-05, "x86_64": 1.95172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"}, "pue": 1.15, "cfe": 0.9, "average_memory_power": 3.92e-06, "average_cpu_power": 0.00212, "available_architectures": ["arm64", "x86_64"]}',
        ),
        (
            "integration_test_provider:lothlorien",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.3e-07, "x86_64": 2.2e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56118e-05, "x86_64": 1.93172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.04, "provider_data_transfer": 0.09, "unit": "USD/GB"}, "pue": 1.15, "cfe": 0.9, "average_memory_power": 3.92e-06, "average_cpu_power": 0.00212, "available_architectures": ["arm64", "x86_64"]}',
        ),
        (
            "integration_test_provider:anduin",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.2e-07, "x86_64": 2.1e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56128e-05, "x86_64": 1.91172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.11, "provider_data_transfer": 0.05, "unit": "USD/GB"}, "pue": 1.15, "cfe": 0.9, "average_memory_power": 3.92e-06, "average_cpu_power": 0.00212, "available_architectures": ["arm64", "x86_64"]}',
        ),
        (
            "integration_test_provider:fangorn",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.1e-07, "x86_64": 2e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56148e-05, "x86_64": 1.89172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.07, "provider_data_transfer": 0.03, "unit": "USD/GB"}, "pue": 1.15, "cfe": 0.9, "average_memory_power": 3.92e-06, "average_cpu_power": 0.00212, "available_architectures": ["arm64", "x86_64"]}',
        ),
    ]

    for region in provider_region_data:
        assert region in expected_provider_region_data

    carbon_collector.run()

    carbon_region_data = remote_client.select_all_from_table(CARBON_REGION_TABLE)

    assert len(carbon_region_data) == 4

    expected_carbon_region_data = [
        (
            "integration_test_provider:rivendell",
            '{"carbon_intensity": 51.391773, "unit": "gCO2eq/kWh", "transmission_carbon": {"integration_test_provider:rivendell": {"carbon_intensity": 0, "distance": 0.0, "unit": "gCO2eq/GB"}, "integration_test_provider:lothlorien": {"carbon_intensity": 53.33861014251194, "distance": 83.13919477602013, "unit": "gCO2eq/GB"}, "integration_test_provider:anduin": {"carbon_intensity": 59.433835047560095, "distance": 343.4342546548361, "unit": "gCO2eq/GB"}, "integration_test_provider:fangorn": {"carbon_intensity": 59.74313540772055, "distance": 356.6428494192109, "unit": "gCO2eq/GB"}}}',
        ),
        (
            "integration_test_provider:lothlorien",
            '{"carbon_intensity": 50.494296, "unit": "gCO2eq/kWh", "transmission_carbon": {"integration_test_provider:rivendell": {"carbon_intensity": 52.447556262638514, "distance": 83.13919477602013, "unit": "gCO2eq/GB"}, "integration_test_provider:lothlorien": {"carbon_intensity": 0, "distance": 0.0, "unit": "gCO2eq/GB"}, "integration_test_provider:anduin": {"carbon_intensity": 60.161081221394646, "distance": 411.46013910805647, "unit": "gCO2eq/GB"}, "integration_test_provider:fangorn": {"carbon_intensity": 60.50469196188435, "distance": 426.0856966065312, "unit": "gCO2eq/GB"}}}',
        ),
        (
            "integration_test_provider:anduin",
            '{"carbon_intensity": 51.208835, "unit": "gCO2eq/kWh", "transmission_carbon": {"integration_test_provider:rivendell": {"carbon_intensity": 58.98294472808719, "distance": 343.4342546548361, "unit": "gCO2eq/GB"}, "integration_test_provider:lothlorien": {"carbon_intensity": 60.52280478258588, "distance": 411.46013910805647, "unit": "gCO2eq/GB"}, "integration_test_provider:anduin": {"carbon_intensity": 0, "distance": 0.0, "unit": "gCO2eq/GB"}, "integration_test_provider:fangorn": {"carbon_intensity": 60.93927680923625, "distance": 429.85848503575806, "unit": "gCO2eq/GB"}}}',
        ),
        (
            "integration_test_provider:fangorn",
            '{"carbon_intensity": 57.265384, "unit": "gCO2eq/kWh", "transmission_carbon": {"integration_test_provider:rivendell": {"carbon_intensity": 65.7164255385307, "distance": 356.6428494192109, "unit": "gCO2eq/GB"}, "integration_test_provider:lothlorien": {"carbon_intensity": 67.36194958896262, "distance": 426.0856966065312, "unit": "gCO2eq/GB"}, "integration_test_provider:anduin": {"carbon_intensity": 67.45134992821912, "distance": 429.85848503575806, "unit": "gCO2eq/GB"}, "integration_test_provider:fangorn": {"carbon_intensity": 0, "distance": 0.0, "unit": "gCO2eq/GB"}}}',
        ),
    ]

    for region in carbon_region_data:
        assert region in expected_carbon_region_data

    performance_collector.run()

    performance_region_data = remote_client.select_all_from_table(PERFORMANCE_REGION_TABLE)

    assert len(performance_region_data) == 4

    expected_performance_region_data = [
        (
            "integration_test_provider:rivendell",
            '{"relative_performance": 1, "transmission_latency": {"integration_test_provider:rivendell": {"average_latency": 0.01, "tail_latency": 0.012, "unit": "s"}, "integration_test_provider:lothlorien": {"average_latency": 0.15, "tail_latency": 0.152, "unit": "s"}, "integration_test_provider:anduin": {"average_latency": 0.08, "tail_latency": 0.082, "unit": "s"}, "integration_test_provider:fangorn": {"average_latency": 0.2, "tail_latency": 0.22, "unit": "s"}}}',
        ),
        (
            "integration_test_provider:lothlorien",
            '{"relative_performance": 1, "transmission_latency": {"integration_test_provider:rivendell": {"average_latency": 0.15, "tail_latency": 0.152, "unit": "s"}, "integration_test_provider:lothlorien": {"average_latency": 0.01, "tail_latency": 0.012, "unit": "s"}, "integration_test_provider:anduin": {"average_latency": 0.1, "tail_latency": 0.12, "unit": "s"}, "integration_test_provider:fangorn": {"average_latency": 0.25, "tail_latency": 0.252, "unit": "s"}}}',
        ),
        (
            "integration_test_provider:anduin",
            '{"relative_performance": 1, "transmission_latency": {"integration_test_provider:rivendell": {"average_latency": 0.08, "tail_latency": 0.082, "unit": "s"}, "integration_test_provider:lothlorien": {"average_latency": 0.1, "tail_latency": 0.12, "unit": "s"}, "integration_test_provider:anduin": {"average_latency": 0.01, "tail_latency": 0.012, "unit": "s"}, "integration_test_provider:fangorn": {"average_latency": 0.15, "tail_latency": 0.152, "unit": "s"}}}',
        ),
        (
            "integration_test_provider:fangorn",
            '{"relative_performance": 1, "transmission_latency": {"integration_test_provider:rivendell": {"average_latency": 0.2, "tail_latency": 0.22, "unit": "s"}, "integration_test_provider:lothlorien": {"average_latency": 0.25, "tail_latency": 0.252, "unit": "s"}, "integration_test_provider:anduin": {"average_latency": 0.15, "tail_latency": 0.152, "unit": "s"}, "integration_test_provider:fangorn": {"average_latency": 0.01, "tail_latency": 0.012, "unit": "s"}}}',
        ),
    ]

    for region in performance_region_data:
        assert region in expected_performance_region_data

    print("Data collection successful.")
