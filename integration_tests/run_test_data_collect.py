from caribou.data_collector.components.carbon.carbon_collector import CarbonCollector
from caribou.data_collector.components.provider.provider_collector import ProviderCollector
from caribou.data_collector.components.performance.performance_collector import PerformanceCollector
from caribou.common.constants import (
    AVAILABLE_REGIONS_TABLE,
    PROVIDER_REGION_TABLE,
    CARBON_REGION_TABLE,
    PERFORMANCE_REGION_TABLE,
)
from caribou.common.models.remote_client.integration_test_remote_client import IntegrationTestRemoteClient


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
        "IntegrationTestProvider:rivendell",
        "IntegrationTestProvider:lothlorien",
        "IntegrationTestProvider:anduin",
        "IntegrationTestProvider:fangorn",
    ]

    for region in available_regions:
        assert region[0] in expected_available_regions

    provider_region_data = remote_client.select_all_from_table(PROVIDER_REGION_TABLE)


    expected_provider_region_data = [
        (
            "IntegrationTestProvider:rivendell",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.4e-07, "x86_64": 2.3e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56138e-05, "x86_64": 1.95172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.09, "provider_data_transfer": 0.02, "unit": "USD/GB"},"sns_cost": {"cost": 0.011, "unit": "USD"}, "dynamodb_cost": {"cost": 0.021, "unit": "USD"}, "ecr_cost": {"cost": 0.031, "unit": "USD"}, "pue": 1.15, "cfe": 1, "average_memory_power": 0.0003725, "available_architectures": ["arm64", "x86_64"], "min_cpu_power_kWh": 0.00074, "max_cpu_power_kWh": 0.0035}',
        ),
        (
            "IntegrationTestProvider:lothlorien",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.3e-07, "x86_64": 2.2e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56118e-05, "x86_64": 1.93172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.04, "provider_data_transfer": 0.09, "unit": "USD/GB"}, "sns_cost": {"cost": 0.012, "unit": "USD"}, "dynamodb_cost": {"cost": 0.022, "unit": "USD"}, "ecr_cost": {"cost": 0.032, "unit": "USD"}, "pue": 1.15, "cfe": 1, "average_memory_power": 0.0003725, "available_architectures": ["arm64", "x86_64"], "min_cpu_power_kWh": 0.00074, "max_cpu_power_kWh": 0.0035}',
        ),
        (
            "IntegrationTestProvider:anduin",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.2e-07, "x86_64": 2.1e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56128e-05, "x86_64": 1.91172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.11, "provider_data_transfer": 0.05, "unit": "USD/GB"}, "sns_cost": {"cost": 0.013, "unit": "USD"}, "dynamodb_cost": {"cost": 0.023, "unit": "USD"}, "ecr_cost": {"cost": 0.033, "unit": "USD"}, "pue": 1.15, "cfe": 1, "average_memory_power": 0.0003725, "available_architectures": ["arm64", "x86_64"], "min_cpu_power_kWh": 0.00074, "max_cpu_power_kWh": 0.0035}',
        ),
        (
            "IntegrationTestProvider:fangorn",
            '{"execution_cost": {"invocation_cost": {"arm64": 2.1e-07, "x86_64": 2e-07, "free_tier_invocations": 1000000}, "compute_cost": {"arm64": 1.56148e-05, "x86_64": 1.89172e-05, "free_tier_compute_gb_s": 400000}, "unit": "USD"}, "transmission_cost": {"global_data_transfer": 0.07, "provider_data_transfer": 0.03, "unit": "USD/GB"}, "sns_cost": {"cost": 0.014, "unit": "USD"}, "dynamodb_cost": {"cost": 0.024, "unit": "USD"}, "ecr_cost": {"cost": 0.034, "unit": "USD"}, "pue": 1.15, "cfe": 1, "average_memory_power": 0.0003725, "available_architectures": ["arm64", "x86_64"], "min_cpu_power_kWh": 0.00074, "max_cpu_power_kWh": 0.0035}',
        ),
    ]

    print("Collected provider_region_data:", provider_region_data)
    print("Expected provider_region_data:", expected_provider_region_data)

    for region in provider_region_data:
        actual_data = region[1].replace(" ", "").replace("\n", "")
        found = False
        for expected_region in expected_provider_region_data:
            if region[0] == expected_region[0]:
                expected_data = expected_region[1].replace(" ", "").replace("\n", "")
                if actual_data == expected_data:
                    found = True
                    break
        assert found, f"Unexpected region data: {region}"

    carbon_collector.run()

    carbon_region_data = remote_client.select_all_from_table(CARBON_REGION_TABLE)
    print("Collected carbon_region_data:", carbon_region_data)
    print("Expected carbon_region_data:", expected_carbon_region_data)

    assert len(carbon_region_data) == 4

    expected_carbon_region_data = [
        (
            'IntegrationTestProvider:rivendell', '{"averages": {"overall": {"carbon_intensity": 51.391773}, "0": {"carbon_intensity": 51.391773}, "1": {"carbon_intensity": 51.391773}, "2": {"carbon_intensity": 51.391773}, "3": {"carbon_intensity": 51.391773}, "4": {"carbon_intensity": 51.391773}, "5": {"carbon_intensity": 51.391773}, "6": {"carbon_intensity": 51.391773}, "7": {"carbon_intensity": 51.391773}, "8": {"carbon_intensity": 51.391773}, "9": {"carbon_intensity": 51.391773}, "10": {"carbon_intensity": 51.391773}, "11": {"carbon_intensity": 51.391773}, "12": {"carbon_intensity": 51.391773}, "13": {"carbon_intensity": 51.391773}, "14": {"carbon_intensity": 51.391773}, "15": {"carbon_intensity": 51.391773}, "16": {"carbon_intensity": 51.391773}, "17": {"carbon_intensity": 51.391773}, "18": {"carbon_intensity": 51.391773}, "19": {"carbon_intensity": 51.391773}, "20": {"carbon_intensity": 51.391773}, "21": {"carbon_intensity": 51.391773}, "22": {"carbon_intensity": 51.391773}, "23": {"carbon_intensity": 51.391773}}, "units": "gCO2eq/kWh", "transmission_distances": {"IntegrationTestProvider:rivendell": 0.0, "IntegrationTestProvider:lothlorien": 83.13919477602013, "IntegrationTestProvider:anduin": 343.4342546548361, "IntegrationTestProvider:fangorn": 356.6428494192109}, "transmission_distances_unit": "km"}'
        ),
        (
            'IntegrationTestProvider:lothlorien', '{"averages": {"overall": {"carbon_intensity": 50.494296}, "0": {"carbon_intensity": 50.494296}, "1": {"carbon_intensity": 50.494296}, "2": {"carbon_intensity": 50.494296}, "3": {"carbon_intensity": 50.494296}, "4": {"carbon_intensity": 50.494296}, "5": {"carbon_intensity": 50.494296}, "6": {"carbon_intensity": 50.494296}, "7": {"carbon_intensity": 50.494296}, "8": {"carbon_intensity": 50.494296}, "9": {"carbon_intensity": 50.494296}, "10": {"carbon_intensity": 50.494296}, "11": {"carbon_intensity": 50.494296}, "12": {"carbon_intensity": 50.494296}, "13": {"carbon_intensity": 50.494296}, "14": {"carbon_intensity": 50.494296}, "15": {"carbon_intensity": 50.494296}, "16": {"carbon_intensity": 50.494296}, "17": {"carbon_intensity": 50.494296}, "18": {"carbon_intensity": 50.494296}, "19": {"carbon_intensity": 50.494296}, "20": {"carbon_intensity": 50.494296}, "21": {"carbon_intensity": 50.494296}, "22": {"carbon_intensity": 50.494296}, "23": {"carbon_intensity": 50.494296}}, "units": "gCO2eq/kWh", "transmission_distances": {"IntegrationTestProvider:rivendell": 83.13919477602013, "IntegrationTestProvider:lothlorien": 0.0, "IntegrationTestProvider:anduin": 411.46013910805647, "IntegrationTestProvider:fangorn": 426.0856966065312}, "transmission_distances_unit": "km"}'
        ),
        (
            'IntegrationTestProvider:anduin', '{"averages": {"overall": {"carbon_intensity": 51.208835}, "0": {"carbon_intensity": 51.208835}, "1": {"carbon_intensity": 51.208835}, "2": {"carbon_intensity": 51.208835}, "3": {"carbon_intensity": 51.208835}, "4": {"carbon_intensity": 51.208835}, "5": {"carbon_intensity": 51.208835}, "6": {"carbon_intensity": 51.208835}, "7": {"carbon_intensity": 51.208835}, "8": {"carbon_intensity": 51.208835}, "9": {"carbon_intensity": 51.208835}, "10": {"carbon_intensity": 51.208835}, "11": {"carbon_intensity": 51.208835}, "12": {"carbon_intensity": 51.208835}, "13": {"carbon_intensity": 51.208835}, "14": {"carbon_intensity": 51.208835}, "15": {"carbon_intensity": 51.208835}, "16": {"carbon_intensity": 51.208835}, "17": {"carbon_intensity": 51.208835}, "18": {"carbon_intensity": 51.208835}, "19": {"carbon_intensity": 51.208835}, "20": {"carbon_intensity": 51.208835}, "21": {"carbon_intensity": 51.208835}, "22": {"carbon_intensity": 51.208835}, "23": {"carbon_intensity": 51.208835}}, "units": "gCO2eq/kWh", "transmission_distances": {"IntegrationTestProvider:rivendell": 343.4342546548361, "IntegrationTestProvider:lothlorien": 411.46013910805647, "IntegrationTestProvider:anduin": 0.0, "IntegrationTestProvider:fangorn": 429.85848503575806}, "transmission_distances_unit": "km"}'
        ), 
        (
            'IntegrationTestProvider:fangorn', '{"averages": {"overall": {"carbon_intensity": 57.265384}, "0": {"carbon_intensity": 57.265384}, "1": {"carbon_intensity": 57.265384}, "2": {"carbon_intensity": 57.265384}, "3": {"carbon_intensity": 57.265384}, "4": {"carbon_intensity": 57.265384}, "5": {"carbon_intensity": 57.265384}, "6": {"carbon_intensity": 57.265384}, "7": {"carbon_intensity": 57.265384}, "8": {"carbon_intensity": 57.265384}, "9": {"carbon_intensity": 57.265384}, "10": {"carbon_intensity": 57.265384}, "11": {"carbon_intensity": 57.265384}, "12": {"carbon_intensity": 57.265384}, "13": {"carbon_intensity": 57.265384}, "14": {"carbon_intensity": 57.265384}, "15": {"carbon_intensity": 57.265384}, "16": {"carbon_intensity": 57.265384}, "17": {"carbon_intensity": 57.265384}, "18": {"carbon_intensity": 57.265384}, "19": {"carbon_intensity": 57.265384}, "20": {"carbon_intensity": 57.265384}, "21": {"carbon_intensity": 57.265384}, "22": {"carbon_intensity": 57.265384}, "23": {"carbon_intensity": 57.265384}}, "units": "gCO2eq/kWh", "transmission_distances": {"IntegrationTestProvider:rivendell": 356.6428494192109, "IntegrationTestProvider:lothlorien": 426.0856966065312, "IntegrationTestProvider:anduin": 429.85848503575806, "IntegrationTestProvider:fangorn": 0.0}, "transmission_distances_unit": "km"}'
        )
    ]
    
    for region in carbon_region_data:
        assert region in expected_carbon_region_data, f"Unexpected carbon data: {region}"

    performance_collector.run()

    performance_region_data = remote_client.select_all_from_table(PERFORMANCE_REGION_TABLE)
    print("Collected performance_region_data:", performance_region_data)
    print("Expected performance_region_data:", expected_performance_region_data)

    assert len(performance_region_data) == 4

    expected_performance_region_data = [
        (
            "IntegrationTestProvider:rivendell",
            '{"relative_performance": 1, "transmission_latency": {"IntegrationTestProvider:rivendell": {"latency_distribution": [10], "unit": "s"}, "IntegrationTestProvider:lothlorien": {"latency_distribution": [150], "unit": "s"}, "IntegrationTestProvider:anduin": {"latency_distribution": [80], "unit": "s"}, "IntegrationTestProvider:fangorn": {"latency_distribution": [200], "unit": "s"}}}',
        ),
        (
            "IntegrationTestProvider:lothlorien",
            '{"relative_performance": 1, "transmission_latency": {"IntegrationTestProvider:rivendell": {"latency_distribution": [150], "unit": "s"}, "IntegrationTestProvider:lothlorien": {"latency_distribution": [10], "unit": "s"}, "IntegrationTestProvider:anduin": {"latency_distribution": [100], "unit": "s"}, "IntegrationTestProvider:fangorn": {"latency_distribution": [250], "unit": "s"}}}',
        ),
        (
            "IntegrationTestProvider:anduin",
            '{"relative_performance": 1, "transmission_latency": {"IntegrationTestProvider:rivendell": {"latency_distribution": [80], "unit": "s"}, "IntegrationTestProvider:lothlorien": {"latency_distribution": [100], "unit": "s"}, "IntegrationTestProvider:anduin": {"latency_distribution": [10], "unit": "s"}, "IntegrationTestProvider:fangorn": {"latency_distribution": [150], "unit": "s"}}}',
        ),
        (
            "IntegrationTestProvider:fangorn",
            '{"relative_performance": 1, "transmission_latency": {"IntegrationTestProvider:rivendell": {"latency_distribution": [200], "unit": "s"}, "IntegrationTestProvider:lothlorien": {"latency_distribution": [250], "unit": "s"}, "IntegrationTestProvider:anduin": {"latency_distribution": [150], "unit": "s"}, "IntegrationTestProvider:fangorn": {"latency_distribution": [10], "unit": "s"}}}',
        ),
    ]

    for region in performance_region_data:
        assert region in expected_performance_region_data, f"Unexpected performance data: {region}"

    print("Data collection successful.")
