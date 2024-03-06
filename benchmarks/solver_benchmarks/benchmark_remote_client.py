from multi_x_serverless.common.models.remote_client.mock_remote_client import MockRemoteClient
from multi_x_serverless.common.constants import (
    AVAILABLE_REGIONS_TABLE,
    CARBON_REGION_TABLE,
    PROVIDER_REGION_TABLE,
    PERFORMANCE_REGION_TABLE,
    WORKFLOW_INSTANCE_TABLE,
)
import json
import random


class BenchmarkRemoteClient(MockRemoteClient):
    def __init__(self, regions: list[str], seed, config) -> None:
        super().__init__()
        self._regions = regions
        self._seed = seed
        self._config = config
        self._random = random.Random(seed)
        self._carbon_data = {}
        self._datacenter_data = {}
        self._performance_data = {}
        self._workflow_data = {}
        self._setup_auxiliary_data()

    def _setup_auxiliary_data(self) -> None:
        for region in self._regions:
            self._setup_carbon_data(region)
            self._setup_datacenter_data(region)
            self._setup_performance_data(region)
            self._setup_workflow_data()

    def _setup_workflow_data(self) -> None:
        self._workflow_data[self._config["workflow_id"]] = {}

        for instance in self._config["instances"]:
            self._workflow_data[self._config["workflow_id"]][instance] = {
                "execution_summary": {
                    region: {"runtime_distribution": [self._random.lognormvariate(0.5, 0.3) for _ in range(100)]}
                    for region in self._regions
                },
                "invocation_summary": {},
            }
            for to_instance in self._config["instances"]:
                self._workflow_data[self._config["workflow_id"]][instance]["invocation_summary"][to_instance] = {
                    "probability_of_invocation": self._random.uniform(0, 1),
                    "data_transfer_size_distribution": [self._random.lognormvariate(0.01, 0.3) for _ in range(100)],
                    "transmission_summary": {
                        from_region: {
                            to_region: {"latency_distribution": [self._random.lognormvariate(0.08, 0.3) for _ in range(100)]}
                            for to_region in self._regions
                        }
                        for from_region in self._regions
                    },
                }

    def _setup_performance_data(self, region: str) -> None:
        performance_data = {
            "relative_performance": 1,
            "transmission_latency": {},
        }
        for to_region in self._regions:
            performance_data["transmission_latency"][to_region] = {
                "latency_distribution": [self._random.lognormvariate(0.1, 0.04) for _ in range(100)]
            }
        self._performance_data[region] = performance_data

    def _setup_datacenter_data(self, region: str) -> None:
        datacenter_data = {
            "pue": 1.15,
            "cfe": 0.9,
            "average_memory_power": 3.92e-6,
            "average_cpu_power": 0.00212,
            "available_architectures": ["arm64", "x86_64"],
        }
        execution_cost = {
            "invocation_cost": {
                "arm64": self._random.lognormvariate(1e-7, 5e-7),
                "x86_64": self._random.lognormvariate(1e-7, 5e-7),
                "free_tier_invocations": 1000000,
            },
            "compute_cost": {
                "arm64": self._random.lognormvariate(1e-5, 5e-5),
                "x86_64": self._random.lognormvariate(1e-5, 5e-5),
                "free_tier_compute_gb_s": 400000,
            },
            "unit": "USD",
        }
        transmission_cost = {
            "global_data_transfer": self._random.uniform(0.09, 0.18),
            "provider_data_transfer": self._random.uniform(0.01, 0.09),
            "unit": "USD/GB",
        }
        datacenter_data["execution_cost"] = execution_cost
        datacenter_data["transmission_cost"] = transmission_cost

        self._datacenter_data[region] = datacenter_data

    def _setup_carbon_data(self, region: str) -> None:
        region_carbon_data = {}
        region_carbon_data["carbon_intensity"] = self._random.uniform(0, 500)
        region_carbon_data["transmission_carbon"] = {}
        for to_region in self._regions:
            region_carbon_data["transmission_carbon"][to_region] = {
                "carbon_intensity": self._random.uniform(0, 5),
                "distance": self._random.uniform(0, 500),
            }

        self._carbon_data[region] = region_carbon_data

    def get_keys(self, table_name: str) -> list[str]:
        if table_name == AVAILABLE_REGIONS_TABLE:
            return self._regions

    def get_value_from_table(self, table_name: str, key: str) -> str:
        if table_name == CARBON_REGION_TABLE:
            return json.dumps(self._carbon_data[key])
        elif table_name == PROVIDER_REGION_TABLE:
            return json.dumps(self._datacenter_data[key])
        elif table_name == PERFORMANCE_REGION_TABLE:
            return json.dumps(self._performance_data[key])
        elif table_name == WORKFLOW_INSTANCE_TABLE:
            return json.dumps(self._workflow_data[key])
