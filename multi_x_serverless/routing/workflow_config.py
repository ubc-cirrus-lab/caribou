import json
from typing import Any

from pydantic import ValidationError

from multi_x_serverless.routing.workflow_config_schema import WorkflowConfigSchema


class WorkflowConfig:
    def __init__(self, workflow_config: dict) -> None:
        self._verify(workflow_config)
        self._workflow_config = workflow_config
        self._modified_regions_and_providers = self.create_altered_regions_and_providers(
            self._workflow_config["regions_and_providers"]
        )

    def _verify(self, workflow_config: dict) -> None:
        try:
            WorkflowConfigSchema(**workflow_config)
        except ValidationError as exc:
            raise RuntimeError(f"Invalid workflow config: {exc}") from exc

    def create_altered_regions_and_providers(self, regions_and_providers: dict) -> dict:
        altered_regions_and_providers = {"providers": regions_and_providers.get("providers", {})}
        allowed_regions = []
        disallowed_regions = []

        # We want to change the format of the internal data of
        # regions_and_providers to be in format of "provider_name:region_name"
        # We will start with the allowed regions
        if "allowed_regions" not in regions_and_providers or regions_and_providers["allowed_regions"] is None:
            regions_and_providers["allowed_regions"] = []
        for provider_regions in regions_and_providers.get("allowed_regions", []):
            provider = provider_regions["provider"]
            region = provider_regions["region"]
            allowed_regions.append(f"{provider}:{region}")

        # Now we will do the same for the disallowed regions
        if "disallowed_regions" not in regions_and_providers or regions_and_providers["disallowed_regions"] is None:
            regions_and_providers["disallowed_regions"] = []
        for provider_regions in regions_and_providers.get("disallowed_regions", []):
            provider = provider_regions["provider"]
            region = provider_regions["region"]
            disallowed_regions.append(f"{provider}:{region}")

        if allowed_regions:
            altered_regions_and_providers["allowed_regions"] = allowed_regions
        if disallowed_regions:
            altered_regions_and_providers["disallowed_regions"] = disallowed_regions

        return altered_regions_and_providers

    @property
    def workflow_name(self) -> str:
        return self._lookup("workflow_name")

    @property
    def workflow_version(self) -> str:
        return self._lookup("workflow_version")

    @property
    def workflow_id(self) -> str:
        return self._lookup("workflow_id")

    @property
    def num_calls_in_one_month(self) -> int:
        return self._lookup("num_calls_in_one_month", 100)

    @property
    def deployment_algorithm(self) -> str:
        allowed_deployment_algorithms = {
            "coarse_grained_deployment_algorithm",
            "fine_grained_deployment_algorithm",
            "stochastic_heuristic_deployment_algorithm",
        }
        result = self._lookup("deployment_algorithm", "coarse_grained_deployment_algorithm")
        if len(result) == 0:
            result = "coarse_grained_deployment_algorithm"
        if result not in allowed_deployment_algorithms:
            raise ValueError(f"Invalid deployment algorithm: {result}")
        return result

    def write_back(self, key: str, value: Any) -> None:
        self._workflow_config[key] = value

    def _lookup(self, key: str, default: Any = None) -> Any:
        return self._workflow_config.get(key, default)

    def to_json(self) -> str:
        return json.dumps(self._workflow_config)

    @property
    def regions_and_providers(self) -> dict:
        return self._modified_regions_and_providers

    @property
    def instances(self) -> dict[str, dict[str, Any]]:
        return self._lookup("instances")

    @property
    def constraints(self) -> dict:
        return self._lookup("constraints")

    @property
    def home_region(self) -> str:
        raw_home_region = self._lookup("home_region")

        if raw_home_region is None:
            raise ValueError("No home region found")

        # Start hop is in format of {"provider": "aws", "region": "us-west-2"}
        # we want it in format of "provider_name:region_name"

        return f"{raw_home_region['provider']}:{raw_home_region['region']}"
