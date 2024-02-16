import json
from typing import Any

from pydantic import ValidationError

from multi_x_serverless.routing.workflow_config_schema import WorkflowConfigSchema


class WorkflowConfig:
    def __init__(self, workflow_config: dict) -> None:
        self._verify(workflow_config)
        self._workflow_config = workflow_config
        self._modified_regions_and_providers = self._create_altered_regions_and_providers(
            self._workflow_config["regions_and_providers"]
        )
        self._modified_instances = self._create_altered_instances(self._workflow_config["instances"])

    def _verify(self, workflow_config: dict) -> None:
        try:
            WorkflowConfigSchema(**workflow_config)
        except ValidationError as exc:
            raise RuntimeError(f"Invalid workflow config: {exc}") from exc

    def _create_altered_instances(self, instances: list[dict]) -> list[dict]:
        altered_instances = []

        for instance in instances:
            altered_instance = {
                "instance_name": instance["instance_name"],
                "function_name": instance["function_name"],
                "regions_and_providers": self._create_altered_regions_and_providers(instance["regions_and_providers"]),
                "succeeding_instances": instance.get("succeeding_instances", []),
                "preceding_instances": instance.get("preceding_instances", []),
            }

            altered_instances.append(altered_instance)

        return altered_instances

    def _create_altered_regions_and_providers(self, regions_and_providers: dict) -> dict:
        altered_regions_and_providers = {"providers": regions_and_providers.get("providers", {})}
        allowed_regions = []
        disallowed_regions = []

        # We want to change the format of the internal data of
        # regions_and_providers to be in format of "provider_name:region_name"
        # We will start with the allowed regions
        for provider_regions in regions_and_providers.get("allowed_regions", []):
            provider = provider_regions["provider"]
            region = provider_regions["region"]
            allowed_regions.append(f"{provider}:{region}")

        # Now we will do the same for the disallowed regions
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
    def solver(self) -> str:
        allowed_solvers = {"coarse_grained_solver", "fine_grained_solver", "stochastic_heuristic_solver"}
        result = self._lookup("solver", "coarse_grained_solver")
        if result not in allowed_solvers:
            raise ValueError(f"Invalid solver: {result}")
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
    def instances(self) -> list[dict]:
        return self._modified_instances

    @property
    def constraints(self) -> dict:
        return self._lookup("constraints")

    @property
    def start_hops(self) -> str:
        raw_start_hops = self._lookup("start_hops")

        if raw_start_hops is None or len(raw_start_hops) == 0:
            raise ValueError("No start hops found")

        # TODO (#68): Allow for multiple "home region" or start hops

        # Start hop is in format of {"provider": "aws", "region": "us-west-2"}
        # we want it in format of "provider_name:region_name"
        start_hops = []
        for start_hop in raw_start_hops:
            start_hops.append(f"{start_hop['provider']}:{start_hop['region']}")

        return start_hops[0]
