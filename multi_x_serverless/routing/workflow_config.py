import json
from typing import Any

from pydantic import ValidationError

from multi_x_serverless.routing.workflow_config_schema import WorkflowConfigSchema


class WorkflowConfig:
    def __init__(self, workflow_config: dict) -> None:
        self._verify(workflow_config)
        self._workflow_config = workflow_config

    def _verify(self, workflow_config: dict) -> None:
        try:
            WorkflowConfigSchema(**workflow_config)
        except ValidationError as exc:
            raise RuntimeError(f"Invalid workflow config: {exc}") from exc

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
        result = self._lookup("num_calls_in_one_month")
        return result if result is not None else 100

    @property
    def solver(self) -> str:
        allowed_solvers = {"coarse_grained_solver", "fine_grained_solver", "stochastic_heuristic_solver"}
        result = self._lookup("solver")

        if result not in allowed_solvers:
            if result is None:
                return "coarse_grained_solver"
            raise ValueError(f"Invalid solver: {result}")

        return result

    def write_back(self, key: str, value: Any) -> None:
        self._workflow_config[key] = value

    def _lookup(self, key: str) -> Any:
        return self._workflow_config.get(key)

    def to_json(self) -> str:
        return json.dumps(self._workflow_config)

    @property
    def regions_and_providers(self) -> dict:
        return self._lookup("regions_and_providers")

    @property
    def instances(self) -> list[dict]:
        return self._lookup("instances")

    @property
    def constraints(self) -> dict:
        return self._lookup("constraints")

    @property
    def start_hops(self) -> dict:
        start_hops = self._lookup("start_hops")
        if start_hops is None or len(start_hops) == 0:
            return {}
        return start_hops[0]
