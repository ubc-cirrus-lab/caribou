import json
from typing import Any

import numpy as np


class WorkflowConfig:
    def __init__(self, workflow_config: dict) -> None:
        self._verify(workflow_config)
        self._workflow_config = workflow_config

    def _verify(self, workflow_config: dict) -> None:
        # TODO (#8): Implement verification of workflow config, should raise an exception if not verified
        pass

    @property
    def workflow_name(self) -> str:
        return self._lookup("workflow_name")

    @property
    def workflow_id(self) -> str:
        return self._lookup("workflow_id")

    def _lookup(self, key: str) -> Any:
        return self._workflow_config.get(key)

    def to_json(self) -> str:
        return json.dumps(self._workflow_config)

    def resolve_functions(self) -> np.ndarray:
        functions = [instance["function_name"] for instance in self.instances]
        functions = list(set(functions))
        return np.array(functions)

    @property
    def regions_and_providers(self) -> dict:
        return self._lookup("regions_and_providers")

    @property
    def functions(self) -> list[dict]:
        return self._lookup("functions")

    @property
    def instances(self) -> list[dict]:
        return self._lookup("instances")

    @property
    def constraints(self) -> dict:
        return self._lookup("constraints")
    
    @property
    def start_hops(self) -> dict:
        # TODO (#68): Allow for multiple start hops / home regions
        start_hops = self._lookup("start_hops")
        if start_hops is None or len(start_hops) == 0:
            return []
        else:
            return start_hops[0]
