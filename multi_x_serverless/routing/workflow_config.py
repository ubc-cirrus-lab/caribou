import json
from typing import Any

import numpy as np


class WorkflowConfig:
    def __init__(self, workflow_config: dict) -> None:
        self._verify(workflow_config)
        self._workflow_config = workflow_config
        self._functions: np.ndarray = self.resolve_functions()

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
        functions = [instance["function_name"] for instance in self._lookup("instances")]
        functions = list(set(functions))
        return np.array(functions)

    @property
    def functions(self) -> np.ndarray:
        return self._functions

    @property
    def instances(self) -> np.ndarray:
        return np.array(self._lookup("instances"))

    @property
    def constraints(self) -> dict:
        return self._lookup("constraints")
