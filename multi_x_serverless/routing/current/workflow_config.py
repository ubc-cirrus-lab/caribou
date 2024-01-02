import json
from typing import Any


class WorkflowConfig:
    def __init__(self, workflow_config: dict):
        self._verify(workflow_config)
        self._workflow_config = workflow_config

    def _verify(self, workflow_config: dict) -> None:
        # TODO (#8): Implement verification of workflow config, should raise an exception if not verified
        pass

    @property
    def workflow_name(self) -> str:
        return self._lookup("workflow_name")

    def _lookup(self, key: str) -> Any:
        return self._workflow_config.get(key)

    def to_json(self) -> str:
        return json.dumps(self._workflow_config)
