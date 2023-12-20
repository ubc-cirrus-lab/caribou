from __future__ import annotations

from typing import Optional, Sequence

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.models.instructions import Instruction


class Resource:
    def __init__(self, name: str, resource_type: str, config: Optional[Config] = None) -> None:
        self.name = name
        self.resource_type = resource_type
        self._config = config

    def dependencies(self) -> Sequence[Resource]:
        return []

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        return {}
