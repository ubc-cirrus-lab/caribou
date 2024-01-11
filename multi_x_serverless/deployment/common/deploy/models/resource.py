from __future__ import annotations

from typing import Sequence

from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction


class Resource:
    def __init__(self, name: str, resource_type: str) -> None:
        self.name = name
        self.resource_type = resource_type

    def dependencies(self) -> Sequence[Resource]:
        return []

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        return {}

    def __repr__(self) -> str:
        return f"Resource(name={self.name}, resource_type={self.resource_type}, "

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Resource):
            return NotImplemented
        return self.name == other.name and self.resource_type == other.resource_type
