from __future__ import annotations

from typing import Sequence

from multi_x_serverless.deployment.client.deploy.models.instructions import Instruction


class Resource:
    def __init__(self, name: str, resource_type: str) -> None:
        self.name = name
        self.resource_type = resource_type

    def dependencies(self) -> Sequence[Resource]:
        return []

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        return {}

    def __repr__(self) -> str:
        return f"""Resource(
                        name={self.name},
                        resource_type={self.resource_type},
                """
