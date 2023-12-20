from dataclasses import dataclass, field

from multi_x_serverless.deployment.client.deploy.models.instructions import Instruction


@dataclass
class DeploymentPlan:
    instructions: dict[str, list[Instruction]] = field(default_factory=dict)

    def __str__(self) -> str:
        return "\n".join([f"{region}: {instructions}" for region, instructions in self.instructions.items()])
