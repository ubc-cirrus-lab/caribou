from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction
from multi_x_serverless.deployment.common.deploy.models.remote_state import RemoteState
from multi_x_serverless.deployment.common.provider import Provider


class DeployInstructions(ABC):
    def __init__(self, region: str, provider: Provider) -> None:
        self._region = region
        self._provider = provider

    @abstractmethod
    def get_deployment_instructions(
        self,
        name: str,
        role: IAMRole,
        providers: dict[str, Any],
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        filename: str,
        remote_state: RemoteState,
        function_exists: bool,
    ) -> list[Instruction]:
        raise NotImplementedError
