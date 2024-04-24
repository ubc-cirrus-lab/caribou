from __future__ import annotations

from typing import Any, Sequence

from caribou.deployment.common.deploy.models.deployment_package import DeploymentPackage
from caribou.deployment.common.deploy.models.iam_role import IAMRole
from caribou.deployment.common.deploy.models.instructions import Instruction
from caribou.deployment.common.deploy.models.remote_state import RemoteState
from caribou.deployment.common.deploy.models.resource import Resource
from caribou.deployment.common.factories.deploy_instruction_factory import DeployInstructionFactory


class Function(Resource):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        name: str,
        entry_point: bool,
        role: IAMRole,
        deployment_package: DeploymentPackage,
        environment_variables: dict[str, str],
        handler: str,
        runtime: str,
        deploy_region: dict[str, str],
        providers: dict[str, Any],
        deploy: bool = True,
    ) -> None:
        super().__init__(name, "function")
        self.entry_point = entry_point
        self.initialise_remote_state(deploy_region)
        self.role = role
        self.deployment_package = deployment_package
        self.environment_variables = environment_variables
        self.handler = handler
        self.runtime = runtime
        self.deploy_region = deploy_region
        self.providers = providers
        self.deploy = deploy

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "entry_point": self.entry_point,
            "role": self.role.to_json(),
            "environment_variables": self.environment_variables,
            "handler": self.handler,
            "runtime": self.runtime,
            "providers": self.providers,
        }

    def __repr__(self) -> str:
        return f"Function({self.name}): Entry point: {self.entry_point}, Role: {self.role}, Deployment package: {self.deployment_package}, Environment variables: {self.environment_variables}, Handler: {self.handler}, Runtime: {self.runtime}, Deploy regions: {self.deploy_region}, Providers: {self.providers}"  # pylint: disable=line-too-long

    def initialise_remote_state(self, deploy_region: dict[str, str]) -> None:
        provider, region = deploy_region["provider"], deploy_region["region"]
        self._remote_state = RemoteState(provider=provider, region=region)

    def dependencies(self) -> Sequence[Resource]:
        resources: list[Resource] = [self.role, self.deployment_package]
        return resources

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        instructions: dict[str, list[Instruction]] = {}
        provider, region = self.deploy_region["provider"], self.deploy_region["region"]
        deploy_instruction = DeployInstructionFactory.get_deploy_instructions(provider, region)
        if self.deployment_package.filename is None:
            raise RuntimeError("Deployment package has not been built")
        instructions[f"{provider}:{region}"] = deploy_instruction.get_deployment_instructions(
            self.name,
            self.role,
            self.providers,
            self.runtime,
            self.handler,
            self.environment_variables,
            self.deployment_package.filename,
            self._remote_state,
            self._remote_state.resource_exists(self),
        )
        return instructions

    def get_deployment_instructions_gcp(self, region: str) -> list[Instruction]:  # pylint: disable=unused-argument
        raise NotImplementedError()
