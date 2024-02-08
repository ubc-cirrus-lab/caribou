from abc import ABC, abstractmethod
from typing import Any

from multi_x_serverless.common.provider import Provider
from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction, RecordResourceVariable
from multi_x_serverless.deployment.common.deploy.models.remote_state import RemoteState


class DeployInstructions(ABC):
    def __init__(self, region: str, provider: Provider) -> None:
        self._region = region
        self._provider = provider
        self._config: dict[str, Any] = {}

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
        self._config = self._get_config(providers, self._provider.value)
        instructions: list[Instruction] = []
        messaging_topic_identifier_varname = f"{name}_messaging_topic"
        instructions.extend(
            [
                self._get_create_messaging_topic_instruction_for_region(messaging_topic_identifier_varname, name),
                RecordResourceVariable(
                    resource_type="messaging_topic",
                    resource_name=name,
                    name="topic_identifier",
                    variable_name=messaging_topic_identifier_varname,
                ),
            ]
        )
        iam_role_varname = f"{role.name}_role_identifier"

        if not remote_state.resource_exists(role):
            instructions.extend(
                [
                    self._get_create_iam_role_instruction(role, iam_role_varname),
                    RecordResourceVariable(
                        resource_type="iam_role",
                        resource_name=f"{name}_iam_role",
                        name="role_identifier",
                        variable_name=iam_role_varname,
                    ),
                ]
            )
        else:
            instructions.extend(
                [
                    self._get_update_iam_role_instruction(role, iam_role_varname),
                    RecordResourceVariable(
                        resource_type="iam_role",
                        resource_name=f"{name}_iam_role",
                        name="role_identifier",
                        variable_name=iam_role_varname,
                    ),
                ]
            )

        with open(filename, "rb") as f:
            zip_contents = f.read()
        function_varname = f"{name}_function_identifier"
        if not function_exists:
            instructions.extend(
                [
                    self._get_create_function_instruction(
                        name,
                        iam_role_varname,
                        zip_contents,
                        runtime,
                        handler,
                        environment_variables,
                        function_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="function",
                        resource_name=name,
                        name="function_identifier",
                        variable_name=function_varname,
                    ),
                ]
            )
        else:
            instructions.extend(
                [
                    self._get_update_function_instruction(
                        name,
                        iam_role_varname,
                        zip_contents,
                        runtime,
                        handler,
                        environment_variables,
                        function_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="function",
                        resource_name=name,
                        name="function_identifier",
                        variable_name=function_varname,
                    ),
                ]
            )
        subscription_varname = f"{name}_messaging_subscription"
        instructions.extend(
            [
                self._get_subscribe_messaging_topic_instruction(
                    messaging_topic_identifier_varname, function_varname, subscription_varname
                ),
                RecordResourceVariable(
                    resource_type="messaging_topic_subscription",
                    resource_name=f"{name}_messaging_subscription",
                    name="topic_identifier",
                    variable_name=subscription_varname,
                ),
                self._add_function_permission_for_messaging_topic_instruction(
                    messaging_topic_identifier_varname, function_varname
                ),
            ]
        )
        return instructions

    @abstractmethod
    def _get_create_function_instruction(
        self,
        name: str,
        iam_role_varname: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        function_varname: str,
    ) -> Instruction:
        raise NotImplementedError

    @abstractmethod
    def _get_subscribe_messaging_topic_instruction(
        self, messaging_topic_identifier_varname: str, function_varname: str, subscription_varname: str
    ) -> Instruction:
        raise NotImplementedError

    @abstractmethod
    def _get_update_function_instruction(
        self,
        name: str,
        iam_role_varname: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        function_varname: str,
    ) -> Instruction:
        raise NotImplementedError

    @abstractmethod
    def _get_update_iam_role_instruction(self, role: IAMRole, iam_role_varname: str) -> Instruction:
        raise NotImplementedError

    @abstractmethod
    def _get_create_iam_role_instruction(self, role: IAMRole, iam_role_varname: str) -> Instruction:
        raise NotImplementedError

    @abstractmethod
    def _get_create_messaging_topic_instruction_for_region(self, output_var: str, name: str) -> Instruction:
        raise NotImplementedError

    @abstractmethod
    def _add_function_permission_for_messaging_topic_instruction(
        self, messaging_topic_identifier_varname: str, function_varname: str
    ) -> Instruction:
        raise NotImplementedError

    def _get_config(self, providers: dict[str, Any], provider: str) -> dict[str, Any]:
        return providers[provider]["config"]
