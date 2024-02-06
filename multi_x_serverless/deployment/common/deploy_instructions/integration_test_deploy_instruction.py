from typing import Any

from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.instructions import Instruction
from multi_x_serverless.deployment.common.deploy_instructions.deploy_instructions import DeployInstructions


class IntegrationTestDeployInstructions(DeployInstructions):
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

    def _get_subscribe_messaging_topic_instruction(
        self, messaging_topic_identifier_varname: str, function_varname: str, subscription_varname: str
    ) -> Instruction:
        raise NotImplementedError

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

    def _get_update_iam_role_instruction(self, role: IAMRole, iam_role_varname: str) -> Instruction:
        raise NotImplementedError

    def _get_create_iam_role_instruction(self, role: IAMRole, iam_role_varname: str) -> Instruction:
        raise NotImplementedError

    def _get_messaging_topic_instruction_for_region(self, output_var: str, name: str) -> Instruction:
        raise NotImplementedError

    def _add_function_permission_for_messaging_topic_instruction(
        self, messaging_topic_identifier_varname: str, function_varname: str
    ) -> Instruction:
        raise NotImplementedError

    def _get_config(self, providers: dict[str, Any], provider: str) -> dict[str, Any]:
        raise NotImplementedError
