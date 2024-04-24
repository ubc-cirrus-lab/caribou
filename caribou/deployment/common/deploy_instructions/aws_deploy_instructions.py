from caribou.common.provider import Provider
from caribou.deployment.common.deploy.models.iam_role import IAMRole
from caribou.deployment.common.deploy.models.instructions import APICall, Instruction
from caribou.deployment.common.deploy.models.variable import Variable
from caribou.deployment.common.deploy_instructions.deploy_instructions import DeployInstructions


class AWSDeployInstructions(DeployInstructions):
    def __init__(self, region: str, provider: Provider) -> None:
        super().__init__(region, provider)
        self._lambda_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

    def _get_create_iam_role_instruction(self, role: IAMRole, iam_role_varname: str) -> Instruction:
        return APICall(
            name="create_role",
            params={
                "role_name": role.name,
                "trust_policy": self._lambda_trust_policy,
                "policy": role.get_policy(self._provider),
            },
            output_var=iam_role_varname,
        )

    def _get_update_iam_role_instruction(self, role: IAMRole, iam_role_varname: str) -> Instruction:
        return APICall(
            name="update_role",
            params={
                "role_name": role.name,
                "trust_policy": self._lambda_trust_policy,
                "policy": role.get_policy(self._provider),
            },
            output_var=iam_role_varname,
        )

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
        return APICall(
            name="create_function",
            params={
                "function_name": name,
                "role_identifier": Variable(iam_role_varname),
                "zip_contents": zip_contents,
                "runtime": runtime,
                "handler": handler,
                "environment_variables": environment_variables,
                "timeout": self._config["timeout"],
                "memory_size": self._config["memory"],
                "additional_docker_commands": self._config.get("additional_docker_commands", []),
            },
            output_var=function_varname,
        )

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
        return APICall(
            name="update_function",
            params={
                "function_name": name,
                "role_identifier": Variable(iam_role_varname),
                "zip_contents": zip_contents,
                "runtime": runtime,
                "handler": handler,
                "environment_variables": environment_variables,
                "timeout": self._config["timeout"],
                "memory_size": self._config["memory"],
                "additional_docker_commands": self._config.get("additional_docker_commands", []),
            },
            output_var=function_varname,
        )

    def _get_create_messaging_topic_instruction_for_region(self, output_var: str, name: str) -> Instruction:
        return APICall(
            name="create_sns_topic",
            params={
                "topic_name": f"{name}_messaging_topic",
            },
            output_var=output_var,
        )

    def _get_subscribe_messaging_topic_instruction(
        self, messaging_topic_identifier_varname: str, function_varname: str, subscription_varname: str
    ) -> Instruction:
        return APICall(
            name="subscribe_sns_topic",
            params={
                "topic_arn": Variable(messaging_topic_identifier_varname),
                "protocol": "lambda",
                "endpoint": Variable(function_varname),
            },
            output_var=subscription_varname,
        )

    def _add_function_permission_for_messaging_topic_instruction(
        self, messaging_topic_identifier_varname: str, function_varname: str
    ) -> Instruction:
        return APICall(
            name="add_lambda_permission_for_sns_topic",
            params={
                "topic_arn": Variable(messaging_topic_identifier_varname),
                "lambda_function_arn": Variable(function_varname),
            },
        )
