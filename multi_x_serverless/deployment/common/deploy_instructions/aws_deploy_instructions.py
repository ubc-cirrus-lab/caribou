import json
from typing import Any

from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.deploy.models.instructions import APICall, Instruction, RecordResourceVariable
from multi_x_serverless.deployment.common.deploy.models.remote_state import RemoteState
from multi_x_serverless.deployment.common.deploy.models.variable import Variable
from multi_x_serverless.deployment.common.deploy_instructions.deploy_instructions import DeployInstructions
from multi_x_serverless.deployment.common.enums import Provider


class AWSDeployInstructions(DeployInstructions):
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
        memory, timeout = self._get_memory_and_timeout(providers)
        instructions: list[Instruction] = []
        sns_topic_arn_varname = f"{name}_{self._region}_sns_topic"
        instructions.extend(
            [
                self.get_sns_topic_instruction_for_region(sns_topic_arn_varname, name),
                RecordResourceVariable(
                    resource_type="sns_topic",
                    resource_name=f"{name}_{self._region}_sns_topic",
                    name="topic_arn",
                    variable_name=sns_topic_arn_varname,
                ),
            ]
        )
        iam_role_varname = f"{role.name}_role_arn_{self._region}"
        lambda_trust_policy = {
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
        with open(role.policy, "r", encoding="utf-8") as f:
            policy = f.read()
        if policy is None:
            raise RuntimeError(f"Lambda policy could not be read, check the path ({role.policy})")
        policy = json.dumps(json.loads(policy))
        if not remote_state.resource_exists(role):
            instructions.extend(
                [
                    APICall(
                        name="create_role",
                        params={
                            "role_name": role.name,
                            "trust_policy": lambda_trust_policy,
                            "policy": policy,
                        },
                        output_var=iam_role_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="iam_role",
                        resource_name=f"{name}_{self._region}_iam_role",
                        name="role_arn",
                        variable_name=iam_role_varname,
                    ),
                ]
            )
        else:
            instructions.extend(
                [
                    APICall(
                        name="update_role",
                        params={
                            "role_name": role.name,
                            "trust_policy": lambda_trust_policy,
                            "policy": policy,
                        },
                        output_var=iam_role_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="iam_role",
                        resource_name=f"{name}_{self._region}_iam_role",
                        name="role_arn",
                        variable_name=iam_role_varname,
                    ),
                ]
            )

        with open(filename, "rb") as f:
            zip_contents = f.read()
        function_varname = f"{name}_lambda_arn_{self._region}"
        if not function_exists:
            instructions.extend(
                [
                    APICall(
                        name="create_function",
                        params={
                            "function_name": name,
                            "role_arn": Variable(iam_role_varname),
                            "zip_contents": zip_contents,
                            "runtime": runtime,
                            "handler": handler,
                            "environment_variables": environment_variables,
                            "timeout": timeout,
                            "memory_size": memory,
                        },
                        output_var=function_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="function",
                        resource_name=f"{name}_{self._region}_function",
                        name="function_arn",
                        variable_name=function_varname,
                    ),
                ]
            )
        else:
            instructions.extend(
                [
                    APICall(
                        name="update_function",
                        params={
                            "function_name": name,
                            "role_arn": Variable(iam_role_varname),
                            "zip_contents": zip_contents,
                            "runtime": runtime,
                            "handler": handler,
                            "environment_variables": environment_variables,
                            "timeout": timeout,
                            "memory_size": memory,
                        },
                        output_var=function_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="function",
                        resource_name=f"{name}_{self._region}_function",
                        name="function_arn",
                        variable_name=function_varname,
                    ),
                ]
            )
        subscription_varname = f"{name}_{self._region}_sns_subscription"
        instructions.extend(
            [
                APICall(
                    name="subscribe_sns_topic",
                    params={
                        "topic_arn": Variable(sns_topic_arn_varname),
                        "protocol": "lambda",
                        "endpoint": Variable(function_varname),
                    },
                    output_var=subscription_varname,
                ),
                RecordResourceVariable(
                    resource_type="sns_topic",
                    resource_name=f"{name}_{self._region}_sns_subscription",
                    name="topic_arn",
                    variable_name=subscription_varname,
                ),
                APICall(
                    name="add_lambda_permission_for_sns_topic",
                    params={
                        "topic_arn": Variable(sns_topic_arn_varname),
                        "lambda_function_arn": Variable(function_varname),
                    },
                ),
            ]
        )
        return instructions

    def get_sns_topic_instruction_for_region(self, output_var: str, name: str) -> Instruction:
        return APICall(
            name="create_sns_topic",
            params={
                "topic_name": f"{name}_{self._region}_sns_topic",
            },
            output_var=output_var,
        )

    def _get_memory_and_timeout(self, providers: dict[str, Any]) -> tuple[int, int]:
        return providers["aws"]["memory"], providers["aws"]["timeout"]
