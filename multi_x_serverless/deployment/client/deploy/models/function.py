from __future__ import annotations

import json
from typing import Sequence

from multi_x_serverless.deployment.client.deploy.models.deployment_package import DeploymentPackage
from multi_x_serverless.deployment.client.deploy.models.enums import Endpoint
from multi_x_serverless.deployment.client.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.client.deploy.models.instructions import (
    APICall,
    Instruction,
    RecordResourceValue,
    RecordResourceVariable,
)
from multi_x_serverless.deployment.client.deploy.models.remote_state import RemoteState
from multi_x_serverless.deployment.client.deploy.models.resource import Resource
from multi_x_serverless.deployment.client.deploy.models.variable import Variable


class Function(Resource):  # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        name: str,
        entry_point: bool,
        timeout: int,
        memory: int,
        region_group: str,
        role: IAMRole,
        deployment_package: DeploymentPackage,
        environment_variables: dict[str, str],
        handler: str,
        runtime: str,
        home_regions: list[str],
    ) -> None:
        super().__init__(name, "function")
        self.entry_point = entry_point
        self.timeout = timeout
        self.memory = memory
        self.region_group = region_group
        self._remote_states: dict[Endpoint, dict[str, RemoteState]] = {}
        self.initialise_remote_states(home_regions)
        self.role = role
        self.deployment_package = deployment_package
        self.environment_variables = environment_variables
        self.handler = handler
        self.runtime = runtime
        self.home_regions = home_regions

    def __str__(self) -> str:
        return f"""Function({self.name}): 
                   {self.region_group}-{self.home_regions}-{self.entry_point}-{self.timeout}-
                   {self.memory}-{self.role}-{self.deployment_package}-{self.environment_variables}-
                   {self.handler}-{self.runtime}"""

    def initialise_remote_states(self, home_regions: list[str]) -> None:
        for home_region in home_regions:
            endpoint, region = home_region.split(":")
            endpoint_type = Endpoint(endpoint)
            if endpoint_type not in self._remote_states:
                self._remote_states[endpoint_type] = {}
            self._remote_states[endpoint_type][region] = RemoteState(endpoint=endpoint_type, region=region)

    def dependencies(self) -> Sequence[Resource]:
        resources: list[Resource] = [self.role, self.deployment_package]
        return resources

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        instructions: dict[str, list[Instruction]] = {}
        for home_region in self.home_regions:
            endpoint, region = home_region.split(":")
            if endpoint == Endpoint.AWS.value:
                instruction = self.get_deployment_instructions_aws(region)
            elif endpoint == Endpoint.GCP.value:
                instruction = self.get_deployment_instructions_gcp(region)
            else:
                raise RuntimeError(f"Unknown endpoint {endpoint}")
            instructions[home_region] = instruction
        return instructions

    def get_deployment_instructions_aws(self, region: str) -> list[Instruction]:
        instructions: list[Instruction] = []
        instructions.append(self.get_sns_topic_instruction_for_region(region))
        iam_role_varname = f"{self.role.name}_role_arn_{region}"
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
        with open(self.role.policy, "r", encoding="utf-8") as f:
            policy = f.read()
        if policy is None:
            raise RuntimeError(f"Lambda policy could not be read, check the path ({self.role.policy})")
        policy = json.dumps(json.loads(policy))
        if not self._remote_states[Endpoint.AWS][region].resource_exists(self.role):
            instructions.extend(
                [
                    APICall(
                        name="create_role",
                        params={
                            "role_name": self.role.name,
                            "trust_policy": lambda_trust_policy,
                            "policy": policy,
                        },
                        output_var=iam_role_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="iam_role",
                        resource_name=iam_role_varname,
                        name="role_arn",
                        variable_name=iam_role_varname,
                    ),
                    RecordResourceValue(
                        resource_type="iam_role",
                        resource_name=iam_role_varname,
                        name="role_name",
                        arn=function_varname,
                        value=self.role.name,
                    ),
                ]
            )
        else:
            instructions.extend(
                [
                    APICall(
                        name="update_role",
                        params={
                            "role_name": self.role.name,
                            "trust_policy": lambda_trust_policy,
                            "policy": policy,
                        },
                        output_var=iam_role_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="iam_role",
                        resource_name=iam_role_varname,
                        name="role_arn",
                        variable_name=iam_role_varname,
                    ),
                    RecordResourceValue(
                        resource_type="iam_role",
                        resource_name=iam_role_varname,
                        name="role_name",
                        arn=function_varname,
                        value=self.role.name,
                    ),
                ]
            )

        if self.deployment_package.filename is None:
            raise RuntimeError("Deployment package has not been built")

        with open(self.deployment_package.filename, "rb") as f:
            zip_contents = f.read()
        function_varname = f"{self.name}_lambda_arn_{region}"
        if not self._remote_states[Endpoint.AWS][region].resource_exists(self):
            instructions.extend(
                [
                    APICall(
                        name="create_function",
                        params={
                            "function_name": self.name,
                            "role_arn": Variable(iam_role_varname),
                            "zip_contents": zip_contents,
                            "runtime": self.runtime,
                            "handler": self.handler,
                            "environment_variables": self.environment_variables,
                            "timeout": self.timeout,
                            "memory_size": self.memory,
                        },
                        output_var=function_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="function",
                        resource_name=self.name,
                        name="function_arn",
                        variable_name=function_varname,
                    ),
                    RecordResourceValue(
                        resource_type="function",
                        resource_name=self.name,
                        name="function_name",
                        arn=function_varname,
                        value=self.name,
                    ),
                ]
            )
        else:
            instructions.extend(
                [
                    APICall(
                        name="update_function",
                        params={
                            "function_name": self.name,
                            "role_arn": Variable(iam_role_varname),
                            "zip_contents": zip_contents,
                            "runtime": self.runtime,
                            "handler": self.handler,
                            "environment_variables": self.environment_variables,
                            "timeout": self.timeout,
                            "memory_size": self.memory,
                        },
                        output_var=function_varname,
                    ),
                    RecordResourceVariable(
                        resource_type="function",
                        resource_name=self.name,
                        name="function_arn",
                        variable_name=function_varname,
                    ),
                    RecordResourceValue(  # TODO: Check for region
                        resource_type="function",
                        resource_name=self.name,
                        name="function_name",
                        arn=function_varname,
                        value=self.name,
                    ),
                ]
            )
        return instructions

    def get_sns_topic_instruction_for_region(self, region: str) -> Instruction:
        return APICall(
            name="create_sns_topic",
            params={
                "topic_name": f"{self.name}_{region}_sns_topic",
            },
            output_var=f"{self.name}_{region}_sns_topic",
        )

    def get_deployment_instructions_gcp(self, region: str) -> list[Instruction]:  # pylint: disable=unused-argument
        return []
