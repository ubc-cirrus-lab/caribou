from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.clients import AWSClient, Client


@dataclass
class Instance:
    name: str


@dataclass
class Variable:
    name: str


@dataclass(frozen=True)
class Instruction:
    pass


@dataclass(frozen=True)
class RecordResourceVariable(Instruction):
    resource_type: str
    resource_name: str
    name: str
    variable_name: str


@dataclass(frozen=True)
class RecordResourceValue(Instruction):
    resource_type: str
    resource_name: str
    name: str
    value: Any


@dataclass
class FunctionInstance(Instance):
    entry_point: bool
    timeout: int
    memory: int
    region_group: str

    def to_json(self) -> dict:
        """
        Get the JSON representation of this function.
        """
        return {
            "name": self.name,
            "entry_point": self.entry_point,
            "timeout": self.timeout,
            "memory": self.memory,
            "region_group": self.region_group,
        }


class Resource:
    def __init__(self, name: str, resource_type: str, config: Optional[Config] = None) -> None:
        self._name = name
        self._resource_type = resource_type
        self._config = config

    def name(self) -> str:
        return self._name

    def resource_type(self) -> str:
        return self._resource_type

    def dependencies(self) -> list[Resource]:
        return []

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        return {}


class RemoteState:
    def __init__(self, endpoint: Endpoint, region: str) -> None:
        self._endpoint = endpoint
        self._client = self.initialise_client(endpoint, region)

    @staticmethod
    def initialise_client(endpoint: Endpoint, region: str) -> Client:
        if endpoint == Endpoint.AWS:
            return AWSClient(region)
        if endpoint == Endpoint.GCP:
            return NotImplementedError()
        raise RuntimeError(f"Unknown endpoint {endpoint}")

    def resource_exists(self, resource: Resource) -> bool:
        if self._endpoint == Endpoint.AWS:
            return self.resource_exists_aws(resource)
        if self._endpoint == Endpoint.GCP:
            return self.resource_exists_gcp(resource)
        raise RuntimeError(f"Unknown endpoint {self._endpoint}")

    def resource_exists_aws(self, resource: Resource) -> bool:
        if resource.resource_type() == "iam_role":
            return self.aws_iam_role_exists(resource)
        if resource.resource_type() == "function":
            return self.aws_lambda_function_exists(resource)
        raise RuntimeError(f"Unknown resource type {resource.resource_type()}")

    def aws_iam_role_exists(self, resource: Resource) -> bool:
        pass

    def aws_lambda_function_exists(self, resource: Resource) -> bool:
        pass

    def resource_exists_gcp(self, resource: Resource) -> bool:
        pass


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
        self.deployment_name = deployment_name
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

    def initialise_remote_states(self, home_regions: list[str]) -> None:
        for home_region in home_regions:
            endpoint, region = home_region.split(":")
            if endpoint not in self._remote_states:
                self._remote_states[endpoint] = {}
            self._remote_states[endpoint][region] = RemoteState(endpoint)

    def dependencies(self) -> list[Resource]:
        resources: list[Resource] = [self.role, self.deployment_package]
        return resources

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        instructions: dict[str, list[Instruction]] = {}
        for home_region in self.home_regions:
            endpoint, region = home_region.split(":")
            if endpoint == Endpoint.AWS:
                instruction = self.get_deployment_instructions_aws(region)
            elif endpoint == Endpoint.GCP:
                instruction = self.get_deployment_instructions_gcp(region)
            else:
                raise RuntimeError(f"Unknown endpoint {endpoint}")
            instructions[home_region] = instruction
        return instructions

    def get_deployment_instructions_aws(self, region: str) -> list[APICall]:
        api_calls: list[APICall] = []
        iam_role_varname = f"{self.role.name}_role_arn"
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
        if not self._remote_states[Endpoint.AWS][region].resource_exists(self.role):
            api_calls.extend(
                [
                    APICall(
                        method_name="create_role",
                        params={
                            "role_name": self.role.name,
                            "trust_policy": lambda_trust_policy,
                            "policy": self.role.policy,
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
                        value=self.role.name,
                    ),
                ]
            )
        else:
            api_calls.extend(
                [
                    APICall(
                        method_name="update_role",
                        params={
                            "role_name": self.role.name,
                            "trust_policy": lambda_trust_policy,
                            "policy": self.role.policy,
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
                        value=self.role.name,
                    ),
                ]
            )

        with open(self.deployment_package.filename, "rb") as f:
            zip_contents = f.read()
        function_varname = f"{self.name}_lambda_arn"
        if not self._remote_states[Endpoint.AWS][region].resource_exists(self):
            api_calls.extend(
                [
                    APICall(
                        method_name="create_function",
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
                        value=self.name,
                    ),
                ]
            )
        else:
            api_calls.extend(
                [
                    APICall(
                        method_name="update_function",
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
                        value=self.name,
                    ),
                ]
            )
        return api_calls

    def get_deployment_instructions_gcp(self, region: str) -> list[Instruction]:  # pylint: disable=unused-argument
        return []


class Workflow(Resource):
    def __init__(
        self,
        name: str,
        resources: list[Function],
        functions: list[FunctionInstance],
        edges: list[tuple[str, str]],
    ) -> None:
        self._resources = resources
        self._functions = functions
        self._edges = edges
        super().__init__(name, "workflow")

    def dependencies(self) -> list[Function]:
        return self._resources

    def get_deployment_instructions(self) -> dict[str, list[Instruction]]:
        plans: dict[str, list[Instruction]] = {}
        for resource in self._resources:
            result = resource.get_deployment_instructions()
            if result:
                for region, instructions in result.items():
                    if region not in plans:
                        plans[region] = []
                    plans[region].extend(instructions)
        return plans


class Endpoint(Enum):
    AWS = "aws"
    GCP = "gcp"


@dataclass(frozen=True)
class APICall(Instruction):
    method_name: str
    params: dict[str, Any]
    output_var: Optional[str] = None


@dataclass
class DeploymentPlan:
    instructions: dict[str, list[Instruction]] = field(default_factory=dict)


class IAMRole(Resource):
    def __init__(self, policy: str, role_name: str) -> None:
        super().__init__(role_name, "iam_role")
        self.policy = policy

    def dependencies(self) -> list[Resource]:
        return []


@dataclass
class DeploymentPackage(Resource):
    filename: Optional[str] = None
