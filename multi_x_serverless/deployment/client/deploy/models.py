from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from multi_x_serverless.deployment.client.config import Config


@dataclass
class Instance:
    name: str


@dataclass
class Variable:
    name: str


@dataclass
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
    resource_name: str
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
    def __init__(self, name: str, resource_type: str) -> None:
        self._name = name
        self._resource_type = resource_type

    def name(self) -> str:
        return self._name

    def resource_type(self) -> str:
        return self._resource_type

    def dependencies(self) -> list[Resource]:
        return []

    def get_deployment_instructions(
        self, config: Config, endpoint: Optional[Endpoint]  # pylint: disable=unused-argument
    ) -> list[Instruction]:
        return []


class RemoteState:
    def __init__(self, endpoint) -> None:
        self._endpoint = endpoint

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


@dataclass
class Function(Resource):  # pylint: disable=too-many-instance-attributes
    resource_type: str = "function"
    environment_variables: dict[str, str]
    runtime: str
    handler: str
    timeout: int
    memory: int
    role: IAMRole
    deployment_package: DeploymentPackage
    region_group: str
    remote_state: RemoteState = field(default_factory=RemoteState)

    def dependencies(self) -> list[Resource]:
        resources: list[Resource] = [self.role, self.deployment_package]
        return resources

    def get_deployment_instructions(self, _: Config, endpoint: Endpoint) -> list[Instruction]:
        if endpoint == Endpoint.AWS:
            return self.get_deployment_instructions_aws()
        if endpoint == Endpoint.GCP:
            return self.get_deployment_instructions_gcp()
        raise RuntimeError(f"Unknown endpoint {endpoint}")

    def get_deployment_instructions_aws(self) -> list[Instruction]:
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
        if not self.remote_state.resource_exists(self.role):
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
        if not self.remote_state.resource_exists(self):
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

    def get_deployment_instructions_gcp(self) -> list[Instruction]:
        pass


class Workflow(Resource):
    def __init__(
        self, name: str, resources: list[Function], functions: list[FunctionInstance], edges: list[tuple[str, str]]
    ) -> None:
        self._resources = resources
        self._functions = functions
        self._edges = edges

        super().__init__(name, "workflow")

    def dependencies(self) -> list[Resource]:
        return self._resources

    def add_resource(self, resource: Resource) -> None:
        self._resources.append(resource)

    def get_deployment_instructions(self, config: Config, _: Endpoint) -> dict[Endpoint, list[Instruction]]:
        plans: dict[Endpoint, list[Instruction]] = []
        for resource in self._resources:
            for endpoint in Endpoint:
                if endpoint not in plans:
                    plans[endpoint] = []
                result = resource.get_deployment_instructions(config, endpoint)
                if result:
                    for instruction in result:
                        plans[endpoint].append(instruction)
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
    instructions: dict[Endpoint, list[Instruction]] = field(default_factory=dict)


class IAMRole(Resource):
    def __init__(self, policy: str, role_name: str) -> None:
        super().__init__(role_name, "iam_role")
        self.policy = policy

    def dependencies(self) -> list[Resource]:
        return [self.policy]


@dataclass
class DeploymentPackage(Resource):
    filename: Optional[str] = None
