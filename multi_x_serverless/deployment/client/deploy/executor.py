from typing import Any

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.models.deployment_plan import DeploymentPlan
from multi_x_serverless.deployment.client.deploy.models.instructions import APICall, Instruction, RecordResourceVariable
from multi_x_serverless.deployment.client.deploy.models.resource import Resource
from multi_x_serverless.deployment.client.deploy.models.variable import Variable
from multi_x_serverless.deployment.client.factories.remote_client_factory import RemoteClientFactory
from multi_x_serverless.deployment.client.remote_client.remote_client import RemoteClient


class Executor:
    def __init__(self, config: Config) -> None:
        self.resource_values: dict[str, list[Any]] = {}
        self.variables: dict[str, Any] = {}
        self._config = config
        self._remote_client_factory = RemoteClientFactory()

    def execute(self, deployment_plan: DeploymentPlan) -> None:
        for home_region, instructions in deployment_plan.instructions.items():
            endpoint, region = home_region.split(":")
            client = self._remote_client_factory.get_remote_client(endpoint, region)
            for instruction in instructions:
                getattr(self, f"_do_{instruction.__class__.__name__.lower()}", self._default_handler)(
                    instruction,
                    client,
                )

    def _do_apicall(self, instruction: APICall, client: RemoteClient) -> None:
        final_kwargs = self.__resolve_variables(instruction)
        try:
            method = getattr(client, instruction.name)
        except AttributeError as e:
            raise RuntimeError(
                f"Unknown method {instruction.name} for client {client.__class__.__name__}, is the client configured correctly?"  # pylint: disable=line-too-long
            ) from e
        response = method(**final_kwargs)
        if instruction.output_var is not None:
            self.variables[instruction.output_var] = response

    def _do_recordresourcevariable(self, instruction: RecordResourceVariable, _: RemoteClient) -> None:
        self.__do_recordresourcevariable(instruction)

    def __do_recordresourcevariable(self, instruction: RecordResourceVariable) -> None:
        payload = {
            "name": instruction.resource_name,
            "resource_type": instruction.resource_type,
            instruction.name: self.variables[instruction.variable_name],
        }
        self.__add_to_deployed_values(payload)

    def __add_to_deployed_values(self, payload: dict[str, str]) -> None:
        resource_type = payload["resource_type"]
        if resource_type not in self.resource_values:
            self.resource_values[resource_type] = []
        self.resource_values[resource_type].append(payload)

    def __resolve_variables(self, call: APICall) -> dict[str, Any]:
        params = call.params
        for key, value in params.items():
            if isinstance(value, Variable):
                params[key] = self.variables[value.name]
        return params

    def _default_handler(self, instruction: Instruction, client: RemoteClient) -> None:
        raise RuntimeError(f"Unknown instruction type: {instruction.__class__.__name__}")

    def get_deployed_resources(self) -> list[Resource]:
        deployed_resources: list[Resource] = []
        for resource_type, resource_names in self.resource_values.items():
            for resource_name in resource_names:
                deployed_resources.append(Resource(resource_type, resource_name))
        return deployed_resources
