from typing import Any

from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.common.models.remote_client.remote_client_factory import RemoteClientFactory
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.models.deployment_plan import DeploymentPlan
from multi_x_serverless.deployment.common.deploy.models.instructions import APICall, Instruction, RecordResourceVariable
from multi_x_serverless.deployment.common.deploy.models.variable import Variable


class Executor:
    def __init__(self, config: Config) -> None:
        self.resource_values: dict[str, list[Any]] = {}
        self.variables: dict[str, Any] = {}
        self._config = config

    def execute(self, deployment_plan: DeploymentPlan) -> None:
        for provider_region_to_deploy, instructions in deployment_plan.instructions.items():
            provider, region = provider_region_to_deploy.split(":")
            client = RemoteClientFactory.get_remote_client(provider, region)
            client.create_sync_tables()
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
        try:
            response = method(**final_kwargs)
        except Exception as e:
            raise RuntimeError(f"Error while executing {instruction.name} on {client.__class__.__name__}") from e
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
            if isinstance(value, list):
                params[key] = [self.variables[item.name] if isinstance(item, Variable) else item for item in value]
            if isinstance(value, dict):
                params[key] = {k: self.variables[v.name] if isinstance(v, Variable) else v for k, v in value.items()}
        return params

    def _default_handler(self, instruction: Instruction, client: RemoteClient) -> None:
        raise RuntimeError(f"Unknown instruction type: {instruction.__class__.__name__}")
