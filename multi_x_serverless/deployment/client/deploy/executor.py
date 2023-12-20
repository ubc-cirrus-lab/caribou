from typing import Any, Optional

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.clients import AWSClient
from multi_x_serverless.deployment.client.deploy.models import (
    APICall,
    DeploymentPlan,
    Instruction,
    RecordResourceValue,
    RecordResourceVariable,
    Resource,
    Variable,
)


class Executor:
    def __init__(self, config: Config) -> None:
        self.resource_values: dict[str, list[Any]] = {}
        self.variables: dict[str, Any] = {}
        self._config = config
        self._aws_client: Optional[AWSClient] = None

    def execute(self, deployment_plan: DeploymentPlan) -> None:
        for home_region, instructions in deployment_plan.instructions.items():
            endpoint, region = home_region.split(":")
            self._aws_client = AWSClient(region)
            for instruction in instructions:
                getattr(self, f"_do_{instruction.__class__.__name__.lower()}_{endpoint}", self._default_handler)(
                    instruction
                )

    def _do_apicall_aws(self, instruction: APICall) -> None:
        final_kwargs = self._resolve_variables(instruction)
        method = getattr(self._aws_client, instruction.name)
        response = method(**final_kwargs)
        if instruction.output_var is not None:
            self.variables[instruction.output_var] = response

    def _do_apicall_gcp(self, instruction: APICall) -> None:
        pass

    def _do_recordresourcevariable_aws(self, instruction: RecordResourceVariable) -> None:
        self._do_recordresourcevariable(instruction)

    def _do_recordresourcevariable_gcp(self, instruction: RecordResourceVariable) -> None:
        self._do_recordresourcevariable(instruction)

    def _do_recordresourcevariable(self, instruction: RecordResourceVariable) -> None:
        payload = {
            "name": instruction.resource_name,
            "resource_type": instruction.resource_type,
            instruction.name: self.variables[instruction.variable_name],
        }
        self._add_to_deployed_values(payload)

    def _do_recordresourcevalue_aws(self, instruction: RecordResourceValue) -> None:
        self._do_recordresourcevalue(instruction)

    def _do_recordresourcevalue_gcp(self, instruction: RecordResourceValue) -> None:
        self._do_recordresourcevalue(instruction)

    def _do_recordresourcevalue(self, instruction: RecordResourceValue) -> None:
        payload = {
            "name": instruction.resource_name,
            "resource_type": instruction.resource_type,
            instruction.name: instruction.value,
        }
        self._add_to_deployed_values(payload)

    def _add_to_deployed_values(self, payload: dict[str, str]) -> None:
        resource_type = payload["resource_type"]
        if resource_type not in self.resource_values:
            self.resource_values[resource_type] = []
        self.resource_values[resource_type].append(payload)

    def _resolve_variables(self, call: APICall) -> dict[str, Any]:
        params = call.params
        for key, value in params.items():
            if isinstance(value, Variable):
                params[key] = self.variables[value.name]
        return params

    def _default_handler(self, instruction: Instruction) -> None:
        raise RuntimeError(f"Unknown instruction type: {instruction.__class__.__name__}")

    def get_deployed_resources(self) -> list[Resource]:
        deployed_resources: list[Resource] = []
        for resource_type, resource_names in self.resource_values.items():
            for resource_name in resource_names:
                deployed_resources.append(Resource(resource_type, resource_name))
        return deployed_resources
