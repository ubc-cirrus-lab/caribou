from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator

from caribou.common.provider import Provider as ProviderEnum


class EnvironmentVariable(BaseModel):
    key: str = Field(..., title="The name of the environment variable")
    value: str = Field(..., title="The value of the environment variable")


class Provider(BaseModel):
    config: dict[str, Any] = Field(..., title="The configuration of the provider")


class ProviderRegion(BaseModel):
    provider: str = Field(..., title="The name of the provider")
    region: str = Field(..., title="The name of the region")


class RegionAndProviders(BaseModel):
    allowed_regions: Optional[List[ProviderRegion]] = Field(None, title="List of regions to deploy to")
    disallowed_regions: Optional[List[ProviderRegion]] = Field(None, title="List of regions to not deploy to")
    providers: Dict[str, Provider] = Field(..., title="List of possible providers with their configurations")

    @model_validator(mode="after")
    def validate_config(cls: Any, values: Any) -> Any:  # pylint: disable=no-self-argument, unused-argument
        provider_values = [provider.value for provider in ProviderEnum]
        for provider in values.providers.keys():
            if provider not in provider_values:
                raise ValueError(f"Provider {provider} is not supported")
            if provider in ("aws", "provider1"):
                config = values.providers[provider].config
                if "memory" not in config or not isinstance(config["memory"], int):
                    raise ValueError("The 'config' dictionary must contain 'memory' key with an integer value")
                if "timeout" not in config or not isinstance(config["timeout"], int):
                    raise ValueError("The 'config' dictionary must contain 'timeout' key with an integer value")
        return values


class Constraint(BaseModel):
    value: float = Field(..., title="The value of the constraint")
    type: str = Field(..., title="The type of the constraint")

    @model_validator(mode="after")
    def validate_config(cls: Any, values: Any) -> Any:  # pylint: disable=no-self-argument, unused-argument
        if values.type not in ["absolute", "relative"]:
            raise ValueError(f"Constraint type {values.type} is not supported")


class Constraints(BaseModel):
    hard_resource_constraints: Dict[str, Optional[Constraint]] = Field(..., title="Hard resource constraints")
    soft_resource_constraints: Dict[str, Optional[Constraint]] = Field(..., title="Soft resource constraints")
    priority_order: List[str] = Field(..., title="Order of priorities for the constraints")

    @model_validator(mode="after")
    def validate_config(cls: Any, values: Any) -> Any:  # pylint: disable=no-self-argument, unused-argument
        possible_constraints = ["cost", "runtime", "carbon"]
        for value in values.priority_order:
            if value not in possible_constraints:
                raise ValueError(f"Priority order value {value} is not supported")

        for constraint in values.hard_resource_constraints.keys():
            if constraint not in possible_constraints:
                raise ValueError(f"Hard resource constraint {constraint} is not supported")

        for constraint in values.soft_resource_constraints.keys():
            if constraint not in possible_constraints:
                raise ValueError(f"Soft resource constraint {constraint} is not supported")
        return values


class ConfigSchema(BaseModel):
    workflow_name: str = Field(..., title="The name of the workflow")
    workflow_version: str = Field(..., title="The version of the workflow")
    environment_variables: Optional[List[EnvironmentVariable]] = Field(None, title="List of environment variables")
    iam_policy_file: str = Field(..., title="The IAM policy file")
    home_region: ProviderRegion = Field(..., title="Home region of the application")
    regions_and_providers: RegionAndProviders = Field(..., title="List of regions and providers")
    constraints: Constraints = Field(..., title="Constraints")
    num_calls_in_one_month: Optional[int] = Field(None, title="Number of calls in one month")
    deployment_algorithm: Optional[str] = Field(None, title="The deployment algorithm to use")
