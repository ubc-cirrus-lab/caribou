from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator


class EnvironmentVariable(BaseModel):
    name: str = Field(..., title="The name of the environment variable")
    value: str = Field(..., title="The value of the environment variable")


class Provider(BaseModel):
    name: str = Field(..., title="The name of the provider")
    config: dict[str, Any] = Field(..., title="The configuration of the provider")

    @model_validator(mode="after")
    def validate_config(cls: Any, values: Any) -> Any:  # pylint: disable=no-self-argument, unused-argument
        if values.name == "aws":
            config = values.config
            if "memory" not in config or not isinstance(config["memory"], int):
                raise ValueError("The 'config' dictionary must contain 'memory' key with an integer value")
            if "timeout" not in config or not isinstance(config["timeout"], int):
                raise ValueError("The 'config' dictionary must contain 'timeout' key with an integer value")
        return values


class RegionAndProviders(BaseModel):
    only_regions: Optional[List[str]] = Field(None, title="List of regions to deploy to")
    forbidden_regions: Optional[List[str]] = Field(None, title="List of regions to not deploy to")
    providers: List[Provider] = Field(..., title="List of possible providers with their configurations")


class ConfigSchema(BaseModel):
    workflow_name: str = Field(..., title="The name of the workflow")
    environment_variables: List[EnvironmentVariable] = Field(..., title="List of environment variables")
    iam_policy_file: str = Field(..., title="The IAM policy file")
    home_regions: List[List[str]] = Field(..., title="List of home regions")
    regions_and_providers: RegionAndProviders = Field(..., title="List of regions and providers")
