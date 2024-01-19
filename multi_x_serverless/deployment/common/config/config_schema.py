from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator


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
        for provider in values.providers.keys():
            if provider not in ["aws", "gcp"]:
                raise ValueError(f"Provider {provider} is not supported")
            if provider == "aws":
                config = values.providers[provider].config
                if "memory" not in config or not isinstance(config["memory"], int):
                    raise ValueError("The 'config' dictionary must contain 'memory' key with an integer value")
                if "timeout" not in config or not isinstance(config["timeout"], int):
                    raise ValueError("The 'config' dictionary must contain 'timeout' key with an integer value")
        return values


class ConfigSchema(BaseModel):
    workflow_name: str = Field(..., title="The name of the workflow")
    workflow_version: str = Field(..., title="The version of the workflow")
    environment_variables: List[EnvironmentVariable] = Field(..., title="List of environment variables")
    iam_policy_file: str = Field(..., title="The IAM policy file")
    home_regions: List[ProviderRegion] = Field(..., title="List of home regions")
    regions_and_providers: RegionAndProviders = Field(..., title="List of regions and providers")
