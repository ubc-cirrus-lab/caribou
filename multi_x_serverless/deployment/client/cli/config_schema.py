from typing import List

from pydantic import BaseModel, Field


class EnvironmentVariable(BaseModel):
    name: str = Field(..., title="The name of the environment variable")
    value: str = Field(..., title="The value of the environment variable")


class ConfigSchema(BaseModel):
    workflow_name: str = Field(..., title="The name of the workflow")
    environment_variables: List[EnvironmentVariable] = Field(..., title="List of environment variables")
    iam_policy_file: str = Field(..., title="The IAM policy file")
    home_regions: List[str] = Field(..., title="List of home regions")
