import sys
from typing import Any, Optional

from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessWorkflow


class Config:
    def __init__(self, project_config: dict, project_dir: Optional[str]) -> None:
        self.project_config = project_config
        self.project_dir = project_dir

    def __repr__(self) -> str:
        return f"Config(project_config={self.project_config}, project_dir={self.project_dir})"

    @property
    def workflow_app(self) -> MultiXServerlessWorkflow:
        workflow = self._lookup("workflow_app")

        if not isinstance(workflow, MultiXServerlessWorkflow):
            raise RuntimeError("workflow_app must be a Workflow instance")

        return workflow

    @property
    def workflow_name(self) -> str:
        return self._lookup("workflow_name")

    def _lookup(self, key: str) -> Any:
        return self.project_config.get(key, {})

    @property
    def python_version(self) -> str:
        sys_version = sys.version_info
        major, minor = sys_version.major, sys_version.minor

        if major == 2:
            return "python2.7"
        if (major, minor) <= (3, 6):
            return "python3.6"
        if (major, minor) <= (3, 10):
            return f"python{major}.{minor}"

        return "python3.11"

    @property
    def environment_variables(self) -> dict[str, str]:
        list_of_env_variables: list[dict] = self._lookup("environment_variables")
        if list_of_env_variables is None:
            return {}
        env_variables: dict[str, str] = {}
        for env_variable in list_of_env_variables:
            if not isinstance(env_variable["value"], str):
                raise RuntimeError("Environment variable value need to be a str")
            if not isinstance(env_variable["key"], str):
                raise RuntimeError("Environment variable key need to be a str")
            env_variables[env_variable["key"]] = env_variable["value"]
        return env_variables

    @property
    def home_regions(self) -> list[dict[str, str]]:
        return self._lookup("home_regions")

    @property
    def estimated_invocations_per_month(self) -> int:
        # TODO (#27): Implement and incorporate Free Tier considerations into data_sources
        return self._lookup("estimated_invocations_per_month")

    @property
    def constraints(self) -> dict:
        return self._lookup("constraints")

    @property
    def regions_and_providers(self) -> dict:
        return self._lookup("regions_and_providers")

    @property
    def iam_policy_file(self) -> str:
        return self._lookup("iam_policy_file")
