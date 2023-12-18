import os
import sys
from typing import Any

import yaml

from multi_x_serverless.deployment.client.deploy.models import Resource
from multi_x_serverless.deployment.client.wrapper import MultiXServerlessWorkflow


class Config(object):
    def __init__(self, project_config: dict, project_dir: str) -> None:
        self.project_config = project_config
        self.project_dir = project_dir
        self._workflow_app = None

    @property
    def workflow_app(self) -> MultiXServerlessWorkflow:
        workflow = self._lookup("workflow_app")

        if not isinstance(workflow, MultiXServerlessWorkflow):
            raise RuntimeError("workflow_app must be a Workflow instance")

        return workflow

    @property
    def workflow_name(self) -> str:
        return self._lookup("workflow_name")

    def _lookup(self, key: str) -> MultiXServerlessWorkflow:
        return self.project_config.get(key)

    @property
    def python_version(self) -> str:
        major, minor = sys.version_info[0], sys.version_info[1]

        if major == 2:
            return "python2.7"
        elif (major, minor) <= (3, 6):
            return "python3.6"
        elif (major, minor) <= (3, 10):
            return "python%s.%s" % (major, minor)

        return "python3.11"

    @property
    def project_dir(self) -> str:
        return self._project_dir

    @property
    def environment_variables(self) -> dict[str, Any]:
        return self._lookup("environment_variables")

    @property
    def home_regions(self) -> list[str]:
        return self._lookup("home_regions")

    @property
    def iam_policy_file(self) -> str:
        return self._lookup("iam_policy_file")

    def deployed_resources(self):
        deployed_resource_file = os.path.join(self.project_dir, ".multi-x-serverless", "deployed_resources.yml")
        with open(deployed_resource_file) as f:
            data = yaml.safe_load(f)

        if data is not None:
            return data["resources"]

    def update_deployed_resources(self, deployed_resources: list[Resource]) -> None:
        deployed_resource_file = os.path.join(self.project_dir, ".multi-x-serverless", "deployed_resources.yml")
        with open(deployed_resource_file, "w") as f:
            yaml.dump({"resources": deployed_resources}, f)
