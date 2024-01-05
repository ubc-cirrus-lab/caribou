import importlib
import os
import sys

import yaml
from botocore.session import Session
from pydantic import ValidationError

from multi_x_serverless.deployment.client import __version__ as MULTI_X_SERVERLESS_VERSION
from multi_x_serverless.deployment.client.cli.config_schema import ConfigSchema
from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.deployer import (
    Deployer,
    create_default_deployer,
    create_deletion_deployer,
)
from multi_x_serverless.deployment.client.enums import Endpoint
from multi_x_serverless.deployment.client.multi_x_serverless_workflow import MultiXServerlessWorkflow


class CLIFactory:
    def __init__(self, project_dir: str):
        self.project_dir = project_dir

    def create_config_obj(self) -> Config:
        try:
            project_config = self.load_project_config()
        except (OSError, IOError) as exc:
            raise RuntimeError("Could not load project config") from exc
        except ValueError as exc:
            raise RuntimeError(f"Unable to parse project config: {exc}") from exc
        self._validate_config(project_config)
        project_config["workflow_app"] = self.load_workflow_app()
        return Config(project_config, self.project_dir)

    def create_session(self) -> Session:
        session = Session()
        self._add_user_agent(session)
        return session

    def _add_user_agent(self, session: Session) -> None:
        suffix = f"{session.user_agent_name}/{session.user_agent_version}"
        session.user_agent_name = "multi-x-serverless"
        session.user_agent_version = MULTI_X_SERVERLESS_VERSION
        session.user_agent_extra = suffix

    def create_deployer(self, config: Config, session: Session) -> Deployer:
        return create_default_deployer(config, session)

    def create_deletion_deployer(self, config: Config, session: Session) -> Deployer:
        return create_deletion_deployer(config, session)

    def load_project_config(self) -> dict:
        config_file = os.path.join(self.project_dir, ".multi-x-serverless", "config.yml")
        with open(config_file, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _validate_config(self, project_config: dict) -> None:
        if not isinstance(project_config, dict):
            raise RuntimeError("project config must be a dictionary")

        try:
            ConfigSchema(**project_config)
        except ValidationError as exc:
            raise RuntimeError(f"Invalid project config: {exc}") from exc

        self.__validate_only_regions_and_providers(project_config)

    def __validate_only_regions_and_providers(self, project_config: dict) -> None:
        if (
            "constraints" in project_config
            and "regions_and_providers" in project_config["constraints"]
            and "only_regions" in project_config["constraints"]["regions_and_providers"]
        ):
            possible_endpoints = [endpoint.value for endpoint in Endpoint]
            defined_endpoints = [
                provider["name"] for provider in project_config["providers"] if provider["name"] in possible_endpoints
            ]
            only_regions = project_config["constraints"]["regions_and_providers"]["only_regions"]
            if not only_regions:
                only_regions = []
            if only_regions and not isinstance(only_regions, list):
                raise RuntimeError("only_regions must be a list")
            for region in only_regions:
                if not isinstance(region, str):
                    raise RuntimeError("only_regions must be a list of strings")
                provider = region.split(":", maxsplit=1)[0]
                if provider not in Endpoint.__members__:
                    raise RuntimeError(f"Provider {provider} is not supported")
                if provider not in defined_endpoints:
                    raise RuntimeError(f"Provider {provider} is not defined in providers")

    def load_workflow_app(self) -> MultiXServerlessWorkflow:
        if self.project_dir not in sys.path:
            sys.path.insert(0, self.project_dir)

        try:
            workflow = importlib.import_module("app")
            workflow_app = getattr(workflow, "workflow")
        except SyntaxError as e:
            raise RuntimeError(f"Unable to import app.py file: {e}") from e
        except ModuleNotFoundError as e:
            raise RuntimeError(f"Unable to import app.py file: {e}") from e
        return workflow_app
