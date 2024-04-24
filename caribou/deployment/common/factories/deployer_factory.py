import importlib
import os
import sys
from typing import Optional

import yaml
from pydantic import ValidationError

from caribou.common.provider import Provider
from caribou.deployment.client import __version__ as MULTI_X_SERVERLESS_VERSION
from caribou.deployment.client.caribou_workflow import CaribouWorkflow
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.config.config_schema import ConfigSchema
from caribou.deployment.common.deploy.deployer import Deployer, create_default_deployer


class DeployerFactory:
    def __init__(self, project_dir: Optional[str]) -> None:
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

    def create_config_obj_from_dict(self, deployment_config: dict) -> Config:
        self._validate_config(deployment_config)
        return Config(deployment_config, self.project_dir)

    def create_deployer(self, config: Config) -> Deployer:
        return create_default_deployer(config)

    def load_project_config(self) -> dict:
        if self.project_dir is None:
            raise RuntimeError("project_dir must be defined")
        config_file = os.path.join(self.project_dir, ".caribou", "config.yml")
        with open(config_file, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _validate_config(self, project_config: dict) -> None:
        if not isinstance(project_config, dict):
            raise RuntimeError("project config must be a dictionary")

        try:
            ConfigSchema(**project_config)
        except ValidationError as exc:
            raise RuntimeError(f"Invalid project config: {exc}") from exc

        self.__validate_allowed_and_disallowed_regions_and_providers(project_config)

    def __validate_allowed_and_disallowed_regions_and_providers(  # pylint: disable=too-many-branches
        self, project_config: dict
    ) -> None:
        if "regions_and_providers" not in project_config:
            raise RuntimeError("regions_and_providers must be defined in project config")
        if not isinstance(project_config["regions_and_providers"], dict):
            raise RuntimeError("regions_and_providers must be a dictionary")
        if "providers" not in project_config["regions_and_providers"]:
            raise RuntimeError("at least one provider must be defined in regions_and_providers")
        if not isinstance(project_config["regions_and_providers"]["providers"], dict):
            raise RuntimeError("providers must be a dictionary")
        if (
            "allowed_regions" in project_config["regions_and_providers"]
            or "disallowed_regions" in project_config["regions_and_providers"]
        ):
            possible_providers = [provider.value for provider in Provider]
            defined_providers = [
                provider_name
                for provider_name in project_config["regions_and_providers"]["providers"].keys()
                if provider_name in possible_providers
            ]
            allowed_regions_collection = set()
            if "allowed_regions" in project_config["regions_and_providers"]:
                allowed_regions = project_config["regions_and_providers"]["allowed_regions"]
                if not allowed_regions:
                    allowed_regions = []
                if allowed_regions and not isinstance(allowed_regions, list):
                    raise RuntimeError("allowed_regions must be a list")
                for provider_region in allowed_regions:
                    if "provider" not in provider_region or "region" not in provider_region:
                        raise RuntimeError(f"Region {provider_region} must have both provider and region defined")
                    allowed_regions_collection.add(f"{provider_region['provider']}-{provider_region['region']}")
                    if not isinstance(provider_region, dict):
                        raise RuntimeError("allowed_regions must be a list of strings")
                    provider = provider_region["provider"]
                    if provider not in [provider.value for provider in Provider]:
                        raise RuntimeError(f"Provider {provider} is not supported")
                    if provider not in defined_providers:
                        raise RuntimeError(f"Provider {provider} is not defined in providers")
            if "disallowed_regions" in project_config["regions_and_providers"]:
                disallowed_regions = project_config["regions_and_providers"]["disallowed_regions"]
                if not disallowed_regions:
                    disallowed_regions = []
                if disallowed_regions and not isinstance(disallowed_regions, list):
                    raise RuntimeError("disallowed_regions must be a list")
                for provider_region in disallowed_regions:
                    if "provider" not in provider_region or "region" not in provider_region:
                        raise RuntimeError(f"Region {provider_region} must have both provider and region defined")
                    if f"{provider_region['provider']}-{provider_region['region']}" in allowed_regions_collection:
                        raise RuntimeError(f"Region {provider_region} cannot be both allowed and disallowed")
                    if not isinstance(provider_region, dict):
                        raise RuntimeError("disallowed_regions must be a list of strings")
                    provider = provider_region["provider"]
                    if provider not in [provider.value for provider in Provider]:
                        raise RuntimeError(f"Provider {provider} is not supported")
                    if provider not in defined_providers:
                        raise RuntimeError(f"Provider {provider} is not defined in providers")
                    if provider_region == project_config["home_region"]:
                        raise RuntimeError(f"Region {provider_region} cannot be both home and disallowed")

    def load_workflow_app(self) -> CaribouWorkflow:
        if self.project_dir is None:
            raise RuntimeError("project_dir must be defined")
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
