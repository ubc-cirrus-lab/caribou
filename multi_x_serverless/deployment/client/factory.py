import os
import sys
import importlib
import yaml
from botocore.session import Session
from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.wrapper import MultiXServerlessWorkflow
from multi_x_serverless.deployment.client.deploy.deployer import (
    Deployer,
    create_default_deployer,
    create_deletion_deployer
)
from multi_x_serverless.deployment.client.constants import (
    multi_x_serverless_version,
)


class CLIFactory(object):
    def __init__(self, project_dir, environ=None):
        self.project_dir = project_dir
        self.environ = environ or os.environ

    def create_config_obj(self) -> Config:
        try:
            project_config = self.load_project_config()
        except (OSError, IOError):
            raise RuntimeError("Could not load project config")
        except ValueError as e:
            raise RuntimeError("Unable to parse project config: %s" % e)
        self._validate_config(project_config)
        project_config["workflow_app"] = self.load_workflow_app()
        return Config(project_config, self.project_dir)

    def create_session(self) -> Session:
        session = Session()
        self._add_user_agent(session)
        return session

    def _add_user_agent(self, session):
        suffix = "%s/%s" % (session.user_agent_name, session.user_agent_version)
        session.user_agent_name = "multi-x-serverless"
        session.user_agent_version = multi_x_serverless_version
        session.user_agent_extra = suffix

    def create_deployer(self, config, session) -> Deployer:
        create_default_deployer(config, session)
    
    def create_deletion_deployer(self, config, session) -> Deployer:
        create_deletion_deployer(config, session)

    def load_project_config(self) -> dict:
        config_file = os.path.join(self.project_dir, ".multi-x-serverless", "config.yml")
        with open(config_file) as f:
            return yaml.safe_load(f)

    def _validate_config(self, project_config: dict):
        if not isinstance(project_config, dict):
            raise RuntimeError("project config must be a dictionary")

        # Check if the config adheres to the schema
        # TODO: Implement this

    def load_workflow_app(self) -> MultiXServerlessWorkflow:
        if self.project_dir not in sys.path:
            sys.path.insert(0, self.project_dir)

        try:
            workflow = importlib.import_module("workflow")
            workflow_app = getattr(workflow, "workflow")
        except SyntaxError as e:
            raise RuntimeError("Unable to import workflow.py file: %s" % e)
        return workflow_app
