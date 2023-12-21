from typing import Optional

import botocore.exceptions
from botocore.session import Session
import json

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.deploy.deployment_packager import DeploymentPackager
from multi_x_serverless.deployment.client.deploy.executor import Executor
from multi_x_serverless.deployment.client.deploy.models import DeploymentPlan, Resource, Workflow
from multi_x_serverless.deployment.client.deploy.workflow_builder import WorkflowBuilder


class Deployer:  # pylint: disable=too-few-public-methods
    def __init__(
        self,
        config: Config,
        session: Session,
        workflow_builder: WorkflowBuilder,
        deployment_packager: DeploymentPackager,
        executor: Optional[Executor],
    ) -> None:
        self._config = config
        self._session = session
        self._workflow_builder = workflow_builder
        self._deployment_packager = deployment_packager
        self._executor = executor

    def deploy(self) -> list[Resource]:
        try:
            return self._deploy()
        except botocore.exceptions.ClientError as e:
            raise DeploymentError(e) from e

    def _deploy(self) -> list[Resource]:
        # This deploys a workflow to the defined home regions

        # Build the workflow (DAG of the workflow)
        workflow: Workflow = self._workflow_builder.build_workflow(self._config)

        # Upload the workflow to the solver
        self._upload_workflow_to_solver(workflow)

        # Build the workflow resources, e.g. deployment packages, iam roles, etc.
        self._deployment_packager.build(self._config, workflow)

        # Chain the commands needed to deploy all the built resources to the serverless platform
        deployment_plan = DeploymentPlan(workflow.get_deployment_instructions())

        if self._executor is None:
            raise RuntimeError("Cannot deploy with deletion deployer")

        # Execute the deployment plan
        self._executor.execute(deployment_plan)

        # Update the config with the deployed resources
        deployed_resources = self._executor.get_deployed_resources()
        self._config.update_deployed_resources(deployed_resources)

        return deployed_resources

    def _deploy(self, regions: list[str]) -> list[Resource]:
        # TODO (#9): This is the same as above, but with a list of regions to deploy to
        # This is meant to be used by the deployment manager. Additionally, the source code does
        # need to be built as we can reuse the same deployment package for all regions.
        pass

    def _upload_workflow_to_solver(self, workflow: Workflow) -> None:
        workflow_description = workflow.get_description()
        workflow_description_json = json.dumps(workflow_description)
        print(workflow_description_json)
        # TODO (#8): Upload workflow to solver
        pass


def create_default_deployer(config: Config, session: Session) -> Deployer:
    return Deployer(
        config,
        session,
        WorkflowBuilder(),
        DeploymentPackager(config),
        Executor(config),
    )


def create_deletion_deployer(config: Config, session: Session) -> Deployer:
    return Deployer(config, session, WorkflowBuilder(), DeploymentPackager(config), None)


class DeploymentError(Exception):
    pass
