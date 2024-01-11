from typing import Any, Optional

import botocore.exceptions
from botocore.session import Session

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.common.deploy.deployment_packager import DeploymentPackager
from multi_x_serverless.deployment.common.deploy.executor import Executor
from multi_x_serverless.deployment.common.deploy.models.deployment_plan import DeploymentPlan
from multi_x_serverless.deployment.common.deploy.models.resource import Resource
from multi_x_serverless.deployment.common.deploy.models.workflow import Workflow
from multi_x_serverless.deployment.common.deploy.workflow_builder import WorkflowBuilder


class Deployer:
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

    def deploy(
        self, regions: list[dict[str, str]], deployment_config: Optional[dict[str, Any]] = None
    ) -> list[Resource]:
        try:
            return self._deploy(regions, deployment_config)
        except botocore.exceptions.ClientError as e:
            raise DeploymentError(e) from e

    def _deploy(self, regions: list[dict[str, str]], deployment_config: Optional[dict[str, Any]]) -> list[Resource]:
        workflow = None
        if deployment_config is not None:
            workflow = self._workflow_builder.re_build_workflow(self._config, regions, deployment_config)

            self._deployment_packager.re_build(self._config, workflow)
        else:
            # Build the workflow (DAG of the workflow)
            workflow = self._workflow_builder.build_workflow(self._config, regions)

            # Upload the workflow to the solver
            self._upload_workflow_to_solver(workflow)
            self._upload_workflow_to_deployer_server(workflow)

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

    def _upload_workflow_to_solver(self, workflow: Workflow) -> None:
        workflow_config = workflow.get_instance_description()

        print(workflow_config.to_json())
        # TODO (#8): Upload workflow to solver

    def _upload_workflow_to_deployer_server(self, workflow: Workflow) -> None:
        workflow_config = workflow.get_instance_description()

        print(workflow_config.to_json())
        # TODO (#9): Upload workflow to deployer server


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
