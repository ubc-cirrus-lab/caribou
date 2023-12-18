from botocore.session import Session
from multi_x_serverless.deployment.client.config import Config

from multi_x_serverless.deployment.client.deploy.workflow_builder import (
    WorkflowBuilder,
)
from multi_x_serverless.deployment.client.deploy.models import (
    Workflow,
    DeploymentPlan,
)
from multi_x_serverless.deployment.client.deploy.executor import Executor
from multi_x_serverless.deployment.client.deploy.deployment_packager import (
    DeploymentPackager,
)


import botocore.exceptions


class Deployer(object):
    def __init__(
        self,
        config: Config,
        session: Session,
        workflow_builder: WorkflowBuilder,
        deployment_packager: DeploymentPackager,
        executor: Executor,
    ) -> None:
        self._config = config
        self._session = session
        self._workflow_builder = workflow_builder
        self._deployment_packager = deployment_packager
        self._executor = executor

    def deploy(self):
        try:
            self._deploy()
        except botocore.exceptions.ClientError as e:
            raise DeploymentError(e)

    def _deploy(self):
        # Build the workflow (DAG of the workflow)
        workflow: Workflow = self._workflow_builder.build_workflow(self._config)

        # Upload the workflow to the solver
        self._upload_workflow_to_solver(workflow)

        # Build the workflow resources, e.g. deployment packages, iam roles, etc.
        self._deployment_packager.build(self._config, workflow)

        # Chain the commands needed to deploy all the built resources to the serverless platform
        deployment_plan = DeploymentPlan(workflow.get_deployment_instructions(self._config))

        # Execute the deployment plan
        self._executor.execute(deployment_plan)

        # Update the config with the deployed resources
        deployed_resources = self._executor.get_deployed_resources()
        self._config.update_deployed_resources(deployed_resources)

        return deployed_resources

    def _upload_workflow_to_solver(self, workflow: Workflow):
        # TODO: Implement based on API defined by Daniel
        pass


def create_default_deployer(config: Config, session: Session) -> Deployer:
    return Deployer(
        config,
        session,
        WorkflowBuilder(),
        DeploymentPackager(),
        Executor(session),
    )


def create_deletion_deployer(config: Config, session: Session) -> Deployer:
    return Deployer(config, session, WorkflowBuilder(), DeploymentPackager(), None)


class DeploymentError(Exception):
    pass
