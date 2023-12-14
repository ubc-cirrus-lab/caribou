from __future__ import annotations
from multi_x_serverless.deployment.client.deploy.models import Workflow
from multi_x_serverless.deployment.client.config import Config


class DeploymentPlanner(object):
    def __init__(self) -> None:
        pass

    def plan_deployment(self, config: Config, workflow: Workflow) -> DeploymentPlan:
        return DeploymentPlan()


class EmptyPlanner(DeploymentPlanner):
    def __init__(self) -> None:
        pass


class DeploymentPlan(object):
    pass
