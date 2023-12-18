from botocore.session import Session

from multi_x_serverless.deployment.client.deploy.models import Resource, DeploymentPlan


class Executor(object):
    def __init__(self, session: Session) -> None:
        pass

    def execute(self, deployment_plan: DeploymentPlan) -> None:
        pass

    def get_deployed_resources(self) -> list[Resource]:
        return []