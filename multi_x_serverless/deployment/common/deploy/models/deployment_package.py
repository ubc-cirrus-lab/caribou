from dataclasses import dataclass
from typing import Optional

from multi_x_serverless.deployment.common.deploy.models.resource import Resource


@dataclass
class DeploymentPackage(Resource):
    filename: Optional[str] = None
