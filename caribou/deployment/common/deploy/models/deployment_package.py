from dataclasses import dataclass
from typing import Optional

from caribou.deployment.common.deploy.models.resource import Resource


@dataclass
class DeploymentPackage(Resource):
    filename: Optional[str] = None
