from typing import Sequence

from multi_x_serverless.deployment.client.deploy.models.resource import Resource


class IAMRole(Resource):
    def __init__(self, policy: str, role_name: str) -> None:
        super().__init__(role_name, "iam_role")
        self.policy = policy

    def dependencies(self) -> Sequence[Resource]:
        return []
    
    def __repr__(self) -> str:
        return super().__repr__() + f"""        policy={self.policy})"""
