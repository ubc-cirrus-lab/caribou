import json
import os
from typing import Sequence

from multi_x_serverless.deployment.common.deploy.models.resource import Resource


class IAMRole(Resource):
    def __init__(self, policy_file: str, role_name: str) -> None:
        super().__init__(role_name, "iam_role")
        print(policy_file)
        if os.path.exists(policy_file):
            if not os.path.exists(policy_file):
                raise RuntimeError(f"Lambda policy not found, check the path ({policy_file})")
            with open(policy_file, "r", encoding="utf-8") as f:
                policy = f.read()
            try:
                policy = json.dumps(json.loads(policy))
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Lambda policy could not be parsed, check the policy ({policy})") from e
            self.policy = policy
            return

        try:
            self.policy = json.dumps(json.loads(policy_file))
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Lambda policy could not be parsed, check the policy ({policy_file})") from e

    def dependencies(self) -> Sequence[Resource]:
        return []

    def to_json(self) -> dict[str, str]:
        return {"policy_file": self.policy, "role_name": self.name}

    def __repr__(self) -> str:
        return super().__repr__() + f"policy={self.policy})"
