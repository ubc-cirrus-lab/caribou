import json
import os
from typing import Sequence

from caribou.common.provider import Provider
from caribou.deployment.common.deploy.models.resource import Resource


class IAMRole(Resource):
    def __init__(self, policy_file: str, role_name: str) -> None:
        super().__init__(role_name, "iam_role")
        # If the policy file is a path, read the file and set the policy
        if os.path.exists(policy_file):
            with open(policy_file, "r", encoding="utf-8") as f:
                policy = f.read()
            try:
                policy = json.loads(policy)
                if not isinstance(policy, dict):
                    raise RuntimeError(f"Policy is not a dictionary, check the policy ({policy_file})")
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Policy could not be parsed, check the policy ({policy})") from e
        else:
            # If the policy file is not a path, check if it is a valid json
            try:
                policy = json.loads(policy_file)
                if not isinstance(policy, dict):
                    raise RuntimeError(f"Policy is not a dictionary, check the policy ({policy_file})")
            except json.JSONDecodeError as e:
                raise RuntimeError(f"Policy could not be parsed, check the policy ({policy_file})") from e
        self._policy: dict = policy

    def dependencies(self) -> Sequence[Resource]:
        return []

    def get_policy(self, provider: Provider) -> str:
        if provider.value in self._policy:
            return json.dumps(self._policy[provider.value])

        raise RuntimeError(f"Provider {provider.value} not found in policy file")

    def to_json(self) -> dict[str, str]:
        return {"policy_file": json.dumps(self._policy), "role_name": self.name}

    def __repr__(self) -> str:
        return super().__repr__() + f"policy={self._policy})"
