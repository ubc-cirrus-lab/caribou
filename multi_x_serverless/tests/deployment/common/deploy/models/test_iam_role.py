import json
from unittest import TestCase, mock
from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole


class TestIAMRole(TestCase):
    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open, read_data='{"Version": "2012-10-17"}')
    def test_init_with_existing_policy_file(self, mock_open, mock_exists):
        role = IAMRole("policy.json", "test_role")
        self.assertEqual(role.policy, json.dumps({"Version": "2012-10-17"}))
        self.assertEqual(role.name, "test_role")
        self.assertEqual(role.resource_type, "iam_role")

    @mock.patch("os.path.exists", return_value=False)
    def test_init_with_non_existing_policy_file(self, mock_exists):
        with self.assertRaises(RuntimeError):
            IAMRole("non_existent_policy.json", "test_role")

    def test_init_with_invalid_policy_file(self):
        with self.assertRaises(RuntimeError):
            IAMRole("invalid_policy", "test_role")

    def test_dependencies(self):
        role = IAMRole('{"Version": "2012-10-17"}', "test_role")
        self.assertEqual(role.dependencies(), [])

    def test_to_json(self):
        role = IAMRole('{"Version": "2012-10-17"}', "test_role")
        self.assertEqual(role.to_json(), {"policy_file": role.policy, "role_name": "test_role"})

    def test_repr(self):
        role = IAMRole('{"Version": "2012-10-17"}', "test_role")
        self.assertEqual(role.__repr__(), f"Resource(name=test_role, resource_type=iam_role, policy={role.policy})")
