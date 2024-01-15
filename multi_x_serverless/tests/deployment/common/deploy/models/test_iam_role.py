import json
from unittest import TestCase, mock
from multi_x_serverless.deployment.common.deploy.models.iam_role import IAMRole
from multi_x_serverless.deployment.common.provider import Provider


class TestIAMRole(TestCase):
    @mock.patch("os.path.exists", return_value=True)
    @mock.patch("builtins.open", new_callable=mock.mock_open, read_data='{"aws": {"Version": "2012-10-17"}}')
    def test_init_with_existing_policy_file(self, mock_open, mock_exists):
        role = IAMRole("policy.json", "test_role")
        self.assertEqual(role.get_policy(Provider.AWS), '{"Version": "2012-10-17"}')
        self.assertEqual(role.name, "test_role")
        self.assertEqual(role.resource_type, "iam_role")

    def test_init_with_invalid_policy_file(self):
        with self.assertRaises(RuntimeError):
            IAMRole("invalid_policy", "test_role")

    def test_init_with_non_dict_policy(self):
        with self.assertRaises(RuntimeError):
            IAMRole('["invalid_policy"]', "test_role")

    @mock.patch("os.path.exists", return_value=False)
    def test_init_with_non_existing_policy_file(self, mock_exists):
        with self.assertRaises(RuntimeError):
            IAMRole("non_existent_policy.json", "test_role")

    def test_get_policy_with_non_existent_provider(self):
        role = IAMRole('{"aws": {"Version": "2012-10-17"}}', "test_role")
        with self.assertRaises(RuntimeError):
            role.get_policy(Provider.GCP)

    def test_dependencies(self):
        role = IAMRole('{"aws": {"Version": "2012-10-17"}}', "test_role")
        self.assertEqual(role.dependencies(), [])

    def test_to_json(self):
        role = IAMRole('{"aws": {"Version": "2012-10-17"}}', "test_role")
        self.assertEqual(
            role.to_json(), {"policy_file": '{"aws": {"Version": "2012-10-17"}}', "role_name": "test_role"}
        )

    def test_repr(self):
        role = IAMRole('{"aws": {"Version": "2012-10-17"}}', "test_role")
        self.assertEqual(
            role.__repr__(),
            "Resource(name=test_role, resource_type=iam_role, policy={'aws': {'Version': '2012-10-17'}})",
        )
