from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory

import unittest
from unittest import mock
from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.deployer import Deployer


class TestDeployerFactory(unittest.TestCase):
    @mock.patch("multi_x_serverless.deployment.common.factories.deployer_factory.yaml.safe_load")
    @mock.patch(
        "builtins.open",
        new_callable=mock.mock_open,
        read_data='{"workflow_name": "test", "workflow_version": "test", "environment_variables": {"test": "test"}, "iam_policy_file": "test", "regions_and_providers": {"providers": {"aws": {"region": "eu-central-1", "account_id": "123456789012", "role_name": "test"}}}}',
    )
    def test_load_project_config(self, mock_open, mock_yaml):
        mock_yaml.return_value = {"provider": "aws"}
        factory = DeployerFactory("project_dir")
        config = factory.load_project_config()
        self.assertEqual(config, {"provider": "aws"})

    @mock.patch("multi_x_serverless.deployment.common.factories.deployer_factory.DeployerFactory.load_project_config")
    @mock.patch("multi_x_serverless.deployment.common.factories.deployer_factory.DeployerFactory.load_workflow_app")
    def test_create_config_obj(self, mock_load_workflow_app, mock_load_config):
        mock_load_config.return_value = {
            "workflow_name": "test",
            "workflow_version": "test",
            "home_regions": [{"provider": "aws", "region": "eu-central-1"}],
            "environment_variables": [{"key": "test", "value": "test"}],
            "iam_policy_file": "test",
            "regions_and_providers": {
                "providers": {
                    "aws": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                }
            },
        }
        mock_load_workflow_app.return_value = "test"
        factory = DeployerFactory("project_dir")
        config = factory.create_config_obj()
        self.assertIsInstance(config, Config)

    def test_create_config_obj_from_dict(self):
        factory = DeployerFactory("project_dir")
        config = factory.create_config_obj_from_dict(
            {
                "workflow_name": "test",
                "workflow_version": "test",
                "home_regions": [{"provider": "aws", "region": "eu-central-1"}],
                "environment_variables": [{"key": "test", "value": "test"}],
                "iam_policy_file": "test",
                "regions_and_providers": {
                    "providers": {
                        "aws": {
                            "config": {"memory": 128, "timeout": 10},
                        }
                    }
                },
            }
        )
        self.assertIsInstance(config, Config)

    @mock.patch("multi_x_serverless.deployment.common.factories.deployer_factory.create_default_deployer")
    def test_create_deployer(self, mock_create_deployer):
        config = Config(
            {
                "workflow_name": "test",
                "workflow_version": "test",
                "home_regions": [{"provider": "aws", "region": "eu-central-1"}],
                "environment_variables": [{"key": "test", "value": "test"}],
                "iam_policy_file": "test",
                "regions_and_providers": {
                    "providers": {"aws": {"region": "eu-central-1", "account_id": "123456789012", "role_name": "test"}}
                },
            },
            "project_dir",
        )
        mock_create_deployer.return_value = Deployer(config, None, None, None)
        factory = DeployerFactory("project_dir")

        deployer = factory.create_deployer(config)
        self.assertIsInstance(deployer, Deployer)

    @mock.patch("multi_x_serverless.deployment.common.factories.deployer_factory.create_deletion_deployer")
    def test_create_deletion_deployer(self, mock_create_deployer):
        config = Config(
            {
                "workflow_name": "test",
                "workflow_version": "test",
                "environment_variables": {"test": "test"},
                "iam_policy_file": "test",
                "regions_and_providers": {
                    "providers": {"aws": {"region": "eu-central-1", "account_id": "123456789012", "role_name": "test"}}
                },
            },
            "project_dir",
        )
        mock_create_deployer.return_value = Deployer(config, None, None, None)
        factory = DeployerFactory("project_dir")
        deployer = factory.create_deletion_deployer(config)
        self.assertIsInstance(deployer, Deployer)

    def test_validate_only_regions_and_providers(self):
        factory = DeployerFactory("project_dir")
        # Test with valid project_config
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "aws": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "only_regions": [{"provider": "aws"}],
            }
        }
        # This should not raise any exceptions
        factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test without regions_and_providers
        project_config = {}
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test with regions_and_providers not a dictionary
        project_config = {"regions_and_providers": "not a dictionary"}
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test without providers
        project_config = {"regions_and_providers": {}}
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test with providers not a dictionary
        project_config = {"regions_and_providers": {"providers": "not a dictionary"}}
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test with only_regions not a list
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "aws": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "only_regions": "not a list",
            }
        }
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test with only_regions a list of non-dictionaries
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "aws": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "only_regions": ["not a dictionary"],
            }
        }
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test with unsupported provider
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "aws": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "only_regions": [{"provider": "unsupported"}],
            }
        }
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)

        # Test with provider not defined in providers
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "aws": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "only_regions": [{"provider": "gcp"}],
            }
        }
        with self.assertRaises(RuntimeError):
            factory._DeployerFactory__validate_only_regions_and_providers(project_config)


if __name__ == "__main__":
    unittest.main()
