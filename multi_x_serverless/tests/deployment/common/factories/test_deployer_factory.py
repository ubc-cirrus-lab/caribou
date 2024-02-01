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
        read_data='{"workflow_name": "test", "workflow_version": "test", "environment_variables": {"test": "test"}, "iam_policy_file": "test", "regions_and_providers": {"providers": {"provider1": {"region": "region4", "account_id": "123456789012", "role_name": "test"}}}}',
    )
    def test_load_project_config(self, mock_open, mock_yaml):
        mock_yaml.return_value = {"provider": "provider1"}
        factory = DeployerFactory("project_dir")
        config = factory.load_project_config()
        self.assertEqual(config, {"provider": "provider1"})

    @mock.patch("multi_x_serverless.deployment.common.factories.deployer_factory.DeployerFactory.load_project_config")
    @mock.patch("multi_x_serverless.deployment.common.factories.deployer_factory.DeployerFactory.load_workflow_app")
    def test_create_config_obj(self, mock_load_workflow_app, mock_load_config):
        mock_load_config.return_value = {
            "workflow_name": "test",
            "workflow_version": "test",
            "home_regions": [{"provider": "provider1", "region": "region4"}],
            "environment_variables": [{"key": "test", "value": "test"}],
            "iam_policy_file": "test",
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                }
            },
            "constraints": {
                "hard_resource_constraints": {"cost": {"value": 100, "type": "absolute"}},
                "soft_resource_constraints": {"carbon": {"value": 0.1, "type": "relative"}},
                "priority_order": ["cost", "runtime", "carbon"],
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
                "home_regions": [{"provider": "provider1", "region": "region4"}],
                "environment_variables": [{"key": "test", "value": "test"}],
                "iam_policy_file": "test",
                "regions_and_providers": {
                    "providers": {
                        "provider1": {
                            "config": {"memory": 128, "timeout": 10},
                        }
                    }
                },
                "constraints": {
                    "hard_resource_constraints": {"cost": {"value": 100, "type": "absolute"}},
                    "soft_resource_constraints": {"carbon": {"value": 0.1, "type": "relative"}},
                    "priority_order": ["cost", "runtime", "carbon"],
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
                "home_regions": [{"provider": "provider1", "region": "region4"}],
                "environment_variables": [{"key": "test", "value": "test"}],
                "iam_policy_file": "test",
                "regions_and_providers": {
                    "providers": {"provider1": {"region": "region4", "account_id": "123456789012", "role_name": "test"}}
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
                    "providers": {"provider1": {"region": "region4", "account_id": "123456789012", "role_name": "test"}}
                },
            },
            "project_dir",
        )
        mock_create_deployer.return_value = Deployer(config, None, None, None)
        factory = DeployerFactory("project_dir")
        deployer = factory.create_deletion_deployer(config)
        self.assertIsInstance(deployer, Deployer)

    def test_validate_allowed_and_disallowed_regions_and_providers(self):
        factory = DeployerFactory("project_dir")
        # Test with valid project_config
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "allowed_regions": [{"provider": "provider1", "region": "region4"}],
                "disallowed_regions": [{"provider": "provider1", "region": "region5"}],
            },
            "home_regions": [{"provider": "provider1", "region": "region4"}],
        }
        # This should not raise any exceptions
        factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test without regions_and_providers
        project_config = {}
        with self.assertRaises(RuntimeError, msg="regions_and_providers must be defined in project config"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with regions_and_providers not a dictionary
        project_config = {"regions_and_providers": "not a dictionary"}
        with self.assertRaises(RuntimeError, msg="regions_and_providers must be a dictionary"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test without providers
        project_config = {"regions_and_providers": {}}
        with self.assertRaises(RuntimeError, msg="at least one provider must be defined in regions_and_providers"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with providers not a dictionary
        project_config = {"regions_and_providers": {"providers": "not a dictionary"}}
        with self.assertRaises(RuntimeError, msg="providers must be a dictionary"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with allowed_regions not a list
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "allowed_regions": "not a list",
            }
        }
        with self.assertRaises(RuntimeError, msg="allowed_regions must be a list"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with allowed_regions a list of non-dictionaries
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "allowed_regions": ["not a dictionary"],
            }
        }
        with self.assertRaises(RuntimeError, msg="allowed_regions must be a list of dictionaries"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with missing region in allowed_regions
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "allowed_regions": [{"provider": "provider1"}],
            }
        }
        with self.assertRaises(
            RuntimeError, msg="Region {'provider': 'provider1'} must have both provider and region defined"
        ):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with unsupported provider
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "allowed_regions": [{"provider": "unsupported", "region": "region4"}],
            }
        }
        with self.assertRaises(RuntimeError, msg="Provider unsupported is not supported"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with provider not defined in providers
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "allowed_regions": [{"provider": "gcp", "region": "region4"}],
            }
        }
        with self.assertRaises(RuntimeError, msg="Provider gcp is not defined in providers"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with a valid project_config
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                        "account_id": "123456789012",
                        "role_name": "test",
                        "region": "region4",
                    }
                },
                "allowed_regions": [{"provider": "provider1", "region": "region4"}],
            }
        }

        # This should not raise any exceptions
        factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with disallowed_regions not a list
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "disallowed_regions": "not a list",
            }
        }
        with self.assertRaises(RuntimeError, msg="disallowed_regions must be a list"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with disallowed_regions a list of non-dictionaries
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "disallowed_regions": ["not a dictionary"],
            }
        }
        with self.assertRaises(RuntimeError, msg="disallowed_regions must be a list of dictionaries"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with missing region in disallowed_regions
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "disallowed_regions": [{"provider": "provider1"}],
            }
        }
        with self.assertRaises(
            RuntimeError, msg="Region {'provider': 'provider1'} must have both provider and region defined"
        ):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with unsupported provider
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "disallowed_regions": [{"provider": "unsupported", "region": "region4"}],
            }
        }
        with self.assertRaises(RuntimeError, msg="Provider unsupported is not supported"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with provider not defined in providers
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "disallowed_regions": [{"provider": "gcp", "region": "region4"}],
            }
        }
        with self.assertRaises(RuntimeError, msg="Provider gcp is not defined in providers"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with provider not defined in providers
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "disallowed_regions": [{"provider": "provider1", "region": "region4"}],
                "allowed_regions": [{"provider": "provider1", "region": "region4"}],
            }
        }
        with self.assertRaises(RuntimeError, msg="Region region4 cannot be both allowed and disallowed"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with home region in disallowed regions
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                    }
                },
                "disallowed_regions": [{"provider": "provider1", "region": "region4"}],
                "allowed_regions": [{"provider": "provider1", "region": "region5"}],
            },
            "home_regions": [{"provider": "provider1", "region": "region4"}],
        }
        with self.assertRaises(RuntimeError, msg="Region region4 cannot be both home and disallowed"):
            factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)

        # Test with a valid project_config
        project_config = {
            "regions_and_providers": {
                "providers": {
                    "provider1": {
                        "config": {"memory": 128, "timeout": 10},
                        "account_id": "123456789012",
                        "role_name": "test",
                        "region": "region4",
                    }
                },
                "disallowed_regions": [{"provider": "provider1", "region": "region4"}],
            },
            "home_regions": [{"provider": "provider1", "region": "region5"}],
        }

        # This should not raise any exceptions
        factory._DeployerFactory__validate_allowed_and_disallowed_regions_and_providers(project_config)


if __name__ == "__main__":
    unittest.main()
