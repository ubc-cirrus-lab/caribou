import pytest
from pydantic import ValidationError
from multi_x_serverless.deployment.client.cli.config_schema import ConfigSchema, RegionAndProviders, Provider
import os
from pathlib import Path
import yaml


def test_config_conforms_to_schema():
    current_dir = Path(__file__).parent.parent.parent.parent.parent
    print(current_dir)
    config_file = os.path.join(
        current_dir,
        "deployment/client/cli/template/.multi-x-serverless/config.yml",
    )

    with open(config_file, "r") as f:
        config_file = f.read()

    if not config_file:
        pytest.fail("Config file is empty")

    config_dict = yaml.safe_load(config_file)

    try:
        ConfigSchema(**config_dict)
    except ValidationError as e:
        pytest.fail(f"Validation error: {e}")


def test_validate_config_unsupported_provider():
    with pytest.raises(ValueError, match="Provider non-provider is not supported"):
        RegionAndProviders(providers={"non-provider": Provider(config={})})


def test_validate_config_aws_missing_memory():
    with pytest.raises(ValueError, match="The 'config' dictionary must contain 'memory' key with an integer value"):
        RegionAndProviders(providers={"aws": Provider(config={"timeout": 10})})


def test_validate_config_aws_missing_timeout():
    with pytest.raises(ValueError, match="The 'config' dictionary must contain 'timeout' key with an integer value"):
        RegionAndProviders(providers={"aws": Provider(config={"memory": 512})})


def test_validate_config_aws_valid_config():
    RegionAndProviders(providers={"aws": Provider(config={"memory": 512, "timeout": 10})})


def test_validate_config_gcp_valid_config():
    RegionAndProviders(providers={"gcp": Provider(config={})})
