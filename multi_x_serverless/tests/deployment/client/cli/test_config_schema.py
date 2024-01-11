import pytest
from pydantic import ValidationError
from multi_x_serverless.deployment.client.cli.config_schema import ConfigSchema
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
