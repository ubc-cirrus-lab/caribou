import pytest
from pydantic import ValidationError
from multi_x_serverless.deployment.common.config.config_schema import (
    ConfigSchema,
    RegionAndProviders,
    Provider,
    Constraint,
    Constraints,
)
import os
from pathlib import Path
import yaml


def test_config_conforms_to_schema():
    current_dir = Path(__file__).parent.parent.parent.parent.parent
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


def test_validate_config_provider1_missing_memory():
    with pytest.raises(ValueError, match="The 'config' dictionary must contain 'memory' key with an integer value"):
        RegionAndProviders(providers={"provider1": Provider(config={"timeout": 10})})


def test_validate_config_provider1_missing_timeout():
    with pytest.raises(ValueError, match="The 'config' dictionary must contain 'timeout' key with an integer value"):
        RegionAndProviders(providers={"provider1": Provider(config={"memory": 512})})


def test_validate_config_provider1_valid_config():
    RegionAndProviders(providers={"provider1": Provider(config={"memory": 512, "timeout": 10})})


def test_validate_config_gcp_valid_config():
    RegionAndProviders(providers={"gcp": Provider(config={})})


def test_constraint_invalid_type():
    with pytest.raises(ValueError, match="Constraint type invalid is not supported"):
        Constraint(value=1.0, type="invalid")


def test_constraint_absolute_type():
    constraint = Constraint(value=1.0, type="absolute")
    assert constraint.value == 1.0
    assert constraint.type == "absolute"


def test_constraint_relative_type():
    constraint = Constraint(value=1.0, type="relative")
    assert constraint.value == 1.0
    assert constraint.type == "relative"


def test_constraints_invalid_priority_order():
    with pytest.raises(ValueError, match="Priority order value invalid is not supported"):
        Constraints(
            hard_resource_constraints={"cost": Constraint(value=1.0, type="absolute")},
            soft_resource_constraints={"runtime": Constraint(value=1.0, type="relative")},
            priority_order=["invalid"],
        )


def test_constraints_invalid_hard_resource_constraint():
    with pytest.raises(ValueError, match="Hard resource constraint invalid is not supported"):
        Constraints(
            hard_resource_constraints={"invalid": Constraint(value=1.0, type="absolute")},
            soft_resource_constraints={"runtime": Constraint(value=1.0, type="relative")},
            priority_order=["cost", "runtime"],
        )


def test_constraints_invalid_soft_resource_constraint():
    with pytest.raises(ValueError, match="Soft resource constraint invalid is not supported"):
        Constraints(
            hard_resource_constraints={"cost": Constraint(value=1.0, type="absolute")},
            soft_resource_constraints={"invalid": Constraint(value=1.0, type="relative")},
            priority_order=["cost", "runtime"],
        )
