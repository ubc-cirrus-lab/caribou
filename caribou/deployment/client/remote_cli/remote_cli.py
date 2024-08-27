import json
import os
import tempfile
from typing import Dict, List, Optional

from caribou.common.constants import GLOBAL_SYSTEM_REGION, REMOTE_CARIBOU_CLI_FUNCTION_NAME
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployment_packager import DeploymentPackager
from caribou.deployment.common.deploy.models.resource import Resource


def remove_aws_framework() -> None:
    print("Removing AWS framework")
    aws_remote_client = AWSRemoteClient(GLOBAL_SYSTEM_REGION)
    iam_policy_name = "caribou_deployment_policy"

    if aws_remote_client.resource_exists(Resource(iam_policy_name, "iam_role")):  # For iam role
        print(f"Deleting role {iam_policy_name}")
        aws_remote_client.remove_role(iam_policy_name)

    if aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_FUNCTION_NAME, "function")):  # For lambda function
        print(f"Deleting (Remote CLI) function {REMOTE_CARIBOU_CLI_FUNCTION_NAME}")
        aws_remote_client.remove_function(REMOTE_CARIBOU_CLI_FUNCTION_NAME)

    # Remove the deployed ECR repository if it exists
    if aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_FUNCTION_NAME, "ecr_repository")):
        print(f"Removing ECR repository {REMOTE_CARIBOU_CLI_FUNCTION_NAME}")
        aws_remote_client.remove_ecr_repository(REMOTE_CARIBOU_CLI_FUNCTION_NAME)


def deploy_aws_framework(project_dir: str, timeout: int, memory_size: int, ephemeral_storage: int) -> None:
    print(f"Deploying framework to AWS in {project_dir}")
    aws_remote_client = AWSRemoteClient(GLOBAL_SYSTEM_REGION)

    handler = "app.caribou_cli"
    iam_policy_name = "caribou_deployment_policy"

    # Read the iam_policies_content from the file
    with open("caribou/deployment/client/remote_cli/aws_framework_iam_policy.json", "r", encoding="utf-8") as file:
        iam_policies_content = file.read()
        iam_policies_content = json.dumps(json.loads(iam_policies_content)["aws"])

    # Delete role if exists
    if aws_remote_client.resource_exists(Resource(iam_policy_name, "iam_role")):  # For iam role
        print(f"Deleting role {iam_policy_name}")
        aws_remote_client.remove_role(iam_policy_name)

    # # Create a role
    role_arn = aws_remote_client.create_role(
        "caribou_deployment_policy", iam_policies_content, _retrieve_iam_trust_policy()
    )

    # Delete remote cli if exists.
    if aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_FUNCTION_NAME, "function")):  # For lambda function
        print(f"Deleting (Remote CLI) function {REMOTE_CARIBOU_CLI_FUNCTION_NAME}")
        aws_remote_client.remove_function(REMOTE_CARIBOU_CLI_FUNCTION_NAME)

    with tempfile.TemporaryDirectory() as tmpdirname:
        # Create lambda function
        ## First zip the code content
        print(f"Creating deployment package for {REMOTE_CARIBOU_CLI_FUNCTION_NAME}")
        deployment_packager_config: Config = Config({}, None)
        deployment_packager: DeploymentPackager = DeploymentPackager(deployment_packager_config)
        zip_path = deployment_packager.create_framework_package(project_dir, tmpdirname)

        # Read the zip file
        with open(zip_path, "rb") as f:
            zip_contents = f.read()

        # Retrieve the required environment variables
        desired_env_vars = [
            "GOOGLE_API_KEY",
            "ELECTRICITY_MAPS_AUTH_TOKEN",
        ]
        env_vars = _get_env_vars(desired_env_vars)

        # Deploy to AWS
        aws_remote_client.deploy_remote_cli(
            REMOTE_CARIBOU_CLI_FUNCTION_NAME,
            handler,
            role_arn,
            timeout,
            memory_size,
            ephemeral_storage,
            zip_contents,
            tmpdirname,
            env_vars,
        )


def valid_framework_dir(project_dir: str) -> bool:
    # Determines if the user invoked the command from a valid framework directory
    # The correct directory should have a 'caribou', 'caribou-go', and 'pyproject.toml'
    # file/folder in it.
    required_files = ["caribou", "caribou-go", "pyproject.toml"]
    return all(os.path.exists(os.path.join(project_dir, file)) for file in required_files)


def _retrieve_iam_trust_policy() -> dict:
    # This is the trust policy for the lambda function
    lambda_trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Principal": {"Service": ["lambda.amazonaws.com", "states.amazonaws.com"]},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    return lambda_trust_policy


def _get_env_vars(variables: List[str]) -> Dict[str, Optional[str]]:
    """
    Retrieve the specified environment variables from the current environment.

    Args:
        variables (List[str]): A list of environment variable names to retrieve.

    Returns:
        Dict[str, Optional[str]]: A dictionary with variable names as keys and their values as values.
                                  If a variable is not set, its value will be None.
    """
    env_vars = {var: os.getenv(var) for var in variables}

    # If any of the variables are not set, print a warning
    unset_vars = {var: val for var, val in env_vars.items() if val is None}
    if unset_vars:
        # Throw an error if any of the required environment variables are not set
        error_message = f"Warning: The following environment variables are not set: {unset_vars.keys()}"
        raise EnvironmentError(error_message)

    return env_vars
