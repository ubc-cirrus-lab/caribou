import json
import os
import tempfile
import traceback
from typing import Dict, List, Optional

from cron_descriptor import Options, get_description

from caribou.common.constants import (
    GLOBAL_SYSTEM_REGION,
    REMOTE_CARIBOU_CLI_FUNCTION_NAME,
    REMOTE_CARIBOU_CLI_IAM_POLICY_NAME,
)
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployment_packager import DeploymentPackager
from caribou.deployment.common.deploy.models.resource import Resource


def remove_remote_framework() -> None:
    print("Removing Remote framework")
    aws_remote_client = AWSRemoteClient(GLOBAL_SYSTEM_REGION)

    # Remove all timer rules
    verbose_remove_timers: bool = True
    if not _is_aws_framework_deployed(aws_remote_client, verbose=False):
        print("AWS Remote CLI framework is not (properly) deployed. But checking for components to remove.")
        verbose_remove_timers = False
    remove_aws_timers(get_all_available_timed_cli_functions(), verbose=verbose_remove_timers)

    if aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_IAM_POLICY_NAME, "iam_role")):  # For iam role
        print(f"Deleting role {REMOTE_CARIBOU_CLI_IAM_POLICY_NAME}")
        aws_remote_client.remove_role(REMOTE_CARIBOU_CLI_IAM_POLICY_NAME)

    if aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_FUNCTION_NAME, "function")):  # For lambda function
        print(f"Deleting (Remote CLI) function {REMOTE_CARIBOU_CLI_FUNCTION_NAME}")
        aws_remote_client.remove_function(REMOTE_CARIBOU_CLI_FUNCTION_NAME)

    # Remove the deployed ECR repository if it exists
    if aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_FUNCTION_NAME, "ecr_repository")):
        print(f"Removing ECR repository {REMOTE_CARIBOU_CLI_FUNCTION_NAME}")
        aws_remote_client.remove_ecr_repository(REMOTE_CARIBOU_CLI_FUNCTION_NAME)


def deploy_remote_framework(project_dir: str, timeout: int, memory_size: int, ephemeral_storage: int) -> None:
    print(f"Deploying framework to AWS in {project_dir}")
    aws_remote_client = AWSRemoteClient(GLOBAL_SYSTEM_REGION)

    handler = "app.caribou_cli"

    # Read the iam_policies_content from the file
    with open("caribou/deployment/client/remote_cli/aws_framework_iam_policy.json", "r", encoding="utf-8") as file:
        iam_policies_content = file.read()
        iam_policies_content = json.dumps(json.loads(iam_policies_content)["aws"])

    # Delete role if exists
    if aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_IAM_POLICY_NAME, "iam_role")):  # For iam role
        print(f"Deleting role {REMOTE_CARIBOU_CLI_IAM_POLICY_NAME}")
        aws_remote_client.remove_role(REMOTE_CARIBOU_CLI_IAM_POLICY_NAME)

    # Create a role
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


def get_all_available_timed_cli_functions() -> List[str]:
    # This is the available CLI functions that can or should be scheduled
    available_timed_cli_functions = [
        "provider_collector",
        "carbon_collector",
        "performance_collector",
        "log_syncer",
        "deployment_manager",
        "deployment_migrator",
    ]

    return available_timed_cli_functions


def get_all_default_timed_cli_functions() -> dict[str, str]:
    """
    The following are the default timed CLI functions that are scheduled to run automatically.

    Setup automatic timers for AWS Lambda functions. (Use cron(...) or rate(...) expressions)
    Format Info: https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-scheduled-rule-pattern.html

    - provider_collector: Invokes Lambda function at 12:05 AM, on day 1 of the month. Default: cron(5 0 1 * ? *).
    - carbon_collector: Invokes Lambda function daily at 12:30 AM. Default: cron(30 0 * * ? *).
    - performance_collector: Invokes Lambda function daily at 12:30 AM. Default: cron(30 0 * * ? *).
    - log_syncer: Invokes Lambda function daily daily at 12:05 AM. Default: cron(5 0 * * ? *).
    - deployment_manager: Invokes Lambda function daily at 01:00 AM. Default: cron(0 1 * * ? *).
    - deployment_migrator: Invokes Lambda function daily at 02:00 AM. Default: cron(0 2 * * ? *).
    """

    # Default schedule expressions
    # Verified using: https://crontab.cronhub.io/
    default_schedule_expressions = {
        "provider_collector": "cron(5 0 1 * ? *)",  # At 12:05 AM, on day 1 of the month
        "carbon_collector": "cron(30 0 * * ? *)",  # Every day at 12:30 AM
        "performance_collector": "cron(30 0 * * ? *)",  # Every day at 12:30 AM
        "log_syncer": "cron(5 0 * * ? *)",  # Every day at 12:05 AM
        "deployment_manager": "cron(0 1 * * ? *)",  # Every day at 01:00 AM
        "deployment_migrator": "cron(0 2 * * ? *)",  # Every day at 02:00 AM
    }

    return default_schedule_expressions


def _is_aws_framework_deployed(
    aws_remote_client: AWSRemoteClient = AWSRemoteClient(GLOBAL_SYSTEM_REGION), verbose: bool = True
) -> bool:
    if not aws_remote_client.resource_exists(Resource(REMOTE_CARIBOU_CLI_IAM_POLICY_NAME, "iam_role")):  # For iam role
        if verbose:
            print("Missing Remote Framework IAM role")
        return False

    if not aws_remote_client.resource_exists(
        Resource(REMOTE_CARIBOU_CLI_FUNCTION_NAME, "function")
    ):  # For lambda function
        if verbose:
            print("Missing Remote Framework function")
        return False

    if not aws_remote_client.resource_exists(
        Resource(REMOTE_CARIBOU_CLI_FUNCTION_NAME, "ecr_repository")
    ):  # For ECR repository
        if verbose:
            print("Missing Remote Framework ECR repository")
        return False

    return True


def _get_timer_rule_name(function_name: str) -> str:
    return f"{function_name}-timer-rule"


def _get_aws_timer_payload(function_name: str) -> str:
    function_name_to_payload = {
        "provider_collector": {
            "action": "data_collect",
            "collector": "provider",
        },
        "carbon_collector": {
            "action": "data_collect",
            "collector": "carbon",
        },
        "performance_collector": {
            "action": "data_collect",
            "collector": "performance",
        },
        "log_syncer": {
            "action": "log_sync",
        },
        "deployment_manager": {
            "action": "manage_deployments",
            "deployment_metrics_calculator_type": "go",  # Set to "simple" for python calculator
        },
        "deployment_migrator": {
            "action": "run_deployment_migrator",
        },
    }

    return json.dumps(function_name_to_payload[function_name])


def setup_aws_timers(new_rules: list[tuple[str, str]]) -> None:
    """Create or update CloudWatch Event rules for Lambda functions."""
    aws_remote_client = AWSRemoteClient(GLOBAL_SYSTEM_REGION)

    # Configure cron descriptor options
    cron_descriptor_options = Options()
    cron_descriptor_options.verbose = True

    # First check if the AWS framework is deployed
    if not _is_aws_framework_deployed(aws_remote_client):
        print("AWS framework is not deployed. Please deploy the framework first.")
        return

    # Next, we can create the timer rules
    for function_name, schedule_expression in new_rules:
        rule_name = _get_timer_rule_name(function_name)
        event_payload = _get_aws_timer_payload(function_name)

        try:
            aws_remote_client.create_timer_rule(
                REMOTE_CARIBOU_CLI_FUNCTION_NAME, schedule_expression, rule_name, event_payload
            )
            if schedule_expression.startswith("rate("):
                description_text = f"Every {schedule_expression.replace('rate(', '').replace(')', '')}"
            elif schedule_expression.startswith("cron("):
                description_text = get_description(
                    schedule_expression.replace("cron(", "").replace(")", ""), cron_descriptor_options
                )
            else:
                # Here the schedule expression is not standard cron or rate, but
                # since its here, it means that its valid and can be used as is.
                description_text = schedule_expression

            print(
                f"Successfully created timer rule for {function_name}, "
                f"schedule expression: {schedule_expression} - "
                f"{description_text}"
            )
        except Exception as e:  # pylint: disable=broad-except
            print(
                f"Error creating timer rule for {function_name}, schedule expression: {schedule_expression}: {str(e)}"
            )
            traceback.print_exc()


def remove_aws_timers(desired_remove_rules: list[str], verbose: bool = True) -> None:
    """Remove CloudWatch Event rules for Lambda functions."""
    aws_remote_client = AWSRemoteClient(GLOBAL_SYSTEM_REGION)

    # First check if the AWS framework is deployed
    if not _is_aws_framework_deployed(aws_remote_client, verbose=verbose):
        if verbose:
            print("AWS framework is not (properly) deployed. No timer rules to remove.")
        return

    # We can remove the timer rules
    for function_name in desired_remove_rules:
        if verbose:
            print(f"Removing timer rule for {function_name}")
        rule_name = _get_timer_rule_name(function_name)

        try:
            # Remove the timer rules
            aws_remote_client.remove_timer_rule(REMOTE_CARIBOU_CLI_FUNCTION_NAME, rule_name)

            # Note the specific permission is not removed.
            # As other timers may still be using the function.
        except Exception as e:  # pylint: disable=broad-except
            print(f"Error removing timer rule for {function_name}: {str(e)}")
            traceback.print_exc()


def report_timer_schedule_expression(function_name: str) -> Optional[str]:
    """
    Report the specification of a timer.
    """
    aws_remote_client = AWSRemoteClient(GLOBAL_SYSTEM_REGION)
    rule_name = _get_timer_rule_name(function_name)

    return aws_remote_client.get_timer_rule_schedule_expression(rule_name)
