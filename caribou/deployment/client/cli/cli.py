import os
from typing import Any, Optional

import click
from cron_descriptor import Options, get_description

# Caribou imports
from caribou.common.models.endpoints import Endpoints
from caribou.common.setup.setup_tables import main as setup_tables_func
from caribou.common.teardown.teardown_tables import main as teardown_tables_func
from caribou.data_collector.components.carbon.carbon_collector import CarbonCollector
from caribou.data_collector.components.performance.performance_collector import PerformanceCollector
from caribou.data_collector.components.provider.provider_collector import ProviderCollector
from caribou.data_collector.components.workflow.workflow_collector import WorkflowCollector
from caribou.deployment.client import __version__ as CARIBOU_VERSION
from caribou.deployment.client.cli.new_workflow import create_new_workflow_directory
from caribou.deployment.client.remote_cli.remote_cli import (
    action_type_to_function_name,
    deploy_remote_framework,
    get_all_available_timed_cli_functions,
    get_all_default_timed_cli_functions,
    get_cli_invoke_payload,
    is_aws_framework_deployed,
    remove_aws_timers,
    remove_remote_framework,
    report_timer_schedule_expression,
    setup_aws_timers,
    valid_framework_dir,
)
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployer import Deployer
from caribou.deployment.common.factories.deployer_factory import DeployerFactory
from caribou.endpoint.client import Client
from caribou.monitors.deployment_manager import DeploymentManager
from caribou.monitors.deployment_migrator import DeploymentMigrator
from caribou.syncers.log_syncer import LogSyncer

# Define constants
AVAILABLE_CLI_FUNCTIONS = get_all_available_timed_cli_functions()


# Helper function to execute a command on the remote framework
def _execute_remote_command(
    action: str,
    additional_payload: Optional[dict[str, Any]] = None,
    verbose: bool = True,
    requires_workflow_id: bool = False,
    workflow_id: Optional[str] = None,
) -> None:
    """Helper function to execute a command on the remote framework."""
    framework_cli_remote_client = Endpoints().get_framework_cli_remote_client()
    framework_deployed = is_aws_framework_deployed(framework_cli_remote_client, verbose=verbose)
    if not framework_deployed:
        raise click.ClickException("The remote framework is not deployed.")

    if verbose:
        click.echo(f"Running {action} on the remote framework CLI.")

    function_type = action_type_to_function_name(action)
    event_payload = get_cli_invoke_payload(function_type)

    if additional_payload:
        event_payload.update(additional_payload)

    if requires_workflow_id:
        if not workflow_id:
            raise click.ClickException("Workflow ID must be provided for this command.")
        event_payload["workflow_id"] = workflow_id

    framework_cli_remote_client.invoke_remote_framework_with_payload(event_payload)


def _validate_parameter(
    value: Optional[str], default: int, min_value: int, max_value: int, name: str, unit: str = ""
) -> int:
    if value is not None:
        int_value = int(value)
        if not min_value <= int_value <= max_value:
            if unit != "":
                unit = f" {unit}"
            raise click.ClickException(f"{name} must be between {min_value} and {max_value}{unit}.")
        return int_value
    return default


# Main CLI functions
@click.group()
@click.option("--workflow-dir", "-p", help="The project directory.")
@click.pass_context
def cli(ctx: click.Context, workflow_dir: str) -> None:
    if workflow_dir is None:
        workflow_dir = os.getcwd()
    elif not os.path.isabs(workflow_dir):
        workflow_dir = os.path.abspath(workflow_dir)
    ctx.obj["project_dir"] = workflow_dir
    ctx.obj["factory"] = DeployerFactory(workflow_dir)
    os.chdir(workflow_dir)


@cli.command(
    "new_workflow",
    help=(
        "Create a new workflow directory from template. The workflow name must be a valid, "
        "non-existing directory name in the current directory."
    ),
)
@click.argument("workflow_name", required=True)
@click.pass_context
def new_workflow(_: click.Context, workflow_name: str) -> None:
    if os.path.exists(workflow_name):
        raise click.ClickException(f"Workflow {workflow_name} already exists.")
    create_new_workflow_directory(workflow_name)
    click.echo(f"Created new workflow in ./{workflow_name}")


@cli.command("deploy", help="Deploy the workflow.")
@click.pass_context
def deploy(ctx: click.Context) -> None:
    factory: DeployerFactory = ctx.obj["factory"]
    config: Config = factory.create_config_obj()
    deployer: Deployer = factory.create_deployer(config=config)
    deployer.deploy([config.home_region])


@cli.command("run", help="Run the workflow.")
@click.argument("workflow_id", required=True)
@click.option("--argument", "-a", help="The input to the workflow. Must be a valid JSON string.")
@click.pass_context
def run(_: click.Context, argument: Optional[str], workflow_id: str) -> None:
    Client(workflow_id).run(argument)


@cli.command("data_collect", help="Run data collection.")
@click.argument("collector", required=True, type=click.Choice(["carbon", "provider", "performance", "workflow", "all"]))
@click.option("--workflow_id", "-w", help="The workflow id to collect data for.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
@click.pass_context
def data_collect(_: click.Context, collector: str, workflow_id: Optional[str], remote: bool) -> None:
    if remote:
        _execute_remote_command(
            "data_collect",
            additional_payload={"collector": collector},
            requires_workflow_id=(collector == "workflow"),
            workflow_id=workflow_id,
        )
    else:
        click.echo("Running data collector locally.")
        if collector in ("provider", "all"):
            click.echo("Running provider collector")
            ProviderCollector().run()
        if collector in ("carbon", "all"):
            click.echo("Running carbon collector")
            CarbonCollector().run()
        if collector in ("performance", "all"):
            click.echo("Running performance collector")
            PerformanceCollector().run()
        if collector == "workflow":
            if workflow_id is None:
                raise click.ClickException("Workflow id must be provided for the workflow collector.")

            click.echo("Running workflow collector")
            WorkflowCollector().run_on_workflow(workflow_id)


@cli.command("log_sync", help="Run log synchronization.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def log_sync(remote: bool) -> None:
    if remote:
        _execute_remote_command("log_sync")
    else:
        LogSyncer(deployed_remotely=False).sync()


@cli.command("manage_deployments", help="Check if the deployment algorithm should be run.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def manage_deployments(remote: bool) -> None:
    if remote:
        _execute_remote_command("manage_deployments")
    else:
        DeploymentManager(deployed_remotely=False).check()


@cli.command("run_deployment_migrator", help="Check if the DP of a function should be updated.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def run_deployment_migrator(remote: bool) -> None:
    if remote:
        _execute_remote_command("run_deployment_migrator")
    else:
        DeploymentMigrator(deployed_remotely=False).check()


@cli.command("setup_tables", help="Setup the tables.")
def setup_tables() -> None:
    setup_tables_func()


@cli.command("teardown_framework", help="Teardown the framework.")
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation prompt.")
def teardown_framework(yes: bool) -> None:
    if yes or click.confirm("Are you sure you want to teardown the framework? This action cannot be undone."):
        ## First remove remote framework cli
        ## This also removes all timers
        remove_remote_framework()

        ## Then remove all deployed workflows
        click.echo("\nRemoving all deployed workflows")
        deployed_workflows: list[str] = Client().list_workflows()
        for workflow_id in deployed_workflows:
            Client(workflow_id).remove()

        # Finally teardown ALL the tables
        click.echo("\nTearing down all framework tables and buckets (if any)")
        teardown_tables_func()

        click.echo("\nFramework teardown attempt has been completed.")
    else:
        click.echo("Teardown aborted.")


@cli.command("version", help="Print the version of caribou.")
def version() -> None:
    click.echo(CARIBOU_VERSION)


@cli.command("list", help="List the workflows.")
def list_workflows() -> None:
    Client().list_workflows()


@cli.command("remove", help="Remove the workflow.")
@click.argument("workflow_id", required=True)
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def remove(workflow_id: str, remote: bool) -> None:
    if remote:
        _execute_remote_command("remove_workflow", requires_workflow_id=True, workflow_id=workflow_id)
    else:
        Client(workflow_id).remove()


@cli.command("deploy_remote_cli", help="Deploy the remote framework cli to AWS Lambda.")
@click.option("--memory", "-m", help="The desired framework memory in MB.")
@click.option("--timeout", "-t", help="The desired remote CLI timeout time in seconds.")
@click.option("--ephemeral_storage", "-s", help="The desired ephemeral storage size of framework in MB.")
@click.pass_context
def deploy_remote_cli(
    ctx: click.Context, memory: Optional[str], timeout: Optional[str], ephemeral_storage: Optional[str]
) -> None:
    project_dir = ctx.obj["project_dir"]

    # Detect and validate input parameters, then if
    # they are not provided, use the default values.

    # Verify that the user is in the valid framework directory
    if not valid_framework_dir(project_dir):
        raise click.ClickException(
            "You must be in the main caribou directory to deploy the remote framework. "
            "Please navigate to the main caribou directory and try again. "
            "This directory should have a 'caribou', 'caribou-go', and 'pyproject.toml' file/folder in it."
        )

    # Print warning about experimental features, and ask for confirmation
    if click.confirm(
        "Remote CLI features, including all remote CLI commands and timer functionalities, "
        + "are experimental. Please use them with caution and at your own risk. "
        + "\nDo you wish to deploy the framework remotely?"
    ):
        # Memory
        ## Default 1769 == 1 full vCPU (https://docs.aws.amazon.com/lambda/latest/dg/configuration-memory.html)
        memory_mb: int = _validate_parameter(memory, 1769, 128, 10240, "Memory", "MB")

        # Timeout
        ## Default 900 == 15 minutes Maximum timeout (15 minutes)
        ## (https://docs.aws.amazon.com/lambda/latest/dg/configuration-timeout.html)
        timeout_s: int = _validate_parameter(timeout, 900, 1, 900, "Timeout", "seconds")

        # Ephemeral Storage
        ## Default 5120 == 5 GB (Should be enough for most use cases)
        ephemeral_storage_mb: int = _validate_parameter(ephemeral_storage, 5120, 512, 10240, "Ephemeral Storage", "MB")

        deploy_remote_framework(project_dir, timeout_s, memory_mb, ephemeral_storage_mb)


@cli.command("list_timers", help="See all available timers.")
def list_timers() -> None:
    # Configure cron descriptor options
    cron_descriptor_options = Options()
    cron_descriptor_options.verbose = True

    # Get all available timers
    click.echo("Available Timers:")
    for function_name in AVAILABLE_CLI_FUNCTIONS:
        schedule_expression = report_timer_schedule_expression(function_name)

        description_text: str = ""
        if schedule_expression:
            if schedule_expression.startswith("rate("):
                description_text = (
                    f"Every {schedule_expression.replace('rate(', '').replace(')', '')}"
                    f"(Rate Expression: {schedule_expression})"
                )
            elif schedule_expression.startswith("cron("):
                description_text = get_description(
                    schedule_expression.replace("cron(", "").replace(")", ""), cron_descriptor_options
                )
                description_text = f"{description_text} (Cron Expression: {schedule_expression})"
            else:
                # Unknown type, but would be valid so just use the expression
                description_text = schedule_expression

        schedule_expression = description_text if schedule_expression is not None else "Not Configured"
        click.echo(f"  {function_name}: {schedule_expression}")


@cli.command("setup_timer", help="Setup or modify existing timer. Use list_timers to see available timers.")
@click.argument(
    "timer",
    required=True,
    type=click.Choice(AVAILABLE_CLI_FUNCTIONS),
)
@click.option(
    "--schedule_expression",
    "-se",
    help="Specify a cron(...) or rate(...) rule. Or use default. Ex: cron(30 0 * * ? *).",
)
@click.pass_context
def setup_timer(
    _: click.Context,
    timer: str,
    schedule_expression: Optional[str],
) -> None:
    # Check if the schedule_expressions is defined
    # If not, revert to default settings
    if schedule_expression is None:
        schedule_expression = get_all_default_timed_cli_functions()[timer]

    # Setup the timer
    setup_aws_timers([(timer, schedule_expression)])


@cli.command(
    "setup_all_timers",
    help=(
        "Setup ALL automatic timer for AWS remote CLI with default rules. "
        "Use list_timers to see available timers, and setup_timer to modify."
    ),
)
@click.pass_context
def setup_all_timers(_: click.Context) -> None:
    """
    Setup automatic timers for AWS Lambda functions. (Use cron(...) or rate(...) expressions)
    Format Info:
    https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-scheduled-rule-pattern.html
    """
    default_schedule_expressions = get_all_default_timed_cli_functions()
    new_rules: list[tuple[str, str]] = []
    for function_name in AVAILABLE_CLI_FUNCTIONS:
        schedule_expr = default_schedule_expressions[function_name]
        new_rules.append((function_name, schedule_expr))

    setup_aws_timers(new_rules)


@cli.command("remove_timer", help="Remove an existing remote timer. Use list_timers to see available timers.")
@click.argument(
    "timer",
    required=True,
    type=click.Choice(AVAILABLE_CLI_FUNCTIONS),
)
def remove_timer(timer: str) -> None:
    # Remove the timer
    remove_aws_timers([timer])


@cli.command("remove_all_timers", help="Remove ALL automatic timers for AWS remote CLI.")
def remove_all_timers() -> None:
    """
    Remove all automatic timers for AWS Lambda functions.
    """
    remove_aws_timers(AVAILABLE_CLI_FUNCTIONS)


@cli.command("remove_remote_cli", help="Deploy the remote framework from AWS Lambda.")
def remove_remote_cli() -> None:
    remove_remote_framework()


__version__ = CARIBOU_VERSION
