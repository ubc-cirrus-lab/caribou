import os
from typing import Any, Optional

import click
from cron_descriptor import Options, get_description

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
    help="Create a new workflow directory from template. The workflow name must be a valid, non-existing directory name in the current directory.",  # pylint: disable=line-too-long
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
    client = Client(workflow_id)

    if argument:
        client.run(argument)
    else:
        client.run()


@cli.command("data_collect", help="Run data collection.")
@click.argument("collector", required=True, type=click.Choice(["carbon", "provider", "performance", "workflow", "all"]))
@click.option("--workflow_id", "-w", help="The workflow id to collect data for.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
@click.pass_context
def data_collect(_: click.Context, collector: str, workflow_id: Optional[str], remote: bool) -> None:
    if remote:
        framework_cli_remote_client = Endpoints().get_framework_cli_remote_client()
        framework_deployed: bool = is_aws_framework_deployed(framework_cli_remote_client, verbose=False)
        if not framework_deployed:
            raise click.ClickException("The remote framework is not deployed.")

        print(f"Running {collector} collector on the remote framework.")

        # Now we know the framework is deployed, we can run the command
        action: str = "data_collect"
        function_type: str = action_type_to_function_name(action)
        event_payload: dict[str, Any] = get_cli_invoke_payload(function_type)

        # Now add the collector and workflow_id
        event_payload["collector"] = collector
        event_payload["workflow_id"] = workflow_id

        framework_cli_remote_client.invoke_remote_framework_with_payload(event_payload)
    else:
        print("Running data collector locally.")
        if collector in ("provider", "all"):
            print("Running provider collector")
            provider_collector = ProviderCollector()
            provider_collector.run()
        if collector in ("carbon", "all"):
            print("Running carbon collector")
            carbon_collector = CarbonCollector()
            carbon_collector.run()
        if collector in ("performance", "all"):
            print("Running performance collector")
            performance_collector = PerformanceCollector()
            performance_collector.run()
        if collector in ("workflow"):
            if workflow_id is None:
                raise click.ClickException("Workflow id must be provided for the workflow collector.")

            print("Running workflow collector")
            workflow_collector = WorkflowCollector()
            workflow_collector.run_on_workflow(workflow_id)


@cli.command("log_sync", help="Run log synchronization.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def log_sync(remote: bool) -> None:
    if remote:
        framework_cli_remote_client = Endpoints().get_framework_cli_remote_client()
        framework_deployed: bool = is_aws_framework_deployed(framework_cli_remote_client, verbose=False)
        if not framework_deployed:
            raise click.ClickException("The remote framework is not deployed.")

        print("Running log syncer on the remote framework.")

        # Now we know the framework is deployed, we can run the command
        action: str = "log_sync"
        function_type: str = action_type_to_function_name(action)
        event_payload = get_cli_invoke_payload(function_type)
        framework_cli_remote_client.invoke_remote_framework_with_payload(event_payload)
    else:
        log_syncer = LogSyncer(deployed_remotely=False)
        log_syncer.sync()


@cli.command("manage_deployments", help="Check if the deployment algorithm should be run.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def manage_deployments(remote: bool) -> None:
    if remote:
        framework_cli_remote_client = Endpoints().get_framework_cli_remote_client()
        framework_deployed: bool = is_aws_framework_deployed(framework_cli_remote_client, verbose=False)
        if not framework_deployed:
            raise click.ClickException("The remote framework is not deployed.")

        print("Running deployment manager on the remote framework.")

        # Now we know the framework is deployed, we can run the command
        action: str = "manage_deployments"
        function_type: str = action_type_to_function_name(action)
        event_payload = get_cli_invoke_payload(function_type)
        framework_cli_remote_client.invoke_remote_framework_with_payload(event_payload)
    else:
        deployment_manager = DeploymentManager(deployed_remotely=False)
        deployment_manager.check()


@cli.command("run_deployment_migrator", help="Check if the DP of a function should be updated.")
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def run_deployment_migrator(remote: bool) -> None:
    if remote:
        framework_cli_remote_client = Endpoints().get_framework_cli_remote_client()
        framework_deployed: bool = is_aws_framework_deployed(framework_cli_remote_client, verbose=False)
        if not framework_deployed:
            raise click.ClickException("The remote framework is not deployed.")

        print("Running deployment migrator on the remote framework.")

        # Now we know the framework is deployed, we can run the command
        action: str = "run_deployment_migrator"
        function_type: str = action_type_to_function_name(action)
        event_payload = get_cli_invoke_payload(function_type)
        framework_cli_remote_client.invoke_remote_framework_with_payload(event_payload)
    else:
        function_deployment_monitor = DeploymentMigrator(deployed_remotely=False)
        function_deployment_monitor.check()


@cli.command("setup_tables", help="Setup the tables.")
def setup_tables() -> None:
    setup_tables_func()


@cli.command("teardown_framework", help="Teardown the framework.")
@click.option("-y", "--yes", is_flag=True, help="Skip confirmation prompt.")
def teardown_framework(yes: bool) -> None:
    if yes:
        confirm = "y"
    else:
        confirm = (
            input("Are you sure you want to teardown the framework? This action cannot be undone. [y/N]: ")
            .strip()
            .lower()
        )
    print(f"confirm: {confirm}")
    if confirm in ["y", "yes"]:
        ## First remove remote framework cli
        ## This also removes all timers
        remove_remote_framework()

        ## Then remove all deployed workflows
        print("\nRemoving all deployed workflows")
        deployed_workflows: list[str] = Client().list_workflows()
        for workflow_id in deployed_workflows:
            client = Client(workflow_id)
            client.remove()

        # Finally teardown ALL the tables
        print("\nTearing down all framework tables and buckets (if any)")
        teardown_tables_func()

        print("\nFramework teardown attempt has been completed.")
    else:
        print("Teardown aborted.")


@cli.command("version", help="Print the version of caribou.")
def version() -> None:
    click.echo(CARIBOU_VERSION)


@cli.command("list", help="List the workflows.")
def list_workflows() -> None:
    client = Client()
    client.list_workflows()


@cli.command("remove", help="Remove the workflow.")
@click.argument("workflow_id", required=True)
@click.option("-r", "--remote", is_flag=True, help="Run the command on the remote framework.")
def remove(workflow_id: str, remote: bool) -> None:
    if remote:
        framework_cli_remote_client = Endpoints().get_framework_cli_remote_client()
        framework_deployed: bool = is_aws_framework_deployed(framework_cli_remote_client, verbose=False)
        if not framework_deployed:
            raise click.ClickException("The remote framework is not deployed.")

        print(f"Removing workflow {workflow_id} on the remote framework.")

        # Now we know the framework is deployed, we can run the command
        action: str = "remove_workflow"
        function_type: str = action_type_to_function_name(action)
        event_payload = get_cli_invoke_payload(function_type)

        # Now add the workflow_id
        event_payload["workflow_id"] = workflow_id

        framework_cli_remote_client.invoke_remote_framework_with_payload(event_payload)
    else:
        client = Client(workflow_id)
        client.remove()


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

    ## Memory
    memory_mb: int = 1769  # 1 full vCPU (https://docs.aws.amazon.com/lambda/latest/dg/configuration-memory.html)
    if memory is not None:
        # Convert to int
        memory_mb = int(memory)

        # Now check if the memory is within the valid range
        # 128 MB to 10240 MB
        if memory_mb < 128 or memory_mb > 10240:
            raise click.ClickException("Memory must be between 128 MB and 10240 MB (10 GB).")

    ## Timeout
    timeout_s = 900  # Maximum timeout (15 minutes)
    if timeout is not None:
        # Convert to int
        timeout_s = int(timeout)

        # Now check if the timeout is within the valid range
        # 1 second to 900 seconds
        if timeout_s < 1 or timeout_s > 900:
            raise click.ClickException("Timeout must be between 1 second and 900 seconds (15 minutes).")

    ## Ephemeral Storage
    ephemeral_storage_mb = 5120  # 5 GB (Should be enough for most use cases)
    if ephemeral_storage is not None:
        # Convert to int
        ephemeral_storage_mb = int(ephemeral_storage)

        # Now check if the ephemeral storage is within the valid range
        # 512 MB to 10240 MB
        if ephemeral_storage_mb < 512 or ephemeral_storage_mb > 10240:
            raise click.ClickException("Ephemeral storage must be between 512 MB and 10240 MB (10 GB).")

    deploy_remote_framework(project_dir, timeout_s, memory_mb, ephemeral_storage_mb)


@cli.command("list_timers", help="See all available timers.")
def list_timers() -> None:
    # Configure cron descriptor options
    cron_descriptor_options = Options()
    cron_descriptor_options.verbose = True

    # Get all available timers
    all_available_timed_cli_functions = get_all_available_timed_cli_functions()
    print("Available Timers:")
    for function_name in all_available_timed_cli_functions:
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
        print(f"  {function_name}: {schedule_expression}")


@cli.command("setup_timer", help="Setup or modify existing timer. Use list_timers to see available timers.")
@click.argument(
    "timer",
    required=True,
    type=click.Choice(
        [
            "provider_collector",
            "carbon_collector",
            "performance_collector",
            "log_syncer",
            "deployment_manager",
            "deployment_migrator",
        ]
    ),
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
    # Check if the timer is valid
    all_available_timed_cli_functions = get_all_available_timed_cli_functions()
    if timer not in all_available_timed_cli_functions:
        print("Invalid timer. Use list_timers to see available timers.")
        return

    # Now check if the schedule_expressions is defined
    # If not, revert to default settings
    if schedule_expression is None:
        schedule_expression = get_all_default_timed_cli_functions()[timer]

    # Setup the timer
    setup_aws_timers([(timer, schedule_expression)])


@cli.command("setup_all_timers", help="Setup ALL automatic timer for AWS remote CLI.")
@click.option(
    "--provider_collector",
    "-prc",
    help="Provider collector function. Refer to doc for default. Example Default: 'cron(5 0 1 * ? *)'.",
)
@click.option(
    "--carbon_collector",
    "-cac",
    help="Carbon collector function. Refer to doc for default. Example Default: 'cron(30 0 * * ? *)'.",
)
@click.option(
    "--performance_collector",
    "-pec",
    help="Performance collector function. Refer to doc for default. Example Default: 'cron(30 0 * * ? *)'.",
)
@click.option(
    "--log_syncer", "-los", help="Log syncer function. Refer to doc for default. Example Default: 'cron(5 0 * * ? *)'."
)
@click.option(
    "--deployment_manager",
    "-dma",
    help="Deployment manager function. Refer to doc for default. Example Default: 'cron(0 1 * * ? *)'.",
)
@click.option(
    "--deployment_migrator",
    "-dmi",
    help="Deployment migrator function. Refer to doc for default. Example Default: 'cron(0 2 * * ? *)'.",
)
@click.pass_context
def setup_all_timers(
    _: click.Context,
    provider_collector: Optional[str],
    carbon_collector: Optional[str],
    performance_collector: Optional[str],
    log_syncer: Optional[str],
    deployment_manager: Optional[str],
    deployment_migrator: Optional[str],
) -> None:
    """
    Setup automatic timers for AWS Lambda functions. (Use cron(...) or rate(...) expressions)
    Format Info:
    https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-scheduled-rule-pattern.html
    """
    function_names = {
        "provider_collector": provider_collector,
        "carbon_collector": carbon_collector,
        "performance_collector": performance_collector,
        "log_syncer": log_syncer,
        "deployment_manager": deployment_manager,
        "deployment_migrator": deployment_migrator,
    }

    default_schedule_expressions = get_all_default_timed_cli_functions()
    new_rules: list[tuple[str, str]] = []
    for function_name in get_all_available_timed_cli_functions():
        user_input: Optional[str] = function_names.get(function_name)
        if user_input is not None:
            schedule_expr = user_input
        else:
            schedule_expr = default_schedule_expressions[function_name]

        new_rules.append((function_name, schedule_expr))

    setup_aws_timers(new_rules)


@cli.command("remove_timer", help="Remove an existing remote timer. Use list_timers to see available timers.")
@click.argument(
    "timer",
    required=True,
    type=click.Choice(
        [
            "provider_collector",
            "carbon_collector",
            "performance_collector",
            "log_syncer",
            "deployment_manager",
            "deployment_migrator",
        ]
    ),
)
def remove_timer(timer: str) -> None:
    # Check if the timer is valid
    all_available_timed_cli_functions = get_all_available_timed_cli_functions()
    if timer not in all_available_timed_cli_functions:
        print("Invalid timer. Use list_timers to see available timers.")
        return

    # Remove the timer
    remove_aws_timers([timer])


@cli.command("remove_all_timers", help="Remove ALL automatic timers for AWS remote CLI.")
def remove_all_timers() -> None:
    """
    Remove all automatic timers for AWS Lambda functions.
    """
    all_available_timed_cli_functions = get_all_available_timed_cli_functions()
    remove_aws_timers(all_available_timed_cli_functions)


@cli.command("remove_remote_cli", help="Deploy the remote framework from AWS Lambda.")
def remove_remote_cli() -> None:
    remove_remote_framework()


__version__ = CARIBOU_VERSION
