import os
from typing import Optional

import click

from caribou.common.setup.setup_tables import main as setup_tables_func
from caribou.data_collector.components.carbon.carbon_collector import CarbonCollector
from caribou.data_collector.components.performance.performance_collector import PerformanceCollector
from caribou.data_collector.components.provider.provider_collector import ProviderCollector
from caribou.data_collector.components.workflow.workflow_collector import WorkflowCollector
from caribou.deployment.client import __version__ as CARIBOU_VERSION
from caribou.deployment.client.cli.new_workflow import create_new_workflow_directory
from caribou.deployment.client.remote_cli.remote_cli import (
    deploy_aws_framework,
    remove_aws_framework,
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
@click.pass_context
def data_collect(_: click.Context, collector: str, workflow_id: Optional[str]) -> None:
    if collector in ("provider", "all"):
        provider_collector = ProviderCollector()
        provider_collector.run()
    if collector in ("carbon", "all"):
        carbon_collector = CarbonCollector()
        carbon_collector.run()
    if collector in ("performance", "all"):
        performance_collector = PerformanceCollector()
        performance_collector.run()
    if collector in ("workflow", "all"):
        if workflow_id is None:
            raise click.ClickException("Workflow id must be provided for the workflow and all collectors.")
        workflow_collector = WorkflowCollector()
        workflow_collector.run_on_workflow(workflow_id)


@cli.command("log_sync", help="Run log synchronization.")
def log_sync() -> None:
    log_syncer = LogSyncer()
    log_syncer.sync()


@cli.command("manage_deployments", help="Check if the deployment algorithm should be run.")
def manage_deployments() -> None:
    deployment_manager = DeploymentManager()
    deployment_manager.check()


@cli.command("run_deployment_migrator", help="Check if the deployment of a function should be updated.")
def run_deployment_migrator() -> None:
    function_deployment_monitor = DeploymentMigrator()
    function_deployment_monitor.check()


@cli.command("setup_tables", help="Setup the tables.")
def setup_tables() -> None:
    setup_tables_func()


@cli.command("version", help="Print the version of caribou.")
def version() -> None:
    click.echo(CARIBOU_VERSION)


@cli.command("list", help="List the workflows.")
def list_workflows() -> None:
    client = Client()
    client.list_workflows()


@cli.command("remove", help="Remove the workflow.")
@click.argument("workflow_id", required=True)
def remove(workflow_id: str) -> None:
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
    memory_mb: int = 5120  # 5 GB (Not the maximum)
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
    ephemeral_storage_mb = 10240  # Maximum ephemeral storage (10 GB)
    if ephemeral_storage is not None:
        # Convert to int
        ephemeral_storage_mb = int(ephemeral_storage)

        # Now check if the ephemeral storage is within the valid range
        # 512 MB to 10240 MB
        if ephemeral_storage_mb < 512 or ephemeral_storage_mb > 10240:
            raise click.ClickException("Ephemeral storage must be between 512 MB and 10240 MB (10 GB).")

    deploy_aws_framework(project_dir, timeout_s, memory_mb, ephemeral_storage_mb)


@cli.command("remove_remote_cli", help="Deploy the deployed remote framework from AWS Lambda.")
def remove_remote_cli() -> None:
    remove_aws_framework()


__version__ = CARIBOU_VERSION
