import os
from typing import Optional

import click

from multi_x_serverless.common.setup.setup_tables import main as setup_tables_func
from multi_x_serverless.data_collector.components.carbon.carbon_collector import CarbonCollector
from multi_x_serverless.data_collector.components.performance.performance_collector import PerformanceCollector
from multi_x_serverless.data_collector.components.provider.provider_collector import ProviderCollector
from multi_x_serverless.data_collector.components.workflow.workflow_collector import WorkflowCollector
from multi_x_serverless.deployment.client import __version__ as MULTI_X_SERVERLESS_VERSION
from multi_x_serverless.deployment.client.cli.new_workflow import create_new_workflow_directory
from multi_x_serverless.deployment.common.config.config import Config
from multi_x_serverless.deployment.common.deploy.deployer import Deployer
from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory
from multi_x_serverless.endpoint.client import Client
from multi_x_serverless.monitors.deployment_optimization_monitor import DeploymentOptimizationMonitor
from multi_x_serverless.monitors.function_deployment_monitor import FunctionDeploymentMonitor
from multi_x_serverless.syncers.log_syncer import LogSyncer


@click.group()
@click.option("--project-dir", "-p", help="The project directory.")
@click.pass_context
def cli(ctx: click.Context, project_dir: str) -> None:
    if project_dir is None:
        project_dir = os.getcwd()
    elif not os.path.isabs(project_dir):
        project_dir = os.path.abspath(project_dir)
    ctx.obj["project_dir"] = project_dir
    ctx.obj["factory"] = DeployerFactory(project_dir)
    os.chdir(project_dir)


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


@cli.command("monitor_deployment_optimization", help="Check if the deployment algorithm should be run.")
def monitor_deployment_optimization() -> None:
    deployment_optimization_monitor = DeploymentOptimizationMonitor()
    deployment_optimization_monitor.check()


@cli.command("monitor_function_deployment", help="Check if the deployment of a function should be updated.")
def monitor_function_deployment() -> None:
    function_deployment_monitor = FunctionDeploymentMonitor()
    function_deployment_monitor.check()


@cli.command("setup_tables", help="Setup the tables.")
def setup_tables() -> None:
    setup_tables_func()


@cli.command("version", help="Print the version of multi_x_serverless.")
def version() -> None:
    click.echo(MULTI_X_SERVERLESS_VERSION)


@cli.command("list", help="List the workflows.")
def list_workflows() -> None:
    client = Client()
    client.list_workflows()


@cli.command("remove", help="Remove the workflow.")
@click.argument("workflow_id", required=True)
def remove(workflow_id: str) -> None:
    client = Client(workflow_id)
    client.remove()


__version__ = MULTI_X_SERVERLESS_VERSION
