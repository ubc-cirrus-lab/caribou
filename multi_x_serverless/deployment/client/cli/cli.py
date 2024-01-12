import os

import click

from multi_x_serverless.deployment.client import __version__ as MULTI_X_SERVERLESS_VERSION
from multi_x_serverless.deployment.client.cli.new_workflow import create_new_workflow_directory
from multi_x_serverless.deployment.common.config import Config
from multi_x_serverless.deployment.common.deploy.deployer import Deployer
from multi_x_serverless.deployment.common.factories.deployer_factory import DeployerFactory


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
    "new-workflow",
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
    deployment_information = deployer.deploy(config.home_regions)
    print(deployment_information)


__version__ = MULTI_X_SERVERLESS_VERSION
