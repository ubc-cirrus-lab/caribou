import click
from botocore.session import Session

import os

from multi_x_serverless.deployment.client.constants import multi_x_serverless_version

from multi_x_serverless.deployment.client.config import Config
from multi_x_serverless.deployment.client.factory import CLIFactory
from multi_x_serverless.deployment.client.deploy.deployer import Deployer
from multi_x_serverless.deployment.client.cli.new_workflow import create_new_workflow_directory


@click.group()
@click.option("--project-dir", "-p", help="The project directory.")
@click.pass_context
def cli(ctx: click.Context, project_dir: str) -> None:
    if project_dir is None:
        project_dir = os.getcwd()
    elif not os.path.isabs(project_dir):
        project_dir = os.path.abspath(project_dir)
    ctx.obj["project_dir"] = project_dir
    ctx.obj["factory"] = CLIFactory(project_dir, environ=os.environ)
    os.chdir(project_dir)


@cli.command("new-workflow")
@click.argument("workflow_name", required=True)
@click.pass_context
def new_workflow(ctx: click.Context, workflow_name: str) -> None:
    if os.path.exists(workflow_name):
        raise click.ClickException(f"Workflow {workflow_name} already exists.")
    create_new_workflow_directory(workflow_name)
    click.echo(f"Created new workflow in ./{workflow_name}")


@cli.command()
@click.pass_context
def deploy(ctx: click.Context) -> None:
    factory: CLIFactory = ctx.obj["factory"]
    config: Config = factory.create_config_obj()
    session: Session = factory.create_session()
    deployer: Deployer = factory.create_deployer(config=config, session=session)
    deployment_information = deployer.deploy()
    print(deployment_information)


__version__ = multi_x_serverless_version
