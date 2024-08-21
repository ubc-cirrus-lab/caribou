import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Dict, List, Optional
from unittest.mock import MagicMock
import zipfile

import click

from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.common.setup.setup_tables import main as setup_tables_func
from caribou.data_collector.components.carbon.carbon_collector import CarbonCollector
from caribou.data_collector.components.performance.performance_collector import PerformanceCollector
from caribou.data_collector.components.provider.provider_collector import ProviderCollector
from caribou.data_collector.components.workflow.workflow_collector import WorkflowCollector
from caribou.deployment.client import __version__ as MULTI_X_SERVERLESS_VERSION
from caribou.deployment.client.cli.new_workflow import create_new_workflow_directory
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployer import Deployer
from caribou.deployment.common.deploy.deployment_packager import DeploymentPackager
from caribou.deployment.common.deploy.models.resource import Resource
from caribou.deployment.common.factories.deployer_factory import DeployerFactory
from caribou.endpoint.client import Client
from caribou.monitors.deployment_manager import DeploymentManager
from caribou.monitors.deployment_migrator import DeploymentMigrator
from caribou.syncers.log_syncer import LogSyncer
import boto3



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

region_name = "us-east-2" # Test region

@cli.command("deploy_framework", help="Deploy the framework to Lambda.")
@click.pass_context
def deploy_framework(ctx: click.Context) -> None:
    # factory: DeployerFactory = ctx.obj["factory"]
    # config: Config = factory.create_config_obj()
    # deployer: Deployer = factory.create_deployer(config=config)
    # deployer.deploy([config.home_region])
    lambda_trust_policy = {
        "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {
                        "Service": ["lambda.amazonaws.com", "states.amazonaws.com"]
                    },
                    "Action": "sts:AssumeRole",
                }
            ],
    }

    

    aws_remote_client = AWSRemoteClient(region_name)
    
    project_dir = ctx.obj["project_dir"]
    handler = "app.caribou_cli"
    function_name = "caribou_cli"
    runtime = "python:3.12"
    timeout = 600
    memory_size = 3008
    iam_policy_name = "caribou_deployment_policy"

    deployment_packager_config: MagicMock = MagicMock(spec=Config)
    deployment_packager_config.workflow_version = "1.0.0"
    deployment_packager: DeploymentPackager = DeploymentPackager(deployment_packager_config)

    # Read the iam_policies_content from the file
    with open("caribou/deployment/client/cli/aws_lambda_cli/iam_policy.json", "r") as file:
        iam_policies_content = file.read()
        iam_policies_content = json.dumps(json.loads(iam_policies_content)["aws"])

    # Delete role if exists, then create a new role
    # Delete a role
    if aws_remote_client.resource_exists(Resource(iam_policy_name, "iam_role")): # For iam role
        print(f"Deleting role {iam_policy_name}")
        aws_remote_client.remove_role(iam_policy_name)

    # # Create a role
    role_arn = aws_remote_client.create_role("caribou_deployment_policy", iam_policies_content, lambda_trust_policy)
    
    # # print(f"Role ARN: {role_arn}")
    # role_arn = "arn:aws:iam::226414417076:role/caribou_deployment_policy"

    # Delete function if exists.
    if aws_remote_client.resource_exists(Resource(function_name, "function")): # For lambda function
        print(f"Deleting function {function_name}")
        aws_remote_client.remove_function(function_name)    

    # Create lambda function
    ## First zip the code content
    deployment_packager_config.workflow_name = function_name

    print(f"Creating deployment package for {function_name}")
    zip_path = deployment_packager.create_framework_package(project_dir)

    with open(zip_path, 'rb') as f:
        zip_contents = f.read()

    deploy_to_aws(function_name, handler, runtime, role_arn, timeout, memory_size, zip_contents)

def deploy_to_aws(
    function_name: str, handler: str, runtime: str, role_arn: str, timeout: int, memory_size: int, zip_contents: bytes
):
    aws_remote_client = AWSRemoteClient(region_name)

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdirname = "/home/daniel/caribou/.caribou"
        
        # Step 1: Unzip the ZIP file
        zip_path = os.path.join(tmpdirname, "code.zip")
        with open(zip_path, "wb") as f_zip:
            f_zip.write(zip_contents)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdirname)

        # Step 2: Create a Dockerfile in the temporary directory

        # Get the environment variables
        desired_env_vars = [
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "GOOGLE_API_KEY",
            "ELECTRICITY_MAPS_AUTH_TOKEN",
        ]
        env_vars = get_env_vars(desired_env_vars)
        dockerfile_content = generate_framework_dockerfile(handler, runtime, env_vars)
        with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
            f_dockerfile.write(dockerfile_content)

        # Step 3: Build the Docker Image
        image_name = f"{function_name.lower()}:latest"
        aws_remote_client._build_docker_image(tmpdirname, image_name)

        # Step 4: Upload the Image to ECR
        image_uri = aws_remote_client._upload_image_to_ecr(image_name)
    
    create_lambda_function(function_name, image_uri, role_arn, timeout, memory_size)

def get_env_vars(variables: List[str]) -> Dict[str, Optional[str]]:
    """
    Retrieve the specified environment variables from the current environment.

    Args:
        variables (List[str]): A list of environment variable names to retrieve.

    Returns:
        Dict[str, Optional[str]]: A dictionary with variable names as keys and their values as values.
                                  If a variable is not set, its value will be None.
    """
    env_vars = {var: os.getenv(var) for var in variables}
    return env_vars

# def generate_framework_dockerfile(handler: str, runtime: str, env_vars: dict) -> str:
#     # Create ENV statements for each environment variable
#     env_statements = "\n".join([f'ENV {key}="{value}"' for key, value in env_vars.items()])

#     return f"""
#     FROM public.ecr.aws/lambda/{runtime}

#     # Install Poetry
#     RUN pip3 install poetry

#     # Copy dependency files
#     COPY pyproject.toml poetry.lock ./

#     # Configure Poetry to avoid creating virtual environments and install dependencies
#     RUN poetry config virtualenvs.create false
#     RUN poetry install --only main

#     # Set environment variables
#     {env_statements}

#     COPY caribou ./caribou
#     COPY caribou-go ./caribou-go
#     COPY app.py ./

#     # Set the command to run the application
#     CMD ["{handler}"]
#     """

def generate_framework_dockerfile(handler: str, runtime: str, env_vars: dict) -> str:
    # Create ENV statements for each environment variable
    env_statements = "\n".join([f'ENV {key}="{value}"' for key, value in env_vars.items()])

    return f"""
    # Stage 1: Install Go using python:3.12-slim
    FROM python:3.12-slim AS builder

    # Install dependencies to download and install Go
    RUN apt-get update && apt-get install -y curl tar gcc

    # Download and install Go 1.22
    RUN curl -LO https://go.dev/dl/go1.22.6.linux-amd64.tar.gz \
        && tar -C /usr/local -xzf go1.22.6.linux-amd64.tar.gz \
        && rm go1.22.6.linux-amd64.tar.gz

    # Set Go environment variables
    ENV PATH="/usr/local/go/bin:$PATH"

    # Install the crane tool
    RUN curl -sL "https://github.com/google/go-containerregistry/releases/download/v0.20.2/go-containerregistry_Linux_x86_64.tar.gz" > go-containerregistry.tar.gz
    RUN tar -zxvf go-containerregistry.tar.gz -C /usr/local/bin/ crane

    COPY caribou-go ./caribou-go

    # Compile the Go code
    RUN cd caribou-go && \
        chmod +x build_caribou.sh && \
        ./build_caribou.sh

    # Stage 2: Build the final image using the Lambda runtime
    FROM public.ecr.aws/lambda/{runtime}

    # Copy Caribou Go folder from the builder stage
    COPY --from=builder caribou-go caribou-go

    # Copy Go and Crane from the builder stage
    COPY --from=builder /usr/local/go /usr/local/go
    COPY --from=builder /usr/local/bin/crane /usr/local/bin/crane

    # Set environment variables
    ENV PATH="/usr/local/go/bin:/usr/local/bin:$PATH"
    ENV GOROOT=/usr/local/go

    # Install Poetry
    RUN pip3 install poetry

    # Copy dependency files
    COPY pyproject.toml poetry.lock ./

    # Configure Poetry to avoid creating virtual environments and install dependencies
    RUN poetry config virtualenvs.create false
    RUN poetry install --only main

    # Set environment variables
    {env_statements}

    # Copy application (framework) code
    COPY caribou ./caribou
    COPY app.py ./

    # Set the command to run the application
    CMD ["{handler}"]
    """

# Potential alternatives
# def generate_deployment_dockerfile(handler: str, runtime: str) -> str:
#     return f"""
# # Build stage
# FROM python:3.12-slim as build

# # Install dependencies and crane
# RUN apt-get update && apt-get install -y curl tar golang-go
# RUN go install github.com/google/go-containerregistry/cmd/crane@latest

# # Copy your Python dependencies and application code
# COPY pyproject.toml poetry.lock ./
# RUN pip3 install poetry && poetry install --no-dev
# COPY caribou ./caribou

# # Final image
# FROM public.ecr.aws/lambda/python:3.12

# # Copy the necessary files from the build stage
# COPY --from=build /caribou /caribou

# """

def create_lambda_function(function_name: str, image_uri: str, role: str, timeout: int, memory_size: int) -> None:
    lambda_client = boto3.client("lambda", region_name=region_name)
    aws_remote_client = AWSRemoteClient(region_name)
    print(f"Creating Lambda function {function_name}")
    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            Role=role,
            Code={"ImageUri": image_uri},
            PackageType="Image",
            Timeout=timeout,
            MemorySize=memory_size,
        )

        arn = response["FunctionArn"]
        state = response["State"]
    except lambda_client.exceptions.ResourceConflictException:
        print(f"Lambda function {function_name} already exists")
        pass

    if state != "Active":
        aws_remote_client._wait_for_function_to_become_active(function_name)

    print(f"Lambda function {function_name} created successfully")

__version__ = MULTI_X_SERVERLESS_VERSION
