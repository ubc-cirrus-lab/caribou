import json
import os
import tempfile
from typing import Dict, List, Optional
import zipfile

from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.deployment.common.config.config import Config
from caribou.deployment.common.deploy.deployment_packager import DeploymentPackager
from caribou.deployment.common.deploy.models.resource import Resource
import boto3

region_name = "us-east-2" # Test region for now

def remove_aws_framework() -> None:
    print("Removing AWS framework")
    aws_remote_client = AWSRemoteClient(region_name)
    function_name = "caribou_cli"
    iam_policy_name = "caribou_deployment_policy"

    if aws_remote_client.resource_exists(Resource(iam_policy_name, "iam_role")): # For iam role
        print(f"Deleting role {iam_policy_name}")
        aws_remote_client.remove_role(iam_policy_name)

    if aws_remote_client.resource_exists(Resource(function_name, "function")): # For lambda function
        print(f"Deleting (Remote CLI) function {function_name}")
        aws_remote_client.remove_function(function_name)

def deploy_aws_framework(project_dir: str, timeout: int, memory_size: int, ephemeral_storage: int) -> None:
    print(f"Deploying framework to AWS in {project_dir}")
    aws_remote_client = AWSRemoteClient(region_name)
    
    handler = "app.caribou_cli"
    function_name = "caribou_cli"
    iam_policy_name = "caribou_deployment_policy"

    # Read the iam_policies_content from the file
    with open("caribou/deployment/client/remote_cli/aws_framework_iam_policy.json", "r") as file:
        iam_policies_content = file.read()
        iam_policies_content = json.dumps(json.loads(iam_policies_content)["aws"])

    # Delete role if exists
    if aws_remote_client.resource_exists(Resource(iam_policy_name, "iam_role")): # For iam role
        print(f"Deleting role {iam_policy_name}")
        aws_remote_client.remove_role(iam_policy_name)

    # # Create a role
    role_arn = aws_remote_client.create_role("caribou_deployment_policy", iam_policies_content, _retrieve_iam_trust_policy())
    
    # Delete remote cli if exists.
    if aws_remote_client.resource_exists(Resource(function_name, "function")): # For lambda function
        print(f"Deleting (Remote CLI) function {function_name}")
        aws_remote_client.remove_function(function_name)    

    with tempfile.TemporaryDirectory() as tmpdirname:
        tmpdirname = "/home/daniel/caribou/.caribou"

        # Create lambda function
        ## First zip the code content
        print(f"Creating deployment package for {function_name}")
        deployment_packager_config: Config = Config({})
        deployment_packager: DeploymentPackager = DeploymentPackager(deployment_packager_config)
        zip_path = deployment_packager.create_framework_package(project_dir, tmpdirname)

        # Read the zip file
        with open(zip_path, 'rb') as f:
            zip_contents = f.read()

        # Deploy to AWS
        _deploy_to_aws(function_name, handler, role_arn, timeout, memory_size, ephemeral_storage, zip_contents, tmpdirname)

def _retrieve_iam_trust_policy() -> dict:
    # This is the trust policy for the lambda function
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
    return lambda_trust_policy

def valid_framework_dir(project_dir: str) -> bool:
    # Determines if the user invoked the command from a valid framework directory
    # The correct directory should have a 'caribou', 'caribou-go', and 'pyproject.toml'
    # file/folder in it.
    required_files = ["caribou", "caribou-go", "pyproject.toml"]
    return all([os.path.exists(os.path.join(project_dir, file)) for file in required_files])

def _deploy_to_aws(
    function_name: str, handler: str, role_arn: str, timeout: int, memory_size: int, ephemeral_storage: int, zip_contents: bytes, tmpdirname: str
):
    aws_remote_client = AWSRemoteClient(region_name)

    # Step 1: Unzip the ZIP file
    zip_path = os.path.join(tmpdirname, "code.zip")
    with open(zip_path, "wb") as f_zip:
        f_zip.write(zip_contents)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(tmpdirname)

    # Step 2: Retrieve the required environment variables
    desired_env_vars = [
        # "AWS_ACCESS_KEY_ID",
        # "AWS_SECRET_ACCESS_KEY",
        "GOOGLE_API_KEY",
        "ELECTRICITY_MAPS_AUTH_TOKEN",
    ]

    # Step 3: Create a Dockerfile in the temporary directory
    env_vars = get_env_vars(desired_env_vars)
    dockerfile_content = _generate_framework_dockerfile(handler, env_vars)
    with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
        f_dockerfile.write(dockerfile_content)

    # Step 4: Build the Docker Image
    image_name = f"{function_name.lower()}:latest"
    aws_remote_client._build_docker_image(tmpdirname, image_name)

    # Step 5: Upload the Image to ECR
    image_uri = aws_remote_client._upload_image_to_ecr(image_name)
    _create_framework_lambda_function(function_name, image_uri, role_arn, timeout, memory_size, ephemeral_storage)

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

    # If any of the variables are not set, print a warning
    unset_vars = {var: val for var, val in env_vars.items() if val is None}
    if unset_vars:
        # Throw an error if any of the required environment variables are not set
        error_message = f"Warning: The following environment variables are not set: {unset_vars.keys()}"
        raise EnvironmentError(error_message)
        
    return env_vars

def _generate_framework_dockerfile(handler: str, env_vars: dict) -> str:#
    # Create ENV statements for each environment variable
    env_statements = "\n".join([f'ENV {key}="{value}"' for key, value in env_vars.items()])

    return f"""
    # Stage 1: Base image with Python 3.12 slim for installing Go
    FROM python:3.12-slim AS builder

    # Install essential packages for downloading and compiling Go
    RUN apt-get update && apt-get install -y curl tar gcc

    # Download and extract Go 1.22.6
    RUN curl -LO https://go.dev/dl/go1.22.6.linux-amd64.tar.gz \
        && tar -C /usr/local -xzf go1.22.6.linux-amd64.tar.gz \
        && rm go1.22.6.linux-amd64.tar.gz

    # Set environment variables for Go
    ENV PATH="/usr/local/go/bin:$PATH"

    # Download and install the crane tool
    RUN curl -sL "https://github.com/google/go-containerregistry/releases/download/v0.20.2/go-containerregistry_Linux_x86_64.tar.gz" > go-containerregistry.tar.gz
    RUN tar -zxvf go-containerregistry.tar.gz -C /usr/local/bin/ crane

    COPY caribou-go ./caribou-go

    # Compile Go application
    RUN cd caribou-go && \
        chmod +x build_caribou.sh && \
        ./build_caribou.sh

    # Stage 2: Build the final image based on Lambda Python 3.12 runtime
    FROM public.ecr.aws/lambda/python:3.12

    # Copy the compiled Go application from the builder stage
    COPY --from=builder caribou-go caribou-go

    # Copy Go and Crane binaries from the builder stage
    COPY --from=builder /usr/local/go /usr/local/go
    COPY --from=builder /usr/local/bin/crane /usr/local/bin/crane

    # Set up PATH and GOROOT environment variables
    ENV PATH="/usr/local/go/bin:/usr/local/bin:$PATH"
    ENV GOROOT=/usr/local/go

    # Install Poetry via pip
    RUN pip3 install poetry

    # Copy Python dependency management files
    COPY pyproject.toml poetry.lock ./

    # Configure Poetry settings and install dependencies
    RUN poetry config virtualenvs.create false
    RUN poetry install --only main

    # Declare environment variables
    {env_statements}

    # Copy application code
    COPY caribou ./caribou
    COPY app.py ./

    # Command to run the application
    CMD ["{handler}"]
    """

def _create_framework_lambda_function(function_name: str, image_uri: str, role: str, timeout: int, memory_size: int, ephemeral_storage_size: int) -> None:
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
            EphemeralStorage={"Size": ephemeral_storage_size},
        )

        arn = response["FunctionArn"]
        state = response["State"]
    except lambda_client.exceptions.ResourceConflictException:
        print(f"Lambda function {function_name} already exists")
        pass

    if state != "Active":
        aws_remote_client._wait_for_function_to_become_active(function_name)

    print(f"Caribou Lambda Framework remote cli function {function_name}"
          f" created successfully, with ARN: {arn}")