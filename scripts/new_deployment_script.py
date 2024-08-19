import json
from pathlib import Path
import subprocess
import sys
import boto3

from caribou.common.constants import GLOBAL_SYSTEM_REGION
from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.deployment.common.deploy.models.resource import Resource

# Deploy to AWS Lambda (Lets go with this region for now as it is 
# the only one we are not using for anything else)
# region_name = GLOBAL_SYSTEM_REGION
region_name = "us-east-2" 

# Perhaps refering to https://stackoverflow.com/questions/66369212/aws-lambda-is-unable-to-find-app-handler-custom-docker-image will be helpful

def generate_dockerfile(handler: str, runtime: str) -> str:
    return f"""FROM public.ecr.aws/lambda/{runtime}
RUN pip3 install poetry
COPY pyproject.toml ./
COPY poetry.lock ./
COPY caribou ./caribou

RUN poetry install --no-dev
# CMD ["caribou/deployment/client/cli/aws_lambda_cli/aws_handler.py", "{handler}"]
CMD ["caribou/deployment/client/cli/cli.py", "{handler}"]
"""


def generate_deployment_dockerfile(handler: str, runtime: str) -> str:
    return f"""
# Build stage
FROM python:3.12-slim as build

# Install dependencies and crane
RUN apt-get update && apt-get install -y curl tar golang-go
RUN go install github.com/google/go-containerregistry/cmd/crane@latest

# Copy your Python dependencies and application code
COPY pyproject.toml poetry.lock ./
RUN pip3 install poetry && poetry install --no-dev
COPY caribou ./caribou

# Final image
FROM public.ecr.aws/lambda/python:3.12

# Copy the necessary files from the build stage
COPY --from=build /caribou /caribou

# Set the command for Lambda to execute using the Python interpreter
CMD ["caribou/deployment/client/cli/aws_lambda_cli/aws_handler.py", "lambda_handler"]
"""

# CMD ["python3", "caribou/deployment/client/cli/aws_lambda_cli/aws_handler.py", "lambda_handler"]
def build_docker_image(image_name: str) -> None:
    print(f"Building docker image {image_name}")
    try:
        subprocess.run(
            ["docker", "build", "--platform", "linux/amd64", "-t", image_name, f"{Path(__file__).parent.parent}/."],
            check=True,
        )
    except subprocess.CalledProcessError as e:
        print(f"Error building docker image {image_name}")
        print(e)
        sys.exit(1)


def upload_image_to_ecr(image_name: str) -> str:
    ecr_client = boto3.client("ecr", region_name=region_name)
    repository_name = image_name.split(":")[0]

    try:
        ecr_client.create_repository(repositoryName=repository_name)
    except ecr_client.exceptions.RepositoryAlreadyExistsException:
        pass  # Repository already exists, proceed

    account_id = boto3.client("sts", region_name=region_name).get_caller_identity().get("Account")
    region = region_name
    ecr_repository_uri = f"{account_id}.dkr.ecr.{region}.amazonaws.com/{repository_name}"

    print(f"Logging in to ECR")
    login_password = (
        subprocess.check_output(["aws", "ecr", "get-login-password", "--region", region]).strip().decode("utf-8")
    )
    subprocess.run(
        ["docker", "login", "--username", "AWS", "--password", login_password, ecr_repository_uri], check=True
    )

    image_uri = f"{ecr_repository_uri}:latest"

    print(f"Tagging and pushing Docker image {image_name} to ECR")

    try:
        subprocess.run(["docker", "tag", image_name, image_uri], check=True)
        subprocess.run(["docker", "push", image_uri], check=True)
        print(f"Pushed Docker image {image_name} to ECR")
    except subprocess.CalledProcessError as e:
        print(f"Error pushing Docker image {image_name} to ECR")
        raise

    return image_uri


def create_lambda_function(handler: str, image_uri: str, role: str, timeout: int, memory_size: int) -> None:
    lambda_client = boto3.client("lambda", region_name=region_name)

    try:
        lambda_client.create_function(
            FunctionName=f"caribou_{handler}",
            Role=role,
            Code={"ImageUri": image_uri},
            PackageType="Image",
            Timeout=timeout,
            MemorySize=memory_size,
        )
    except lambda_client.exceptions.ResourceConflictException:
        print(f"Lambda function {handler} already exists")
        pass


def deploy_to_aws(
    handler: str, runtime: str, role_arn: str, timeout: int, memory_size: int, deployer_env: bool = False
):
    if not deployer_env:
        dockerfile_content = generate_dockerfile(handler, runtime)
    else:
        dockerfile_content = generate_deployment_dockerfile(handler, runtime)

    dockerfile_path = Path(f"{Path(__file__).parent.parent}/Dockerfile")
    dockerfile_path.write_text(dockerfile_content)
    print(dockerfile_path.read_text())
    print("Deploying to AWS")

    image_name = f"caribou_{handler}:{runtime.replace(':', '-')}"
    build_docker_image(image_name)

    image_uri = upload_image_to_ecr(image_name)

    print(f"Deployed {handler} to AWS Lambda with image URI {image_uri}")

    create_lambda_function(handler, image_uri, role_arn, timeout, memory_size)

    print(f"Created Lambda function caribou_{handler}")


# if __name__ == "__main__":
#     if len(sys.argv) < 4 or len(sys.argv) > 6:
#         print("Usage: deploy_to_aws.py handler runtime role_arn [timeout] [memory_size]")
#         sys.exit(1)

#     handler = sys.argv[1]
#     runtime = sys.argv[2]
#     role_arn = sys.argv[3]
#     timeout = 600
#     memory_size = 3008

#     if len(sys.argv) == 5:
#         timeout = int(sys.argv[4])
#     elif len(sys.argv) == 6:
#         memory_size = int(sys.argv[5])

#     if not runtime.startswith("python:"):
#         print("Runtime must be a Python runtime of the form python:x.x")
#         sys.exit(1)

#     deploy_to_aws(handler, runtime, role_arn, timeout, memory_size, deployer_env=handler == "update_check_deployment")

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

if __name__ == "__main__":
    # handler = "lambda_handler"
    handler = "data_collect"
    runtime = "python:3.12"
    timeout = 600
    memory_size = 3008
    iam_policy_name = "caribou_deployment_policy"
    function_name = "caribou_lambda_handler"

    aws_remote_client = AWSRemoteClient(region_name)

    # Read the iam_policies_content from the file
    with open("caribou/deployment/client/cli/aws_lambda_cli/iam_policy.json", "r") as file:
        iam_policies_content = file.read()
        iam_policies_content = json.dumps(json.loads(iam_policies_content)["aws"])


    # # Delete role if exists, then create a new role
    # # Delete a role
    # if aws_remote_client.resource_exists(Resource(iam_policy_name, "iam_role")): # For iam role
    #     print(f"Deleting role {iam_policy_name}")
    #     aws_remote_client.remove_role(iam_policy_name)

    # # Create a role
    # role_arn = aws_remote_client.create_role("caribou_deployment_policy", iam_policies_content, lambda_trust_policy)

    # print(f"Created role {role_arn}")

    role_arn = "arn:aws:iam::226414417076:role/caribou_deployment_policy"

    # Delete function if exists.
    if aws_remote_client.resource_exists(Resource(function_name, "function")): # For lambda function
        aws_remote_client.remove_function(function_name)    

    # deploy_to_aws(handler, runtime, role_arn, timeout, memory_size, deployer_env=True)
    deploy_to_aws(handler, runtime, role_arn, timeout, memory_size, deployer_env=False)