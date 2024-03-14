from pathlib import Path
import subprocess
import sys
import boto3


def generate_dockerfile(handler: str, runtime: str) -> str:
    return f"""FROM public.ecr.aws/lambda/{runtime}
RUN pip3 install poetry
COPY pyproject.toml ./
COPY poetry.lock ./
COPY multi_x_serverless ./multi_x_serverless

RUN poetry install --no-dev
CMD ["multi_x_serverless/deployment/client/cli/cli.py", "{handler}"]
"""


def generate_deployment_dockerfile(handler: str, runtime: str) -> str:
    return f"""FROM public.ecr.aws/lambda/{runtime}
RUN pip3 install poetry
COPY pyproject.toml ./
COPY poetry.lock ./
COPY multi_x_serverless ./multi_x_serverless
RUN yum install -y go && go install github.com/google/go-containerregistry/cmd/crane@latest

RUN poetry install --no-dev
CMD ["multi_x_serverless/deployment/client/cli/cli.py", "{handler}"]
"""


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
    ecr_client = boto3.client("ecr")
    repository_name = image_name.split(":")[0]

    try:
        ecr_client.create_repository(repositoryName=repository_name)
    except ecr_client.exceptions.RepositoryAlreadyExistsException:
        pass  # Repository already exists, proceed

    account_id = boto3.client("sts").get_caller_identity().get("Account")
    region = boto3.session.Session().region_name
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
    lambda_client = boto3.client("lambda")

    try:
        lambda_client.create_function(
            FunctionName=f"multi_x_serverless_{handler}",
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

    image_name = f"multi_x_serverless_{handler}:{runtime.replace(':', '-')}"
    build_docker_image(image_name)

    image_uri = upload_image_to_ecr(image_name)

    print(f"Deployed {handler} to AWS Lambda with image URI {image_uri}")

    create_lambda_function(handler, image_uri, role_arn, timeout, memory_size)

    print(f"Created Lambda function multi_x_serverless_{handler}")


if __name__ == "__main__":
    if len(sys.argv) < 4 or len(sys.argv) > 6:
        print("Usage: deploy_to_aws.py handler runtime role_arn [timeout] [memory_size]")
        sys.exit(1)

    handler = sys.argv[1]
    runtime = sys.argv[2]
    role_arn = sys.argv[3]
    timeout = 600
    memory_size = 3008

    if len(sys.argv) == 5:
        timeout = int(sys.argv[4])
    elif len(sys.argv) == 6:
        memory_size = int(sys.argv[5])

    if not runtime.startswith("python:"):
        print("Runtime must be a Python runtime of the form python:x.x")
        sys.exit(1)

    deploy_to_aws(handler, runtime, role_arn, timeout, memory_size, deployer_env=handler == "update_check_deployment")
