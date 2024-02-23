from pathlib import Path
import subprocess
import sys
import boto3


def generate_dockerfile(handler: str, runtime: str) -> str:
    parent_parent_dir = Path(__file__).parent.parent
    return f"""
    FROM public.ecr.aws/lambda/{runtime.replace("python", "python:")}
    RUN pip3 install poetry
    COPY {parent_parent_dir}/pyproject.toml ./
    COPY {parent_parent_dir}/poetry.lock ./
    COPY {parent_parent_dir}/multi_x_serverless ./multi_x_serverless
    RUN poetry install --no-dev

    COPY {parent_parent_dir}/depoyment/handlers/{handler}.sh ./
    CMD ["{handler}.sh"]
    """


def generate_handler_script(handler: str) -> str:
    return f"""
    #!/bin/bash
    echo "Running multi_x_serverless {handler}"
    poetry run multi_x_serverless {handler}
    """


def build_docker_image(image_name: str) -> None:
    print(f"Building docker image {image_name}")
    try:
        subprocess.run(["docker", "build", "--platform", "linux/amd64", "-t", image_name, "."], check=True)
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

    image_uri = f"{ecr_repository_uri}/{repository_name}:latest"

    try:
        subprocess.run(["docker", "tag", image_name, image_uri], check=True)
        subprocess.run(["docker", "push", image_uri], check=True)
        print(f"Pushed Docker image {image_name} to ECR")
    except subprocess.CalledProcessError as e:
        print(f"Error pushing Docker image {image_name} to ECR")
        raise

    return image_uri


def create_lambda_function(handler: str, image_uri: str, role: str) -> None:
    lambda_client = boto3.client("lambda")

    try:
        lambda_client.create_function(
            FunctionName=f"multi_x_serverless_{handler}",
            Runtime="provided",
            Role=role,
            Code={"ImageUri": image_uri},
            PackageType="Image",
        )
    except lambda_client.exceptions.ResourceConflictException:
        print(f"Lambda function {handler} already exists")
        pass


def deploy_to_aws(handler: str, runtime: str, role: str):
    if not Path(f"./handlers/{handler}.sh").exists():
        Path(f"./handlers/{handler}.sh").write_text(generate_handler_script(handler))

    dockerfile_content = generate_dockerfile(handler, runtime)

    dockerfile_path = Path("./Dockerfile")
    dockerfile_path.write_text(dockerfile_content)
    print(dockerfile_path.read_text())
    print("Deploying to AWS")

    image_name = f"multi_x_serverless_{handler}:{runtime}"
    build_docker_image(image_name)

    image_uri = upload_image_to_ecr(image_name)

    print(f"Deployed {handler} to AWS Lambda with image URI {image_uri}")

    create_lambda_function(handler, image_uri, role)


if __name__ == "__main__":
    handler = sys.argv[1]
    runtime = sys.argv[2]
    role = sys.argv[3]

    deploy_to_aws(handler, runtime, role)
