import json
import logging
import os
import subprocess
import tempfile
import time
import zipfile
from datetime import datetime
from typing import Any, Optional

from boto3.session import Session
from botocore.exceptions import ClientError

from caribou.common.constants import (
    CARIBOU_WORKFLOW_IMAGES_TABLE,
    DEPLOYMENT_RESOURCES_BUCKET,
    GLOBAL_SYSTEM_REGION,
    REMOTE_CARIBOU_CLI_FUNCTION_NAME,
    SYNC_MESSAGES_TABLE,
    SYNC_PREDECESSOR_COUNTER_TABLE,
)
from caribou.common.models.remote_client.remote_client import RemoteClient
from caribou.common.utils import compress_json_str, decompress_json_str
from caribou.deployment.common.deploy.models.resource import Resource

logger = logging.getLogger(__name__)


# pylint: disable=too-many-lines
class AWSRemoteClient(RemoteClient):  # pylint: disable=too-many-public-methods
    LAMBDA_CREATE_ATTEMPTS = 30
    DELAY_TIME = 5

    def __init__(self, region: str) -> None:
        self._session = Session(region_name=region)
        self._client_cache: dict[str, Any] = {}
        self._workflow_image_cache: dict[str, dict[str, str]] = {}

        # Allow for override of the deployment resources bucket (Due to S3 bucket name restrictions)
        self._deployment_resource_bucket: str = os.environ.get(
            "CARIBOU_OVERRIDE_DEPLOYMENT_RESOURCES_BUCKET", DEPLOYMENT_RESOURCES_BUCKET
        )

    def get_current_provider_region(self) -> str:
        return f"aws_{self._session.region_name}"

    def _client(self, service_name: str) -> Any:
        if service_name not in self._client_cache:
            self._client_cache[service_name] = self._session.client(service_name)
        return self._client_cache[service_name]

    def get_iam_role(self, role_name: str) -> str:
        client = self._client("iam")
        response = client.get_role(RoleName=role_name)
        return response["Role"]["Arn"]

    def get_lambda_function(self, function_name: str) -> dict[str, Any]:
        client = self._client("lambda")
        response = client.get_function(FunctionName=function_name)
        return response["Configuration"]

    def resource_exists(self, resource: Resource) -> bool:
        if resource.resource_type == "iam_role":
            return self.iam_role_exists(resource)
        if resource.resource_type == "function":
            return self.lambda_function_exists(resource)
        if resource.resource_type == "ecr_repository":
            return self.ecr_repository_exists(resource)
        if resource.resource_type == "messaging_topic":
            return False
        raise RuntimeError(f"Unknown resource type {resource.resource_type}")

    def iam_role_exists(self, resource: Resource) -> bool:
        try:
            role = self.get_iam_role(resource.name)
        except ClientError:
            return False
        return role is not None

    def lambda_function_exists(self, resource: Resource) -> bool:
        try:
            function = self.get_lambda_function(resource.name)
        except ClientError:
            return False
        return function is not None

    def set_predecessor_reached(
        self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str, direct_call: bool
    ) -> tuple[list[bool], float, float]:
        client = self._client("dynamodb")

        # Record the consumed capacity (Write Capacity Units) for this operation
        # Refer to: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
        consumed_write_capacity = 0.0

        # Check if the map exists and create it if not
        mc_response = client.update_item(
            TableName=SYNC_PREDECESSOR_COUNTER_TABLE,
            Key={"id": {"S": workflow_instance_id}},
            UpdateExpression="SET #s = if_not_exists(#s, :empty_map)",
            ExpressionAttributeNames={"#s": sync_node_name},
            ExpressionAttributeValues={":empty_map": {"M": {}}},
            ReturnConsumedCapacity="TOTAL",
        )

        consumed_write_capacity += mc_response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

        # Update the map with the new predecessor_name and direct_call
        if not direct_call:
            response = client.update_item(
                TableName=SYNC_PREDECESSOR_COUNTER_TABLE,
                Key={"id": {"S": workflow_instance_id}},
                UpdateExpression="SET #s.#p = if_not_exists(#s.#p, :direct_call)",
                ExpressionAttributeNames={"#s": sync_node_name, "#p": predecessor_name},
                ExpressionAttributeValues={":direct_call": {"BOOL": direct_call}},
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL",
            )
        else:
            response = client.update_item(
                TableName=SYNC_PREDECESSOR_COUNTER_TABLE,
                Key={"id": {"S": workflow_instance_id}},
                UpdateExpression="SET #s.#p = :direct_call",
                ExpressionAttributeNames={"#s": sync_node_name, "#p": predecessor_name},
                ExpressionAttributeValues={":direct_call": {"BOOL": direct_call}},
                ReturnValues="ALL_NEW",
                ReturnConsumedCapacity="TOTAL",
            )

        consumed_write_capacity += response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

        # Measure the size of the response
        response_size = len(json.dumps(response).encode("utf-8")) / (1024**3)

        return (
            [item["BOOL"] for item in response["Attributes"][sync_node_name]["M"].values()],
            response_size,
            consumed_write_capacity,
        )

    def create_sync_tables(self) -> None:
        # Check if table exists
        client = self._client("dynamodb")
        for table in [SYNC_MESSAGES_TABLE, SYNC_PREDECESSOR_COUNTER_TABLE]:
            try:
                client.describe_table(TableName=table)
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    client.create_table(
                        TableName=table,
                        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                        BillingMode="PAY_PER_REQUEST",
                    )
                else:
                    raise

    def upload_predecessor_data_at_sync_node(
        self, function_name: str, workflow_instance_id: str, message: str
    ) -> float:
        client = self._client("dynamodb")
        sync_node_id = f"{function_name}:{workflow_instance_id}"
        response = client.update_item(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": sync_node_id}},
            ExpressionAttributeNames={
                "#M": "message",
            },
            ExpressionAttributeValues={
                ":m": {"SS": [message]},
            },
            UpdateExpression="ADD #M :m",
            ReturnConsumedCapacity="TOTAL",
        )

        return response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

    def get_predecessor_data(
        self,
        current_instance_name: str,
        workflow_instance_id: str,
        consistent_read: bool = True,  # pylint: disable=unused-argument
    ) -> tuple[list[str], float]:
        client = self._client("dynamodb")
        sync_node_id = f"{current_instance_name}:{workflow_instance_id}"
        # Currently we use strongly consistent reads for the sync messages
        # Refer to: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/read-write-operations.html
        response = client.get_item(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": sync_node_id}},
            ReturnConsumedCapacity="TOTAL",
            ConsistentRead=consistent_read,
        )

        # Record the consumed capacity (Read Capacity Units) for the sync node
        consumed_read_capacity = response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

        if "Item" not in response:
            return [], consumed_read_capacity

        item = response.get("Item")
        if item is not None and "message" in item:
            return item["message"]["SS"], consumed_read_capacity

        return [], consumed_read_capacity

    def create_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: Optional[bytes],
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        deployed_image_uri = self._get_deployed_image_uri(function_name)
        if len(deployed_image_uri) > 0:
            image_uri = self._copy_image_to_region(deployed_image_uri)
        else:
            if zip_contents is None:
                raise RuntimeError("No deployed image AND No deployment package provided for function creation")

            with tempfile.TemporaryDirectory() as tmpdirname:
                # Step 1: Unzip the ZIP file
                zip_path = os.path.join(tmpdirname, "code.zip")
                with open(zip_path, "wb") as f_zip:
                    f_zip.write(zip_contents)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

                # Step 2: Create a Dockerfile in the temporary directory
                dockerfile_content = self._generate_dockerfile(runtime, handler, additional_docker_commands)
                with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
                    f_dockerfile.write(dockerfile_content)

                # Step 3: Build the Docker Image
                image_name = f"{function_name.lower()}:latest"
                self._build_docker_image(tmpdirname, image_name)

                # Step 4: Upload the Image to ECR
                image_uri = self._upload_image_to_ecr(image_name)
                self._store_deployed_image_uri(function_name, image_uri)

        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Code": {"ImageUri": image_uri},
            "PackageType": "Image",
            "Role": role_identifier,
            "Environment": {"Variables": environment_variables},
            "MemorySize": memory_size,
        }
        if timeout >= 1:
            kwargs["Timeout"] = timeout
        arn, state = self._create_lambda_function(kwargs)

        if state != "Active":
            self._wait_for_function_to_become_active(function_name)

        return arn

    def _copy_image_to_region(self, deployed_image_uri: str) -> str:
        parts = deployed_image_uri.split("/")
        original_region = parts[0].split(".")[3]
        original_image_name = parts[1]

        ecr_client = self._client("ecr")
        new_region = ecr_client.meta.region_name
        new_image_name = original_image_name.replace(original_region, new_region)

        # Assume AWS CLI is configured. Customize these commands based on your AWS setup.
        repository_name = new_image_name.split(":")[0]
        try:
            ecr_client.create_repository(repositoryName=repository_name)
        except ecr_client.exceptions.RepositoryAlreadyExistsException:
            pass  # Repository already exists, proceed

        account_id = self._client("sts").get_caller_identity().get("Account")

        original_ecr_registry = f"{account_id}.dkr.ecr.{original_region}.amazonaws.com"
        ecr_registry = f"{account_id}.dkr.ecr.{new_region}.amazonaws.com"

        new_image_uri = f"{ecr_registry}/{new_image_name}"

        login_password_original = (
            subprocess.check_output(["aws", "--region", original_region, "ecr", "get-login-password"])
            .strip()
            .decode("utf-8")
        )
        login_password_new = (
            subprocess.check_output(["aws", "--region", new_region, "ecr", "get-login-password"])
            .strip()
            .decode("utf-8")
        )

        # Use /tmp directory which is writable in AWS Lambda
        with tempfile.TemporaryDirectory(dir="/tmp") as temp_dir:
            # Force environment variables to use the temporary directory
            env = os.environ.copy()
            env["HOME"] = temp_dir
            env["XDG_CACHE_HOME"] = os.path.join(temp_dir, ".cache")
            env["XDG_CONFIG_HOME"] = os.path.join(temp_dir, ".config")
            env["XDG_DATA_HOME"] = os.path.join(temp_dir, ".local", "share")

            print(f"Using crane to copy image from {original_ecr_registry} to {ecr_registry}")
            try:
                subprocess.run(
                    ["crane", "auth", "login", original_ecr_registry, "-u", "AWS", "-p", login_password_original],
                    cwd=temp_dir,
                    env=env,  # Use the modified environment variables
                    check=True,
                )
                subprocess.run(
                    ["crane", "auth", "login", ecr_registry, "-u", "AWS", "-p", login_password_new],
                    cwd=temp_dir,
                    env=env,  # Use the modified environment variables
                    check=True,
                )
                subprocess.run(
                    ["crane", "cp", deployed_image_uri, new_image_uri],
                    cwd=temp_dir,
                    env=env,  # Use the modified environment variables
                    check=True,
                )
                logger.info("Docker image %s copied successfully.", new_image_uri)
            except subprocess.CalledProcessError as e:
                logger.error("Failed to copy Docker image %s. Error: %s", new_image_uri, e)
            return new_image_uri

    def _store_deployed_image_uri(self, function_name: str, image_name: str) -> None:
        workflow_instance_id = "-".join(function_name.split("-")[0:2])

        function_name_simple = function_name[len(workflow_instance_id) + 1 :].rsplit("_", 1)[0]

        if workflow_instance_id not in self._workflow_image_cache:
            self._workflow_image_cache[workflow_instance_id] = {}

        self._workflow_image_cache[workflow_instance_id].update({function_name_simple: image_name})

        client = self._session.client("dynamodb", region_name=GLOBAL_SYSTEM_REGION)

        # Check if the item exists and create dictionary if not
        client.update_item(
            TableName=CARIBOU_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": workflow_instance_id}},
            UpdateExpression="SET #v = if_not_exists(#v, :empty_map)",
            ExpressionAttributeNames={"#v": "value"},
            ExpressionAttributeValues={":empty_map": {"M": {}}},
        )

        client.update_item(
            TableName=CARIBOU_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": workflow_instance_id}},
            UpdateExpression="SET #v.#f = :value",
            ExpressionAttributeNames={"#v": "value", "#f": function_name_simple},
            ExpressionAttributeValues={":value": {"S": image_name}},
        )

    def _get_deployed_image_uri(self, function_name: str, consistent_read: bool = True) -> str:
        workflow_instance_id = "-".join(function_name.split("-")[0:2])

        function_name_simple = function_name[len(workflow_instance_id) + 1 :].rsplit("_", 1)[0]

        if function_name_simple in self._workflow_image_cache.get(workflow_instance_id, {}):
            return self._workflow_image_cache[workflow_instance_id][function_name_simple]

        client = self._session.client("dynamodb", region_name=GLOBAL_SYSTEM_REGION)

        response = client.get_item(
            TableName=CARIBOU_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": workflow_instance_id}},
            ConsistentRead=consistent_read,
        )

        if "Item" not in response:
            return ""

        item = response.get("Item")
        if item is not None and "value" in item:
            if workflow_instance_id not in self._workflow_image_cache:
                self._workflow_image_cache[workflow_instance_id] = {}
            self._workflow_image_cache[workflow_instance_id].update(
                {function_name_simple: item["value"]["M"].get(function_name_simple, {}).get("S", "")}
            )
            return item["value"]["M"].get(function_name_simple, {}).get("S", "")
        return ""

    def _generate_dockerfile(self, runtime: str, handler: str, additional_docker_commands: Optional[list[str]]) -> str:
        run_command = ""
        if additional_docker_commands and len(additional_docker_commands) > 0:
            run_command += " && ".join(additional_docker_commands)
        if len(run_command) > 0:
            run_command = f"RUN {run_command}"

        # For AWS lambda insights for CPU and IO logging
        # https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Lambda-Insights-Getting-Started-docker.html
        lambda_insight_command = (
            """RUN curl -O https://lambda-insights-extension.s3-ap-northeast-1.amazonaws.com/amazon_linux/lambda-insights-extension.rpm && """  # pylint: disable=line-too-long
            """rpm -U lambda-insights-extension.rpm && """
            """rm -f lambda-insights-extension.rpm"""
        )

        return f"""
        FROM public.ecr.aws/lambda/{runtime.replace("python", "python:")}
        COPY requirements.txt ./
        {lambda_insight_command}
        {run_command}
        RUN pip3 install --no-cache-dir -r requirements.txt
        COPY app.py ./
        COPY src ./src
        COPY caribou ./caribou
        CMD ["{handler}"]
        """

    def _build_docker_image(self, context_path: str, image_name: str) -> None:
        try:
            subprocess.run(["docker", "build", "--platform", "linux/amd64", "-t", image_name, context_path], check=True)
            logger.info("Docker image %s built successfully.", image_name)
        except subprocess.CalledProcessError as e:
            # This will catch errors from the subprocess and logger.info a message.
            logger.error("Failed to build Docker image %s. Error: %s", image_name, e)

    def _upload_image_to_ecr(self, image_name: str) -> str:
        ecr_client = self._client("ecr")
        # Assume AWS CLI is configured. Customize these commands based on your AWS setup.
        repository_name = image_name.split(":")[0]
        try:
            ecr_client.create_repository(repositoryName=repository_name)
        except ecr_client.exceptions.RepositoryAlreadyExistsException:
            pass  # Repository already exists, proceed

        # Retrieve an authentication token and authenticate your Docker client to your registry.
        # Use the AWS CLI 'get-login-password' command to get the token.
        account_id = self._client("sts").get_caller_identity().get("Account")
        region = self._client("ecr").meta.region_name
        ecr_registry = f"{account_id}.dkr.ecr.{region}.amazonaws.com"

        login_password = (
            subprocess.check_output(["aws", "--region", region, "ecr", "get-login-password"]).strip().decode("utf-8")
        )
        subprocess.run(["docker", "login", "--username", "AWS", "--password", login_password, ecr_registry], check=True)

        # Tag and push the image to ECR
        image_uri = f"{ecr_registry}/{repository_name}:latest"
        try:
            subprocess.run(["docker", "tag", image_name, image_uri], check=True)
            subprocess.run(["docker", "push", image_uri], check=True)
            logger.info("Successfully pushed Docker image %s to ECR.", image_uri)
        except subprocess.CalledProcessError as e:
            logger.error("Failed to push Docker image %s to ECR. Error: %s", image_name, e)
            raise

        return image_uri

    def update_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: Optional[bytes],
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
        deployed_image_uri = self._get_deployed_image_uri(function_name)
        client = self._client("lambda")
        if len(deployed_image_uri) > 0:
            image_uri = self._copy_image_to_region(deployed_image_uri)
        else:
            if zip_contents is None:
                raise RuntimeError("No deployed image AND No deployment package provided for function update")

            # Process the ZIP contents to build and upload a Docker image,
            # then update the function code with the image URI
            with tempfile.TemporaryDirectory() as tmpdirname:
                zip_path = os.path.join(tmpdirname, "code.zip")
                with open(zip_path, "wb") as f_zip:
                    f_zip.write(zip_contents)
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(tmpdirname)

                dockerfile_content = self._generate_dockerfile(runtime, handler, additional_docker_commands)
                with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
                    f_dockerfile.write(dockerfile_content)

                image_name = f"{function_name.lower()}:latest"
                self._build_docker_image(tmpdirname, image_name)
                image_uri = self._upload_image_to_ecr(image_name)
                self._store_deployed_image_uri(function_name, image_uri)

        response = client.update_function_code(FunctionName=function_name, ImageUri=image_uri)

        time.sleep(self.DELAY_TIME)
        if response.get("State") != "Active":
            self._wait_for_function_to_become_active(function_name)

        kwargs = {
            "FunctionName": function_name,
            "Role": role_identifier,
            "Environment": {"Variables": environment_variables},
            "MemorySize": memory_size,
        }
        if timeout >= 1:
            kwargs["Timeout"] = timeout

        try:
            response = client.update_function_configuration(**kwargs)
        except ClientError as e:
            logger.error("Error while updating function configuration: %s", e)

        if response.get("State") != "Active":
            self._wait_for_function_to_become_active(function_name)

        return response["FunctionArn"]

    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        if len(role_name) > 64:
            role_name = role_name[:64]
        client = self._client("iam")
        response = client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))
        self.put_role_policy(role_name=role_name, policy_name=role_name, policy_document=policy)

        time.sleep(self.DELAY_TIME * 2)  # Wait for role to become active
        return response["Role"]["Arn"]

    def _wait_for_role_to_become_active(self, role_name: str) -> None:
        client = self._client("iam")
        for _ in range(self.LAMBDA_CREATE_ATTEMPTS):
            response = client.get_role(RoleName=role_name)
            state = response["Role"]["State"]
            if state == "Active":
                return
            time.sleep(self.DELAY_TIME)
        raise RuntimeError(f"Role {role_name} did not become active")

    def put_role_policy(self, role_name: str, policy_name: str, policy_document: str) -> None:
        client = self._client("iam")
        client.put_role_policy(RoleName=role_name, PolicyName=policy_name, PolicyDocument=policy_document)

    def update_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        client = self._client("iam")
        try:
            current_role_policy = client.get_role_policy(RoleName=role_name, PolicyName=role_name)
            if current_role_policy["PolicyDocument"] != policy:
                client.delete_role_policy(RoleName=role_name, PolicyName=role_name)
                self.put_role_policy(role_name=role_name, policy_name=role_name, policy_document=policy)
        except ClientError:
            self.put_role_policy(role_name=role_name, policy_name=role_name, policy_document=policy)
        try:
            current_trust_policy = client.get_role(RoleName=role_name)["Role"]["AssumeRolePolicyDocument"]
            if current_trust_policy != trust_policy:
                client.delete_role(RoleName=role_name)
                client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))
        except ClientError:
            client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))
        try:
            return self.get_iam_role(role_name)
        except ClientError:
            self._wait_for_role_to_become_active(role_name)
            return self.get_iam_role(role_name)

    def _create_lambda_function(self, kwargs: dict[str, Any]) -> tuple[str, str]:
        client = self._client("lambda")
        response = client.create_function(**kwargs)
        return response["FunctionArn"], response["State"]

    def _wait_for_function_to_become_active(self, function_name: str) -> None:
        client = self._client("lambda")
        for _ in range(self.LAMBDA_CREATE_ATTEMPTS):
            response = client.get_function(FunctionName=function_name)
            state = response["Configuration"]["State"]
            if state == "Active":
                return
            time.sleep(self.DELAY_TIME)
        raise RuntimeError(f"Lambda function {function_name} did not become active")

    def create_sns_topic(self, topic_name: str) -> str:
        client = self._client("sns")
        # If topic exists, the following will return the existing topic
        response = client.create_topic(Name=topic_name)
        # See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/create_topic.html
        return response["TopicArn"]

    def subscribe_sns_topic(self, topic_arn: str, protocol: str, endpoint: str) -> None:
        client = self._client("sns")
        response = client.subscribe(
            TopicArn=topic_arn,
            Protocol=protocol,
            Endpoint=endpoint,
            ReturnSubscriptionArn=True,
        )
        return response["SubscriptionArn"]

    def add_lambda_permission_for_sns_topic(self, topic_arn: str, lambda_function_arn: str) -> None:
        client = self._client("lambda")
        try:
            client.remove_permission(FunctionName=lambda_function_arn, StatementId="sns")
        except ClientError:
            # No permission to remove
            pass
        client.add_permission(
            FunctionName=lambda_function_arn,
            StatementId="sns",
            Action="lambda:InvokeFunction",
            Principal="sns.amazonaws.com",
            SourceArn=topic_arn,
        )

    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        client = self._client("sns")
        client.publish(TopicArn=identifier, Message=message)

    def set_value_in_table(self, table_name: str, key: str, value: str, convert_to_bytes: bool = False) -> None:
        client = self._client("dynamodb")

        if convert_to_bytes:
            client.put_item(TableName=table_name, Item={"key": {"S": key}, "value": {"B": compress_json_str(value)}})
        else:
            client.put_item(TableName=table_name, Item={"key": {"S": key}, "value": {"S": value}})

    def update_value_in_table(self, table_name: str, key: str, value: str, convert_to_bytes: bool = False) -> None:
        client = self._client("dynamodb")
        expression_attribute_values: dict[str, Any]
        if convert_to_bytes:
            expression_attribute_values = {":value": {"B": compress_json_str(value)}}
        else:
            expression_attribute_values = {":value": {"S": value}}

        client.update_item(
            TableName=table_name,
            Key={"key": {"S": key}},
            UpdateExpression="SET #v = :value",
            ExpressionAttributeNames={"#v": "value"},
            ExpressionAttributeValues=expression_attribute_values,
        )

    def set_value_in_table_column(
        self, table_name: str, key: str, column_type_value: list[tuple[str, str, str]]
    ) -> None:
        client = self._client("dynamodb")
        expression_attribute_names = {}
        expression_attribute_values = {}
        update_expression = "SET "
        for column, type_, value in column_type_value:
            expression_attribute_names[f"#{column}"] = column
            expression_attribute_values[f":{column}"] = {type_: value}
            update_expression += f"#{column} = :{column}, "
        update_expression = update_expression[:-2]
        client.update_item(
            TableName=table_name,
            Key={"key": {"S": key}},
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values,
            UpdateExpression=update_expression,
        )

    def get_value_from_table(
        self, table_name: str, key: str, consistent_read: bool = True, convert_from_bytes: bool = False
    ) -> tuple[str, float]:
        client = self._client("dynamodb")
        response = client.get_item(
            TableName=table_name,
            Key={"key": {"S": key}},
            ConsistentRead=consistent_read,
            ReturnConsumedCapacity="TOTAL",
        )

        # Record the consumed capacity (Read Capacity Units) for the sync node
        consumed_read_capacity = response.get("ConsumedCapacity", {}).get("CapacityUnits", 0.0)

        if "Item" not in response:
            return "", consumed_read_capacity

        item = response.get("Item")
        if item is not None and "value" in item:
            if convert_from_bytes:
                return decompress_json_str(item["value"]["B"]), consumed_read_capacity

            return item["value"]["S"], consumed_read_capacity

        return "", consumed_read_capacity

    def remove_value_from_table(self, table_name: str, key: str) -> None:
        client = self._client("dynamodb")
        client.delete_item(TableName=table_name, Key={"key": {"S": key}})

    def get_all_values_from_table(self, table_name: str, convert_from_bytes: bool = False) -> dict[str, Any]:
        client = self._client("dynamodb")
        response = client.scan(TableName=table_name)
        if "Items" not in response:
            return {}
        items = response.get("Items")
        if items is not None:
            if convert_from_bytes:
                return {item["key"]["S"]: decompress_json_str(item["value"]["B"]) for item in items}

            return {item["key"]["S"]: item["value"]["S"] for item in items}

        return {}

    def get_key_present_in_table(self, table_name: str, key: str, consistent_read: bool = True) -> bool:
        client = self._client("dynamodb")
        response = client.get_item(TableName=table_name, Key={"key": {"S": key}}, ConsistentRead=consistent_read)
        return "Item" in response

    def upload_resource(self, key: str, resource: bytes) -> None:
        client = self._client("s3")
        try:
            client.put_object(Body=resource, Bucket=self._deployment_resource_bucket, Key=key)
        except ClientError as e:
            raise RuntimeError(
                f"Could not upload resource {key} to S3, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e

    def download_resource(self, key: str) -> bytes:
        client = self._client("s3")
        try:
            response = client.get_object(Bucket=self._deployment_resource_bucket, Key=key)
        except ClientError as e:
            raise RuntimeError(
                f"Could not upload resource {key} to S3, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e
        return response["Body"].read()

    def get_keys(self, table_name: str) -> list[str]:
        client = self._client("dynamodb")
        response = client.scan(TableName=table_name)
        if "Items" not in response:
            return []
        items = response.get("Items")
        if items is not None:
            return [item["key"]["S"] for item in items]
        return []

    def get_logs_since(self, function_instance: str, since: datetime) -> list[str]:
        time_ms_since_epoch = int(time.mktime(since.timetuple())) * 1000
        client = self._client("logs")

        next_token = None

        log_events: list[str] = []
        while True:
            if next_token:
                response = client.filter_log_events(
                    logGroupName=f"/aws/lambda/{function_instance}",
                    startTime=time_ms_since_epoch,
                    nextToken=next_token,
                )
            else:
                try:
                    response = client.filter_log_events(
                        logGroupName=f"/aws/lambda/{function_instance}", startTime=time_ms_since_epoch
                    )
                except client.exceptions.ResourceNotFoundException:
                    # No logs found
                    return []

            log_events.extend(event["message"] for event in response.get("events", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return log_events

    def get_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        time_ms_start = int(start.timestamp() * 1000)
        time_ms_end = int(end.timestamp() * 1000)
        client = self._client("logs")

        next_token = None

        log_events: list[str] = []
        while True:
            if next_token:
                response = client.filter_log_events(
                    logGroupName=f"/aws/lambda/{function_instance}",
                    startTime=time_ms_start,
                    endTime=time_ms_end,
                    nextToken=next_token,
                )
            else:
                try:
                    response = client.filter_log_events(
                        logGroupName=f"/aws/lambda/{function_instance}", startTime=time_ms_start, endTime=time_ms_end
                    )
                except client.exceptions.ResourceNotFoundException:
                    # No logs found
                    return []

            log_events.extend(event["message"] for event in response.get("events", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return log_events

    def get_insights_logs_between(self, function_instance: str, start: datetime, end: datetime) -> list[str]:
        time_ms_start = int(start.timestamp() * 1000)
        time_ms_end = int(end.timestamp() * 1000)
        client = self._client("logs")

        next_token = None

        log_events: list[str] = []
        while True:
            if next_token:
                response = client.filter_log_events(
                    logGroupName="/aws/lambda-insights",
                    logStreamNamePrefix=function_instance,
                    startTime=time_ms_start,
                    endTime=time_ms_end,
                    nextToken=next_token,
                )
            else:
                try:
                    response = client.filter_log_events(
                        logGroupName="/aws/lambda-insights",
                        logStreamNamePrefix=function_instance,
                        startTime=time_ms_start,
                        endTime=time_ms_end,
                    )
                except client.exceptions.ResourceNotFoundException:
                    # No logs found
                    return []

            log_events.extend(event["message"] for event in response.get("events", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return log_events

    def remove_key(self, table_name: str, key: str) -> None:
        client = self._client("dynamodb")

        try:
            client.delete_item(TableName=table_name, Key={"key": {"S": key}})
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                print(f"Table '{table_name}' Remove Key '{key}' Error.")
                print(f"{e.response['Error']['Code']}: {e.response['Error']['Message']}")
            else:
                raise

    def remove_function(self, function_name: str) -> None:
        client = self._client("lambda")
        client.delete_function(FunctionName=function_name)

    def remove_role(self, role_name: str) -> None:
        client = self._client("iam")

        managed_policies = client.list_attached_role_policies(RoleName=role_name)
        for policy in managed_policies.get("AttachedPolicies", []):
            client.detach_role_policy(RoleName=role_name, PolicyArn=policy["PolicyArn"])

        inline_policies = client.list_role_policies(RoleName=role_name)
        for policy_name in inline_policies.get("PolicyNames", []):
            client.delete_role_policy(RoleName=role_name, PolicyName=policy_name)

        client.delete_role(RoleName=role_name)

    def remove_messaging_topic(self, topic_identifier: str) -> None:
        client = self._client("sns")

        # Get all subscriptions for the topic
        response = client.list_subscriptions_by_topic(TopicArn=topic_identifier)
        subscriptions = response.get("Subscriptions", [])

        # Handle pagination if there are more subscriptions
        while "NextToken" in response:
            response = client.list_subscriptions_by_topic(TopicArn=topic_identifier, NextToken=response["NextToken"])
            subscriptions.extend(response.get("Subscriptions", []))

        # Unsubscribe each subscription
        for subscription in subscriptions:
            client.unsubscribe(SubscriptionArn=subscription["SubscriptionArn"])

        # Delete the topic after unsubscribing all its subscriptions
        client.delete_topic(TopicArn=topic_identifier)

    def get_topic_identifier(self, topic_name: str) -> str:
        client = self._client("sns")
        next_token = ""

        while True:
            response = client.list_topics(NextToken=next_token) if next_token else client.list_topics()

            for topic in response["Topics"]:
                if topic_name in topic["TopicArn"]:
                    return topic["TopicArn"]

            next_token = response.get("NextToken")
            if not next_token:
                break

        raise RuntimeError(f"Topic {topic_name} not found")

    def remove_resource(self, key: str) -> None:
        client = self._client("s3")
        try:
            client.delete_object(Bucket=self._deployment_resource_bucket, Key=key)
        except ClientError as e:
            raise RuntimeError(
                f"Could not upload resource {key} to S3, does the bucket {self._deployment_resource_bucket} exist and do you have permission to access it: {str(e)}"  # pylint: disable=line-too-long
            ) from e

    def remove_ecr_repository(self, repository_name: str) -> None:
        repository_name = repository_name.lower()
        client = self._client("ecr")
        client.delete_repository(repositoryName=repository_name, force=True)

    def ecr_repository_exists(self, resource: Resource) -> bool:
        repository_name = resource.name.lower()
        client = self._client("ecr")

        try:
            response = client.describe_repositories(repositoryNames=[repository_name])
        except ClientError:
            return False

        return response["repositories"] is not None and len(response["repositories"]) > 0

    def deploy_remote_cli(
        self,
        function_name: str,
        handler: str,
        role_arn: str,
        timeout: int,
        memory_size: int,
        ephemeral_storage: int,
        zip_contents: bytes,
        tmpdirname: str,
        env_vars: dict,
    ) -> None:
        # Step 1: Unzip the ZIP file
        zip_path = os.path.join(tmpdirname, "code.zip")
        with open(zip_path, "wb") as f_zip:
            f_zip.write(zip_contents)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(tmpdirname)

        # Step 2: Create a Dockerfile in the temporary directory
        dockerfile_content = self._generate_framework_dockerfile(handler, env_vars)
        with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
            f_dockerfile.write(dockerfile_content)

        # Step 3: Build the Docker Image
        image_name = f"{function_name.lower()}:latest"
        self._build_docker_image(tmpdirname, image_name)

        # Step 4: Upload the Image to ECR
        image_uri = self._upload_image_to_ecr(image_name)
        self._create_framework_lambda_function(
            function_name, image_uri, role_arn, timeout, memory_size, ephemeral_storage
        )

    def _generate_framework_dockerfile(self, handler: str, env_vars: dict) -> str:
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
            chmod +x build_caribou_no_tests.sh && \
            ./build_caribou_no_tests.sh

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

    def _create_framework_lambda_function(
        self, function_name: str, image_uri: str, role: str, timeout: int, memory_size: int, ephemeral_storage_size: int
    ) -> str:
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Role": role,
            "Code": {"ImageUri": image_uri},
            "PackageType": "Image",
            "Timeout": timeout,
            "MemorySize": memory_size,
            "EphemeralStorage": {"Size": ephemeral_storage_size},
        }
        if timeout >= 1:
            kwargs["Timeout"] = timeout
        arn, state = self._create_lambda_function(kwargs)

        if state != "Active":
            self._wait_for_function_to_become_active(function_name)

        print(f"Caribou Lambda Framework remote cli function {function_name}" f" created successfully, with ARN: {arn}")

        return arn

    def get_timer_rule_schedule_expression(self, rule_name: str) -> Optional[str]:
        """Retrieve the schedule expression of a timer rule if it exist."""
        try:
            events_client = self._client("events")

            # Describe the rule using the EventBridge client
            rule_details = events_client.describe_rule(Name=rule_name)

            # Return the rule schedule expression
            return rule_details.get("ScheduleExpression")
        except ClientError as e:
            # Check if its ResourceNotFoundException, which means the rule doesn't exist
            # We don't need to do anything in this case
            if not e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"Error removing the EventBridge rule {rule_name}: {e}")

            return None

    def remove_timer_rule(self, lambda_function_name: str, rule_name: str) -> None:
        """Remove the EventBridge rule and its associated targets."""
        try:
            events_client = self._client("events")

            # Remove the targets from the rule
            events_client.remove_targets(Rule=rule_name, Ids=[f"{lambda_function_name}-target"])

            # Delete the rule itself
            events_client.delete_rule(
                Name=rule_name, Force=True  # Ensures the rule is deleted even if it's still in use
            )
        except ClientError as e:
            # Check if its ResourceNotFoundException, which means the rule doesn't exist
            # We don't need to do anything in this case
            if not e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"Error removing the EventBridge rule {rule_name}: {e}")

    def event_bridge_permission_exists(self, lambda_function_name: str, statement_id: str) -> bool:
        """Check if a specific permission exists in the Lambda function's policy based on the StatementId."""
        try:
            lambda_client = self._client("lambda")

            # Get the current policy for the Lambda function
            policy_response = lambda_client.get_policy(FunctionName=lambda_function_name)

            # Parse the policy JSON
            policy_statements = json.loads(policy_response["Policy"])["Statement"]

            # Check if a permission with the given StatementId exists
            for statement in policy_statements:
                if statement["Sid"] == statement_id:
                    return True

            return False  # If no matching StatementId is found, return False
        except ClientError as e:
            if not e.response["Error"]["Code"] == "ResourceNotFoundException":
                print(f"Error in asserting if permission exists {lambda_function_name} - {statement_id}: {e}")

            return False

    def create_timer_rule(
        self, lambda_function_name: str, schedule_expression: str, rule_name: str, event_payload: str
    ) -> None:
        # Initialize the EventBridge and Lambda clients
        events_client = self._client("events")
        lambda_client = self._client("lambda")

        # Create a rule with the specified schedule expression
        response = events_client.put_rule(Name=rule_name, ScheduleExpression=schedule_expression, State="ENABLED")
        rule_arn = response["RuleArn"]

        # Add permission for EventBridge to invoke the Lambda function
        statement_id = f"{rule_name}-invoke-lambda"
        if not self.event_bridge_permission_exists(lambda_function_name, statement_id):
            lambda_client.add_permission(
                FunctionName=lambda_function_name,
                StatementId=statement_id,
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=rule_arn,
            )

        # Get the ARN of the Lambda function
        lambda_arn = self.get_lambda_function(lambda_function_name)["FunctionArn"]

        # Attach the Lambda function to the rule
        events_client.put_targets(
            Rule=rule_name,
            Targets=[{"Id": f"{lambda_function_name}-target", "Arn": lambda_arn, "Input": event_payload}],
        )

    def invoke_remote_framework_internal_action(self, action_type: str, action_events: dict[str, Any]) -> None:
        payload = {
            "action": "internal_action",
            "type": action_type,
            "event": action_events,
        }

        self.invoke_remote_framework_with_payload(payload, invocation_type="Event")

    def invoke_remote_framework_with_payload(self, payload: dict[str, Any], invocation_type: str = "Event") -> None:
        # Get the boto3 lambda client
        lambda_client = self._client("lambda")
        remote_framework_cli_name = REMOTE_CARIBOU_CLI_FUNCTION_NAME

        # Invoke the lambda function with the payload
        lambda_client.invoke(
            FunctionName=remote_framework_cli_name, InvocationType=invocation_type, Payload=json.dumps(payload)
        )
