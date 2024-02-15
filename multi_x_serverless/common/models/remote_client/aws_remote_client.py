import json
import os
import subprocess
import tempfile
import time
import zipfile
from datetime import datetime
from typing import Any

from boto3.session import Session
from botocore.exceptions import ClientError

from multi_x_serverless.common.constants import SYNC_MESSAGES_TABLE, SYNC_PREDECESSOR_COUNTER_TABLE
from multi_x_serverless.common.models.remote_client.remote_client import RemoteClient
from multi_x_serverless.deployment.common.deploy.models.resource import Resource


class AWSRemoteClient(RemoteClient):  # pylint: disable=too-many-public-methods
    LAMBDA_CREATE_ATTEMPTS = 30
    DELAY_TIME = 5

    def __init__(self, region: str) -> None:
        self._session = Session(region_name=region)
        self._client_cache: dict[str, Any] = {}

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

    def set_predecessor_reached(self, predecessor_name: str, sync_node_name: str, workflow_instance_id: str) -> int:
        client = self._client("dynamodb")
        response = client.update_item(
            TableName=SYNC_PREDECESSOR_COUNTER_TABLE,
            Key={"id": {"S": workflow_instance_id}},
            UpdateExpression="SET #s = list_append(if_not_exists(#s, :empty_list), :new_predecessor)",
            ExpressionAttributeNames={"#s": sync_node_name},
            ExpressionAttributeValues={":new_predecessor": {"L": [{"S": predecessor_name}]}, ":empty_list": {"L": []}},
            ReturnValues="UPDATED_NEW",
        )

        return len(response["Attributes"][sync_node_name]["L"])

    def create_sync_tables(self) -> None:
        # Check if table exists
        client = self._client("dynamodb")
        for table in [SYNC_MESSAGES_TABLE, SYNC_PREDECESSOR_COUNTER_TABLE]:
            try:
                client.describe_table(TableName=table)
            except client.exceptions.ResourceNotFoundException:
                client.create_table(
                    TableName=table,
                    KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                    AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                    BillingMode="PAY_PER_REQUEST",
                )

    def upload_predecessor_data_at_sync_node(self, function_name: str, workflow_instance_id: str, message: str) -> None:
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
        )
        return response

    def get_predecessor_data(
        self, current_instance_name: str, workflow_instance_id: str  # pylint: disable=unused-argument
    ) -> list[str]:
        client = self._client("dynamodb")
        sync_node_id = f"{current_instance_name}:{workflow_instance_id}"
        response = client.get_item(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": sync_node_id}},
        )
        if "Item" not in response:
            return []
        item = response.get("Item")
        if item is not None and "message" in item:
            return item["message"]["SS"]
        return []

    def create_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        with tempfile.TemporaryDirectory() as tmpdirname:
            # Step 1: Unzip the ZIP file
            zip_path = os.path.join(tmpdirname, "code.zip")
            with open(zip_path, "wb") as f_zip:
                f_zip.write(zip_contents)
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmpdirname)

            # Step 2: Create a Dockerfile in the temporary directory
            dockerfile_content = self.generate_dockerfile(runtime, handler)
            with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
                f_dockerfile.write(dockerfile_content)

            # Step 3: Build the Docker Image
            image_name = f"{function_name.lower()}:latest"
            self.build_docker_image(tmpdirname, image_name)

            # Step 4: Upload the Image to ECR
            image_uri = self.upload_image_to_ecr(image_name)

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

    def generate_dockerfile(self, runtime: str, handler: str) -> str:
        return f"""
        FROM public.ecr.aws/lambda/{runtime.replace("python", "python:")}
        COPY requirements.txt ./
        RUN pip3 install --no-cache-dir -r requirements.txt
        COPY app.py ./
        COPY src ./src
        COPY multi_x_serverless ./multi_x_serverless
        CMD ["{handler}"]
        """

    def build_docker_image(self, context_path: str, image_name: str) -> None:
        try:
            subprocess.run(["docker", "build", "--platform", "linux/amd64", "-t", image_name, context_path], check=True)
            print(f"Docker image {image_name} built successfully.")
        except subprocess.CalledProcessError as e:
            # This will catch errors from the subprocess and print a message.
            print(f"Failed to build Docker image {image_name}. Error: {e}")

    def upload_image_to_ecr(self, image_name: str) -> str:
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
            subprocess.check_output(["aws", "ecr", "get-login-password", "--region", region]).strip().decode("utf-8")
        )
        subprocess.run(["docker", "login", "--username", "AWS", "--password", login_password, ecr_registry], check=True)

        # Tag and push the image to ECR
        image_uri = f"{ecr_registry}/{repository_name}:latest"
        try:
            subprocess.run(["docker", "tag", image_name, image_uri], check=True)
            subprocess.run(["docker", "push", image_uri], check=True)
            print(f"Successfully pushed Docker image {image_uri} to ECR.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to push Docker image {image_name} to ECR. Error: {e}")
            raise

        return image_uri

    def update_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        client = self._client("lambda")

        # Process the ZIP contents to build and upload a Docker image, then update the function code with the image URI
        with tempfile.TemporaryDirectory() as tmpdirname:
            zip_path = os.path.join(tmpdirname, "code.zip")
            with open(zip_path, "wb") as f_zip:
                f_zip.write(zip_contents)
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmpdirname)

            dockerfile_content = self.generate_dockerfile(runtime, handler)
            with open(os.path.join(tmpdirname, "Dockerfile"), "w", encoding="utf-8") as f_dockerfile:
                f_dockerfile.write(dockerfile_content)

            image_name = f"{function_name.lower()}:latest"
            self.build_docker_image(tmpdirname, image_name)
            image_uri = self.upload_image_to_ecr(image_name)

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
            print(f"Error while updating function configuration: {e}")

        if response.get("State") != "Active":
            self._wait_for_function_to_become_active(function_name)

        return response["FunctionArn"]

    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
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

    def set_value_in_table(self, table_name: str, key: str, value: str) -> None:
        client = self._client("dynamodb")
        client.put_item(TableName=table_name, Item={"key": {"S": key}, "value": {"S": value}})

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

    def get_value_from_table(self, table_name: str, key: str) -> str:
        client = self._client("dynamodb")
        response = client.get_item(TableName=table_name, Key={"key": {"S": key}})
        if "Item" not in response:
            return ""
        item = response.get("Item")
        if item is not None and "value" in item:
            return item["value"]["S"]
        return ""

    def remove_value_from_table(self, table_name: str, key: str) -> None:
        client = self._client("dynamodb")
        client.delete_item(TableName=table_name, Key={"key": {"S": key}})

    def get_all_values_from_table(self, table_name: str) -> dict[str, Any]:
        client = self._client("dynamodb")
        response = client.scan(TableName=table_name)
        if "Items" not in response:
            return {}
        items = response.get("Items")
        if items is not None:
            return {item["key"]["S"]: item["value"]["S"] for item in items}
        return {}

    def get_key_present_in_table(self, table_name: str, key: str) -> bool:
        client = self._client("dynamodb")
        response = client.get_item(TableName=table_name, Key={"key": {"S": key}})
        return "Item" in response

    def upload_resource(self, key: str, resource: bytes) -> None:
        client = self._client("s3")
        client.put_object(Body=resource, Bucket="multi-x-serverless-resources", Key=key)

    def download_resource(self, key: str) -> bytes:
        client = self._client("s3")
        response = client.get_object(Bucket="multi-x-serverless-resources", Key=key)
        return response["Body"].read()

    def get_all_values_from_sort_key_table(self, table_name: str, key: str) -> list[str]:
        client = self._client("dynamodb")
        try:
            response = client.query(
                TableName=table_name,
                KeyConditionExpression="#pk = :pkValue",
                ExpressionAttributeValues={":pkValue": {"S": key}},
                ExpressionAttributeNames={"#pk": "key"},
            )
        except client.exceptions.DynamoDBError as e:
            print(f"Error querying DynamoDB: {e}")
            return []

        return [item.get("value", {}).get("S", "") for item in response.get("Items", [])]

    def get_keys(self, table_name: str) -> list[str]:
        client = self._client("dynamodb")
        response = client.scan(TableName=table_name)
        if "Items" not in response:
            return []
        items = response.get("Items")
        if items is not None:
            return [item["key"]["S"] for item in items]
        return []

    def get_last_value_from_sort_key_table(self, table_name: str, key: str) -> tuple[str, str]:
        client = self._client("dynamodb")
        try:
            response = client.query(
                TableName=table_name,
                KeyConditionExpression="#pk = :pkValue",
                ExpressionAttributeValues={":pkValue": {"S": key}},
                ExpressionAttributeNames={"#pk": "key"},
                ScanIndexForward=False,  # Sorts the results in descending order based on the sort key
                Limit=1,
            )
        except client.exceptions.DynamoDBError as e:
            print(f"Error querying DynamoDB: {e}")
            return "", ""

        # Check if any items were returned
        if response.get("Items"):
            item = response["Items"][0]
            sort_key = item.get("sort_key", {}).get("S", "")
            value = item.get("value", {}).get("S", "")
            return sort_key, value

        # Return empty if no items found
        return "", ""

    def put_value_to_sort_key_table(self, table_name: str, key: str, sort_key: str, value: str) -> None:
        client = self._client("dynamodb")
        client.put_item(
            TableName=table_name,
            Item={"key": {"S": key}, "sort_key": {"S": sort_key}, "value": {"S": value}},
        )

    def get_logs_since_last_sync(self, function_instance: str, last_synced_time: datetime) -> list[str]:
        last_synced_time_ms_since_epoch = int(time.mktime(last_synced_time.timetuple())) * 1000
        client = self._client("logs")

        next_token = None

        log_events: list[str] = []
        while True:
            if next_token:
                response = client.filter_log_events(
                    logGroupName=f"/aws/lambda/{function_instance}",
                    startTime=last_synced_time_ms_since_epoch,
                    nextToken=next_token,
                )
            else:
                response = client.filter_log_events(
                    logGroupName=f"/aws/lambda/{function_instance}", startTime=last_synced_time_ms_since_epoch
                )

            log_events.extend(event["message"] for event in response.get("events", []))

            next_token = response.get("nextToken")
            if not next_token:
                break

        return log_events

    def remove_key(self, table_name: str, key: str) -> None:
        client = self._client("dynamodb")
        client.delete_item(TableName=table_name, Key={"key": {"S": key}})

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
        client.delete_object(Bucket="multi-x-serverless-resources", Key=key)

    def remove_ecr_repository(self, repository_name: str) -> None:
        repository_name = repository_name.lower()
        client = self._client("ecr")
        client.delete_repository(repositoryName=repository_name, force=True)
