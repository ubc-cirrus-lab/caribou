import json
import time
from typing import Any

from boto3.session import Session
from botocore.exceptions import ClientError

from multi_x_serverless.deployment.common.deploy.models.resource import Resource
from multi_x_serverless.deployment.common.remote_client.remote_client import RemoteClient


class AWSRemoteClient(RemoteClient):
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

    def increment_counter(self, function_name: str, workflow_instance_id: str) -> int:
        client = self._client("dynamodb")
        counter_id = f"{function_name}:{workflow_instance_id}"
        # Atomic counter update
        response = client.update_item(
            TableName="counters",
            Key={"id": {"S": counter_id}},
            UpdateExpression="SET counter_value = counter_value + :val",
            ExpressionAttributeValues={":val": 1},
            ReturnValues="UPDATED_NEW",
        )
        return int(response["Attributes"]["counter_value"]["N"])

    def upload_message_for_merge(self, function_name: str, workflow_instance_id: str, message: str) -> None:
        client = self._client("dynamodb")
        merge_id = f"{function_name}:{workflow_instance_id}"
        response = client.update_item(
            TableName="merge_messages",
            Key={"id": {"S": merge_id}},
            ExpressionAttributeNames={
                "#M": "message",
            },
            ExpressionAttributeValues={
                ":m": {"SS": [message]},
            },
            UpdateExpression="ADD #M :m",
        )
        print(f"Successfully uploaded message {message} for merge")
        return response

    def get_predecessor_data(
        self, current_instance_name: str, workflow_instance_id: str  # pylint: disable=unused-argument
    ) -> list[str]:
        client = self._client("dynamodb")
        merge_id = f"{current_instance_name}:{workflow_instance_id}"
        response = client.get_item(
            TableName="merge_messages",
            Key={"id": {"S": merge_id}},
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
        role_arn: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Runtime": runtime,
            "Code": {"ZipFile": zip_contents},
            "Handler": handler,
            "Role": role_arn,
            "Environment": {"Variables": environment_variables},
            "MemorySize": memory_size,
        }
        if timeout >= 1:
            kwargs["Timeout"] = timeout
        arn, state = self._create_lambda_function(kwargs)

        if state != "Active":
            self._wait_for_function_to_become_active(function_name)
        print(f"Successfully created function {function_name}")
        return arn

    def update_function(
        self,
        function_name: str,
        role_arn: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
    ) -> str:
        client = self._client("lambda")
        response = client.update_function_code(FunctionName=function_name, ZipFile=zip_contents)
        time.sleep(self.DELAY_TIME)
        if response["State"] != "Active":
            self._wait_for_function_to_become_active(function_name)
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Runtime": runtime,
            "Handler": handler,
            "Role": role_arn,
            "Environment": {"Variables": environment_variables},
            "MemorySize": memory_size,
        }

        if timeout >= 1:
            kwargs["Timeout"] = timeout

        response = client.update_function_configuration(**kwargs)
        if response["State"] != "Active":
            self._wait_for_function_to_become_active(function_name)

        print(f"Successfully updated function {function_name}")
        return response["FunctionArn"]

    def create_role(self, role_name: str, policy: str, trust_policy: dict) -> str:
        client = self._client("iam")
        response = client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))
        self.put_role_policy(role_name=role_name, policy_name=role_name, policy_document=policy)

        time.sleep(self.DELAY_TIME * 2)  # Wait for role to become active
        print(f"Successfully created role {role_name}")
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
        print(f"Successfully updated role {role_name}")
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
        response = client.create_topic(Name=topic_name)  # If topic exists, this will return the existing topic
        # See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/create_topic.html
        print(f"Successfully created SNS topic {topic_name}")
        return response["TopicArn"]

    def subscribe_sns_topic(self, topic_arn: str, protocol: str, endpoint: str) -> None:
        client = self._client("sns")
        response = client.subscribe(
            TopicArn=topic_arn,
            Protocol=protocol,
            Endpoint=endpoint,
            ReturnSubscriptionArn=True,
        )
        print(f"Successfully subscribed function {endpoint} to SNS topic {topic_arn}")
        return response["SubscriptionArn"]

    def add_lambda_permission_for_sns_topic(self, topic_arn: str, lambda_function_arn: str) -> None:
        client = self._client("lambda")
        client.add_permission(
            FunctionName=lambda_function_arn,
            StatementId="sns",
            Action="lambda:InvokeFunction",
            Principal="sns.amazonaws.com",
            SourceArn=topic_arn,
        )
        print(f"Successfully added lambda permission for SNS topic {topic_arn}")

    def send_message_to_messaging_service(self, identifier: str, message: str) -> None:
        client = self._client("sns")
        client.publish(TopicArn=identifier, Message=message)
        print(f"Successfully sent message to SNS topic {identifier}")

    def set_value_in_table(self, table_name: str, key: str, value: str) -> None:
        client = self._client("dynamodb")
        client.put_item(TableName=table_name, Item={key: {"S": value}})
        print(f"Successfully set value {value} in table {table_name}")

    def upload_resource(self, key: str, resource: bytes) -> None:
        client = self._client("s3")
        client.put_object(Body=resource, Bucket="multi-x-serverless-resources", Key=key)
        print(f"Successfully uploaded resource {key}")

    def download_resource(self, key: str) -> bytes:
        client = self._client("s3")
        response = client.get_object(Bucket="multi-x-serverless-resources", Key=key)
        return response["Body"].read()
