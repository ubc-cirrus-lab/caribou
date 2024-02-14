import json
import time
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
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Runtime": runtime,
            "Code": {"ZipFile": zip_contents},
            "Handler": handler,
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
        response = client.update_function_code(FunctionName=function_name, ZipFile=zip_contents)
        time.sleep(self.DELAY_TIME)
        if response["State"] != "Active":
            self._wait_for_function_to_become_active(function_name)
        kwargs: dict[str, Any] = {
            "FunctionName": function_name,
            "Runtime": runtime,
            "Handler": handler,
            "Role": role_identifier,
            "Environment": {"Variables": environment_variables},
            "MemorySize": memory_size,
        }

        if timeout >= 1:
            kwargs["Timeout"] = timeout

        response = client.update_function_configuration(**kwargs)
        if response["State"] != "Active":
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

    def get_all_values_from_sort_key_table(self, table_name: str, key: str) -> list[dict[str, Any]]:
        client = self._client("dynamodb")
        response = client.query(
            TableName=table_name,
            KeyConditionExpression="key = :key ",
            ExpressionAttributeValues={":key": {"S": key}},
        )
        return [item["value"]["S"] for item in response.get("Items", [])]

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
        response = client.query(
            TableName=table_name,
            KeyConditionExpression="key = :key ",
            ExpressionAttributeValues={":key": {"S": key}},
            ScanIndexForward=False,
            Limit=1,
        )
        if "Items" not in response:
            return "", ""
        items = response.get("Items")
        if items is not None:
            return (items[0]["sort_key"]["S"], items[0]["value"]["S"])
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
