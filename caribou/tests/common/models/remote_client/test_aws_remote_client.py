from io import StringIO
import os
import sys
import unittest
from unittest.mock import patch
from unittest.mock import MagicMock
from datetime import datetime, timedelta

from caribou.common.models.remote_client.aws_remote_client import AWSRemoteClient
from caribou.deployment.common.deploy.models.resource import Resource

import json
import zipfile
import tempfile

from botocore.exceptions import ClientError
from unittest.mock import call

from caribou.common.constants import (
    REMOTE_CARIBOU_CLI_FUNCTION_NAME,
    SYNC_MESSAGES_TABLE,
    CARIBOU_WORKFLOW_IMAGES_TABLE,
    GLOBAL_TIME_ZONE,
    DEPLOYMENT_RESOURCES_BUCKET,
    SYNC_TABLE_TTL,
    SYNC_TABLE_TTL_ATTRIBUTE_NAME,
)


class TestAWSRemoteClient(unittest.TestCase):
    @patch("boto3.session.Session")
    def setUp(self, mock_session):
        self.region = "region1"
        self.aws_client = AWSRemoteClient(self.region)
        self.mock_session = mock_session
        self.aws_client.LAMBDA_CREATE_ATTEMPTS = 2
        self.aws_client.DELAY_TIME = 0

    @patch("boto3.session.Session.client")
    def test_client(self, mock_client):
        service_name = "lambda"
        self.aws_client._client(service_name)
        mock_client.assert_called_once_with(service_name)

    @patch.object(AWSRemoteClient, "_client")
    def test_get_iam_role(self, mock_client):
        role_name = "test_role"
        mock_client.return_value.get_role.return_value = {"Role": {"Arn": "arn:aws:iam::123456789012:role/test_role"}}
        result = self.aws_client.get_iam_role(role_name)
        mock_client.assert_called_once_with("iam")
        mock_client.return_value.get_role.assert_called_once_with(RoleName=role_name)
        self.assertEqual(result, "arn:aws:iam::123456789012:role/test_role")

    @patch.object(AWSRemoteClient, "_client")
    def test_get_lambda_function(self, mock_client):
        function_name = "test_function"
        mock_client.return_value.get_function.return_value = {"Configuration": {"FunctionName": "test_function"}}
        result = self.aws_client.get_lambda_function(function_name)
        mock_client.assert_called_once_with("lambda")
        mock_client.return_value.get_function.assert_called_once_with(FunctionName=function_name)
        self.assertEqual(result, {"FunctionName": "test_function"})

    @patch.object(AWSRemoteClient, "_client")
    def test_create_role(self, mock_client):
        role_name = "test_role"
        policy = "test_policy"
        trust_policy = {"test_key": "test_value"}

        mock_client.return_value.create_role.return_value = {"Role": {"Arn": "test_role_arn"}}

        result = self.aws_client.create_role(role_name, policy, trust_policy)

        mock_client.assert_called_with("iam")
        mock_client.return_value.create_role.assert_called_once_with(
            RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        self.assertEqual(result, "test_role_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_put_role_policy(self, mock_client):
        role_name = "test_role"
        policy_name = "test_policy"
        policy_document = "test_policy_document"

        self.aws_client.put_role_policy(role_name, policy_name, policy_document)

        mock_client.assert_called_with("iam")
        mock_client.return_value.put_role_policy.assert_called_once_with(
            RoleName=role_name, PolicyName=policy_name, PolicyDocument=policy_document
        )

    @patch.object(AWSRemoteClient, "_client")
    def test_update_role_simple(self, mock_client):
        role_name = "test_role"
        policy = "test_policy"
        trust_policy = {"test_key": "test_value"}

        mock_client.return_value.get_role_policy.return_value = {"PolicyDocument": policy}
        mock_client.return_value.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": trust_policy, "Arn": "test_role_arn"}
        }

        result = self.aws_client.update_role(role_name, policy, trust_policy)

        mock_client.assert_called_with("iam")
        mock_client.return_value.get_role_policy.assert_called_once_with(RoleName=role_name, PolicyName=role_name)
        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)
        self.assertEqual(result, "test_role_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_update_role_same_policy(self, mock_client):
        # Scenario 1: The current role policy matches the provided policy
        role_name = "test_role"
        policy = "test_policy"
        trust_policy = {"test_key": "test_value"}

        mock_client.return_value.get_role_policy.return_value = {"PolicyDocument": policy}
        mock_client.return_value.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": trust_policy, "Arn": "test_role_arn"}
        }

        result = self.aws_client.update_role(role_name, policy, trust_policy)

        mock_client.assert_called_with("iam")
        mock_client.return_value.get_role_policy.assert_called_once_with(RoleName=role_name, PolicyName=role_name)
        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)
        self.assertEqual(result, "test_role_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_update_role_different_role_policy(self, mock_client):
        # Scenario 2: The current role policy does not match the provided policy
        role_name = "test_role"
        policy = "new_test_policy"
        trust_policy = {"test_key": "test_value"}

        mock_client.return_value.get_role_policy.return_value = {"PolicyDocument": "old_test_policy"}
        mock_client.return_value.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": trust_policy, "Arn": "test_role_arn"}
        }

        result = self.aws_client.update_role(role_name, policy, trust_policy)

        mock_client.assert_called_with("iam")
        mock_client.return_value.get_role_policy.assert_called_once_with(RoleName=role_name, PolicyName=role_name)
        mock_client.return_value.delete_role_policy.assert_called_once_with(RoleName=role_name, PolicyName=role_name)
        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)
        self.assertEqual(result, "test_role_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_update_role_different_policy(self, mock_client):
        # Scenario 3: The current trust policy does not match the provided trust policy
        role_name = "test_role"
        policy = "test_policy_here"
        trust_policy = {"new_test_key": "new_test_value"}

        mock_client.return_value.get_role_policy.return_value = {"PolicyDocument": policy}
        mock_client.return_value.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": {"old_test_key": "old_test_value"}, "Arn": "test_role_arn"}
        }

        result = self.aws_client.update_role(role_name, policy, trust_policy)

        mock_client.assert_called_with("iam")
        mock_client.return_value.get_role_policy.assert_called_once_with(RoleName=role_name, PolicyName=role_name)
        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)
        mock_client.return_value.delete_role.assert_called_once_with(RoleName=role_name)
        mock_client.return_value.create_role.assert_called_once_with(
            RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        self.assertEqual(result, "test_role_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_update_role_no_role(self, mock_client):
        # Scenario 4: The role does not exist
        role_name = "test_role"
        policy = "test_policy"
        trust_policy = {"test_key": "test_value"}

        mock_client.return_value.get_role_policy.side_effect = ClientError(
            {"Error": {"Code": "NoSuchEntity"}}, "get_role_policy"
        )
        mock_client.return_value.get_role.side_effect = ClientError({"Error": {"Code": "NoSuchEntity"}}, "get_role")
        mock_client.return_value.create_role.return_value = None
        self.aws_client.get_iam_role = MagicMock()
        self.aws_client.get_iam_role.return_value = "test_role_arn"

        result = self.aws_client.update_role(role_name, policy, trust_policy)

        mock_client.assert_called_with("iam")
        mock_client.return_value.get_role_policy.assert_called_once_with(RoleName=role_name, PolicyName=role_name)
        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)
        mock_client.return_value.create_role.assert_called_once_with(
            RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy)
        )
        self.assertEqual(result, "test_role_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_create_sns_topic(self, mock_client):
        topic_name = "test_topic"

        mock_client.return_value.create_topic.return_value = {"TopicArn": "test_topic_arn"}

        result = self.aws_client.create_sns_topic(topic_name)

        mock_client.assert_called_with("sns")
        mock_client.return_value.create_topic.assert_called_once_with(Name=topic_name)
        self.assertEqual(result, "test_topic_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_update_role(self, mock_client):
        role_name = "test_role"
        policy = "test_policy"
        trust_policy = {"test_key": "test_value"}

        # Mock the client's methods to simulate different scenarios
        mock_client.return_value.get_role_policy.side_effect = [
            {"PolicyDocument": policy},  # First call: policy matches
            ClientError({"Error": {"Code": "NoSuchEntity"}}, "get_role_policy"),  # Second call: policy doesn't exist
        ]
        mock_client.return_value.get_role.return_value = {
            "Role": {"AssumeRolePolicyDocument": trust_policy, "Arn": "test_role_arn"}
        }  # Trust policy matches

        result = self.aws_client.update_role(role_name, policy, trust_policy)

        # Check that the client's methods were called with the correct arguments
        mock_client.assert_called_with("iam")
        mock_client.return_value.get_role_policy.assert_has_calls([call(RoleName=role_name, PolicyName=role_name)])
        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)

        self.assertEqual(result, "test_role_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_create_lambda_function(self, mock_client):
        kwargs = {
            "FunctionName": "test_function",
            "Runtime": "python3.8",
            "Role": "test_role",
            "Handler": "test_handler",
        }
        mock_client.return_value.create_function.return_value = {"FunctionArn": "test_function_arn", "State": "Active"}

        result = self.aws_client._create_lambda_function(kwargs)

        mock_client.assert_called_with("lambda")
        mock_client.return_value.create_function.assert_called_once_with(**kwargs)
        self.assertEqual(result, ("test_function_arn", "Active"))

    @patch.object(AWSRemoteClient, "_client")
    @patch("time.sleep", return_value=None)  # To speed up the test
    def test_wait_for_function_to_become_active(self, mock_sleep, mock_client):
        function_name = "test_function"
        mock_client.return_value.get_function.side_effect = [
            {"Configuration": {"State": "Pending"}},  # First call: function is pending
            {"Configuration": {"State": "Active"}},  # Second call: function is active
        ]

        self.aws_client._wait_for_function_to_become_active(function_name)

        mock_client.assert_called_with("lambda")
        mock_client.return_value.get_function.assert_has_calls(
            [call(FunctionName=function_name), call(FunctionName=function_name)]
        )
        mock_sleep.assert_called_once_with(self.aws_client.DELAY_TIME)

    @patch.object(AWSRemoteClient, "_client")
    @patch("time.sleep", return_value=None)  # To speed up the test
    def test_wait_for_function_to_become_active_timeout(self, mock_sleep, mock_client):
        function_name = "test_function"
        mock_client.return_value.get_function.return_value = {
            "Configuration": {"State": "Pending"}
        }  # Function remains pending

        with self.assertRaises(RuntimeError):
            self.aws_client._wait_for_function_to_become_active(function_name)

        mock_client.assert_called_with("lambda")
        self.assertEqual(mock_client.return_value.get_function.call_count, self.aws_client.LAMBDA_CREATE_ATTEMPTS)
        self.assertEqual(mock_sleep.call_count, self.aws_client.LAMBDA_CREATE_ATTEMPTS)

    @patch.object(AWSRemoteClient, "_client")
    def test_subscribe_sns_topic(self, mock_client):
        topic_arn = "test_topic_arn"
        protocol = "https"
        endpoint = "test_endpoint"
        mock_client.return_value.subscribe.return_value = {"SubscriptionArn": "test_subscription_arn"}

        result = self.aws_client.subscribe_sns_topic(topic_arn, protocol, endpoint)

        mock_client.assert_called_with("sns")
        mock_client.return_value.subscribe.assert_called_once_with(
            TopicArn=topic_arn,
            Protocol=protocol,
            Endpoint=endpoint,
            ReturnSubscriptionArn=True,
        )
        self.assertEqual(result, "test_subscription_arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_add_lambda_permission_for_sns_topic(self, mock_client):
        topic_arn = "test_topic_arn"
        lambda_function_arn = "test_lambda_function_arn"

        self.aws_client.add_lambda_permission_for_sns_topic(topic_arn, lambda_function_arn)

        mock_client.assert_called_with("lambda")
        mock_client.return_value.add_permission.assert_called_once_with(
            FunctionName=lambda_function_arn,
            StatementId="sns",
            Action="lambda:InvokeFunction",
            Principal="sns.amazonaws.com",
            SourceArn=topic_arn,
        )

    @patch.object(AWSRemoteClient, "_client")
    def test_send_message_to_messaging_service(self, mock_client):
        identifier = "test_identifier"
        message = "test_message"

        self.aws_client.send_message_to_messaging_service(identifier, message)

        mock_client.assert_called_with("sns")
        mock_client.return_value.publish.assert_called_once_with(TopicArn=identifier, Message=message)

    @patch.object(AWSRemoteClient, "iam_role_exists")
    @patch.object(AWSRemoteClient, "lambda_function_exists")
    def test_resource_exists(self, mock_lambda_function_exists, mock_iam_role_exists):
        resource_iam = Resource(name="test_role", resource_type="iam_role")
        resource_lambda = Resource(name="test_function", resource_type="function")

        self.aws_client.resource_exists(resource_iam)
        mock_iam_role_exists.assert_called_once_with(resource_iam)

        self.aws_client.resource_exists(resource_lambda)
        mock_lambda_function_exists.assert_called_once_with(resource_lambda)

        with self.assertRaises(RuntimeError):
            self.aws_client.resource_exists(Resource(name="test_unknown", resource_type="unknown"))

    @patch.object(AWSRemoteClient, "get_iam_role")
    def test_iam_role_exists(self, mock_get_iam_role):
        resource = Resource(name="test_role", resource_type="iam_role")
        mock_get_iam_role.return_value = None

        self.assertFalse(self.aws_client.iam_role_exists(resource))

        mock_get_iam_role.return_value = "test_role"
        self.assertTrue(self.aws_client.iam_role_exists(resource))

        mock_get_iam_role.side_effect = ClientError({}, "get_iam_role")
        self.assertFalse(self.aws_client.iam_role_exists(resource))

    @patch.object(AWSRemoteClient, "get_lambda_function")
    def test_lambda_function_exists(self, mock_get_lambda_function):
        resource = Resource(name="test_function", resource_type="function")
        mock_get_lambda_function.return_value = None

        self.assertFalse(self.aws_client.lambda_function_exists(resource))

        mock_get_lambda_function.return_value = "test_function"
        self.assertTrue(self.aws_client.lambda_function_exists(resource))

        mock_get_lambda_function.side_effect = ClientError({}, "get_lambda_function")
        self.assertFalse(self.aws_client.lambda_function_exists(resource))

    @patch.object(AWSRemoteClient, "_client")
    def test_get_predecessor_data(self, mock_client):
        current_instance_name = "test_current_instance_name"
        workflow_instance_id = "test_workflow_instance_id"
        mock_client.return_value.get_item.return_value = {"Item": {"message": {"SS": ["test_message"]}}}

        result = self.aws_client.get_predecessor_data(current_instance_name, workflow_instance_id)

        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.get_item.assert_called_once_with(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": f"{current_instance_name}:{workflow_instance_id}"}},
            ReturnConsumedCapacity="TOTAL",
            ConsistentRead=True,
        )
        self.assertEqual(result, (["test_message"], 0.0))

        mock_client.return_value.get_item.return_value = {}
        result = self.aws_client.get_predecessor_data(current_instance_name, workflow_instance_id)
        self.assertEqual(result, ([], 0.0))

        mock_client.return_value.get_item.return_value = {"Item": {}}
        result = self.aws_client.get_predecessor_data(current_instance_name, workflow_instance_id)
        self.assertEqual(result, ([], 0.0))

    @patch.object(AWSRemoteClient, "_client")
    @patch("time.sleep", return_value=None)
    def test_wait_for_role_to_become_active(self, mock_sleep, mock_client):
        role_name = "test_role"
        mock_client.return_value.get_role.return_value = {"Role": {"State": "Inactive"}}

        with self.assertRaises(RuntimeError):
            self.aws_client._wait_for_role_to_become_active(role_name)

        mock_client.assert_called_with("iam")
        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)
        self.assertEqual(mock_client.return_value.get_role.call_count, self.aws_client.LAMBDA_CREATE_ATTEMPTS)
        mock_sleep.assert_called_with(self.aws_client.DELAY_TIME)

        mock_client.return_value.get_role.return_value = {"Role": {"State": "Active"}}

        try:
            self.aws_client._wait_for_role_to_become_active(role_name)
        except RuntimeError:
            self.fail("_wait_for_role_to_become_active raised RuntimeError unexpectedly!")

        mock_client.return_value.get_role.assert_called_with(RoleName=role_name)

    @patch.object(AWSRemoteClient, "_client")
    def test_set_value_in_table(self, mock_client):
        table_name = "test_table"
        key = "test_key"
        value = "test_value"

        # Test without convert_to_bytes
        self.aws_client.set_value_in_table(table_name, key, value)
        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.put_item.assert_called_once_with(
            TableName=table_name, Item={"key": {"S": key}, "value": {"S": value}}
        )

        # Test with convert_to_bytes
        mock_client.reset_mock()
        with patch(
            "caribou.common.models.remote_client.aws_remote_client.compress_json_str", return_value=b"compressed_value"
        ):
            self.aws_client.set_value_in_table(table_name, key, value, convert_to_bytes=True)
            mock_client.assert_called_with("dynamodb")
            mock_client.return_value.put_item.assert_called_once_with(
                TableName=table_name, Item={"key": {"S": key}, "value": {"B": b"compressed_value"}}
            )

    @patch.object(AWSRemoteClient, "_client")
    def test_set_value_in_table_column(self, mock_client):
        table_name = "test_table"
        key = "test_key"
        column_type_value = [("column1", "S", "value1"), ("column2", "N", "123")]
        self.aws_client.set_value_in_table_column(table_name, key, column_type_value)
        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.update_item.assert_called_once()

    @patch.object(AWSRemoteClient, "_client")
    def test_get_value_from_table(self, mock_client):
        table_name = "test_table"
        key = "test_key"

        # Scenario 1: Item exists and is of type byte is False
        mock_client.return_value.get_item.return_value = {
            "Item": {"key": {"S": key}, "value": {"S": "test_value"}},
            "ConsumedCapacity": {"CapacityUnits": 1.0},
        }
        result, consumed_capacity = self.aws_client.get_value_from_table(table_name, key)
        self.assertEqual(result, "test_value")
        self.assertEqual(consumed_capacity, 1.0)

        # Scenario 2: Item exists and is of type byte is True
        mock_client.return_value.get_item.return_value = {
            "Item": {"key": {"S": key}, "value": {"B": b"compressed_value"}},
            "ConsumedCapacity": {"CapacityUnits": 1.0},
        }
        with patch(
            "caribou.common.models.remote_client.aws_remote_client.decompress_json_str",
            return_value="decompressed_value",
        ):
            result, consumed_capacity = self.aws_client.get_value_from_table(table_name, key)
            self.assertEqual(result, "decompressed_value")
            self.assertEqual(consumed_capacity, 1.0)

        # Scenario 3: Item does not exist
        mock_client.return_value.get_item.return_value = {
            "ConsumedCapacity": {"CapacityUnits": 1.0},
        }
        result, consumed_capacity = self.aws_client.get_value_from_table(table_name, key)
        self.assertEqual(result, "")
        self.assertEqual(consumed_capacity, 1.0)

        # Scenario 4: Item exists but no value field
        mock_client.return_value.get_item.return_value = {
            "Item": {"key": {"S": key}},
            "ConsumedCapacity": {"CapacityUnits": 1.0},
        }
        result, consumed_capacity = self.aws_client.get_value_from_table(table_name, key)
        self.assertEqual(result, "")
        self.assertEqual(consumed_capacity, 1.0)

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_value_from_table(self, mock_client):
        table_name = "test_table"
        key = "test_key"
        self.aws_client.remove_value_from_table(table_name, key)
        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.delete_item.assert_called_once_with(TableName=table_name, Key={"key": {"S": key}})

    @patch.object(AWSRemoteClient, "_client")
    def test_get_all_values_from_table(self, mock_client):
        table_name = "test_table"

        # Scenario 1: Items exist and is of type byte is False
        mock_client.return_value.scan.return_value = {
            "Items": [
                {"key": {"S": "key1"}, "value": {"S": "value1"}},
                {"key": {"S": "key2"}, "value": {"S": "value2"}},
            ]
        }
        result = self.aws_client.get_all_values_from_table(table_name)
        self.assertEqual(result, {"key1": "value1", "key2": "value2"})

        # Scenario 2: Items exist and is of type byte is True
        mock_client.return_value.scan.return_value = {
            "Items": [
                {"key": {"S": "key1"}, "value": {"B": b"compressed_value1"}},
                {"key": {"S": "key2"}, "value": {"B": b"compressed_value2"}},
            ]
        }
        with patch(
            "caribou.common.models.remote_client.aws_remote_client.decompress_json_str",
            side_effect=["decompressed_value1", "decompressed_value2"],
        ):
            result = self.aws_client.get_all_values_from_table(table_name)
            self.assertEqual(result, {"key1": "decompressed_value1", "key2": "decompressed_value2"})

        # Scenario 3: No items in response
        mock_client.return_value.scan.return_value = {}
        result = self.aws_client.get_all_values_from_table(table_name)
        self.assertEqual(result, {})

        # Scenario 4: Items key is None
        mock_client.return_value.scan.return_value = {"Items": None}
        result = self.aws_client.get_all_values_from_table(table_name)
        self.assertEqual(result, {})

    @patch.object(AWSRemoteClient, "_client")
    def test_get_key_present_in_table(self, mock_client):
        table_name = "test_table"
        key = "test_key"
        mock_client.return_value.get_item.return_value = {"Item": {"key": {"S": key}, "value": {"S": "test_value"}}}
        result = self.aws_client.get_key_present_in_table(table_name, key)
        self.assertTrue(result)

    @patch.object(AWSRemoteClient, "_client")
    def test_upload_resource(self, mock_client):
        key = "test_key"
        resource = b"test_resource"
        self.aws_client.upload_resource(key, resource)
        mock_client.assert_called_with("s3")

        deployment_resource_bucket: str = os.environ.get(
            "CARIBOU_OVERRIDE_DEPLOYMENT_RESOURCES_BUCKET", DEPLOYMENT_RESOURCES_BUCKET
        )

        mock_client.return_value.put_object.assert_called_once_with(
            Body=resource, Bucket=deployment_resource_bucket, Key=key
        )

    @patch.object(AWSRemoteClient, "_client")
    def test_download_resource(self, mock_client):
        key = "test_key"
        mock_client.return_value.get_object.return_value = {"Body": MagicMock(read=lambda: b"test_resource")}
        result = self.aws_client.download_resource(key)
        self.assertEqual(result, b"test_resource")

    @patch.object(AWSRemoteClient, "_client")
    def test_get_keys(self, mock_client):
        table_name = "test_table"
        mock_client.return_value.scan.return_value = {"Items": [{"key": {"S": "key1"}}, {"key": {"S": "key2"}}]}
        result = self.aws_client.get_keys(table_name)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], "key1")
        self.assertEqual(result[1], "key2")

    @patch.object(AWSRemoteClient, "_client")
    def test_set_predecessor_reached(self, mock_client):
        # Mocking the scenario where the predecessor is set successfully
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the return value of update_item
        update_item_return_value = {
            "Attributes": {"sync_node_name": {"M": {"workflow_instance_id": {"BOOL": True}}}},
            "ConsumedCapacity": {"CapacityUnits": 5.3},
        }
        mock_dynamodb_client.update_item.return_value = update_item_return_value

        result = client.set_predecessor_reached("predecessor_name", "sync_node_name", "workflow_instance_id", True)

        # Check that the return value is correct
        update_item_response_size = len(json.dumps(update_item_return_value).encode("utf-8")) / (1024**3)
        self.assertEqual(result, ([True], update_item_response_size, 5.3 * 2))

    @patch.object(AWSRemoteClient, "_client")
    def test_create_sync_tables(self, mock_client):
        # Mocking the scenario where the tables are created successfully
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of describe_table
        mock_dynamodb_client.describe_table.side_effect = [
            ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "describe_table"),
            ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "describe_table"),
        ]

        client.create_sync_tables()

        # Check that the create_table method was called twice
        self.assertEqual(mock_dynamodb_client.create_table.call_count, 2)

    @patch.object(AWSRemoteClient, "_client")
    @patch("subprocess.run")
    def test_create_function(self, mock_subprocess_run, mock_client):
        # Mocking the scenario where the function is created successfully
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client
        mock_subprocess_run.return_value = MagicMock(check_returncode=lambda: None)

        client = AWSRemoteClient("region1")

        # Mock the return value of _create_lambda_function and _wait_for_function_to_become_active
        client._create_lambda_function = MagicMock(return_value=("arn", "Active"))
        client._wait_for_function_to_become_active = MagicMock()
        client._store_deployed_image_uri = MagicMock()
        client._get_deployed_image_uri = MagicMock(return_value="")

        # Mock the input to create_function
        function_name = "function_name"
        role_identifier = "role_identifier"

        runtime = "python"
        handler = "handler.handler"
        environment_variables = {"key": "value"}
        timeout = 10
        memory_size = 128

        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(tmpdirname + "/app.py", "w") as f:
                f.write("print('hello world')")

            with zipfile.ZipFile(tmpdirname + "/test.zip", "w") as z:
                z.write(tmpdirname + "/app.py")

            with open(tmpdirname + "/test.zip", "rb") as f:
                zip_contents = f.read()
            with patch("tempfile.TemporaryDirectory") as mock_tempdir:
                mock_tempdir.return_value.__enter__.return_value = tmpdirname

                result = client.create_function(
                    function_name,
                    role_identifier,
                    zip_contents,
                    runtime,
                    handler,
                    environment_variables,
                    timeout,
                    memory_size,
                )

        # Check that the return value is correct
        self.assertEqual(result, "arn")

    def test_generate_dockerfile(self):
        client = AWSRemoteClient("region1")

        # Test with python runtime
        result = client._generate_dockerfile("python", "handler.handler", [])
        expected_result = """
        FROM public.ecr.aws/lambda/python:
        COPY requirements.txt ./
        RUN curl -O https://lambda-insights-extension.s3-ap-northeast-1.amazonaws.com/amazon_linux/lambda-insights-extension.rpm && rpm -U lambda-insights-extension.rpm && rm -f lambda-insights-extension.rpm
        
        RUN pip3 install --no-cache-dir -r requirements.txt
        COPY app.py ./
        COPY src ./src
        COPY caribou ./caribou
        COPY generic_handler.py ./
        CMD ["generic_handler.lambda_handler"]
        """
        self.assertEqual(result.strip(), expected_result.strip())

        result = client._generate_dockerfile("python", "handler.handler", ["command1", "command2"])
        expected_result = """
        FROM public.ecr.aws/lambda/python:
        COPY requirements.txt ./
        RUN curl -O https://lambda-insights-extension.s3-ap-northeast-1.amazonaws.com/amazon_linux/lambda-insights-extension.rpm && rpm -U lambda-insights-extension.rpm && rm -f lambda-insights-extension.rpm
        RUN command1 && command2
        RUN pip3 install --no-cache-dir -r requirements.txt
        COPY app.py ./
        COPY src ./src
        COPY caribou ./caribou
        COPY generic_handler.py ./
        CMD ["generic_handler.lambda_handler"]
        """
        self.assertEqual(result.strip(), expected_result.strip())

    @patch("subprocess.run")
    def test_build_docker_image(self, mock_subprocess_run):
        # Mocking the scenario where the Docker image is built successfully
        mock_subprocess_run.return_value = MagicMock(check_returncode=lambda: None)

        client = AWSRemoteClient("region1")

        with tempfile.TemporaryDirectory() as tmpdirname:
            client._build_docker_image(tmpdirname, "image_name")

        # Check that the subprocess.run method was called
        mock_subprocess_run.assert_called()

    def test_store_deployed_image_uri(self):
        # Mocking the scenario where the image URI is stored successfully
        mock_dynamodb_client = MagicMock()
        mock_session = MagicMock()
        mock_session.client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        client._session = mock_session

        # Define the input
        function_name = "image_processing-0_0_1-getinput_aws-us-east-1"
        image_name = "image_processing_light-0_0_1-getinput_aws-us-east-1:latest"

        client._store_deployed_image_uri(function_name, image_name)

        # Check that the client.update_item method was called
        mock_dynamodb_client.update_item.assert_any_call(
            TableName=CARIBOU_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": "image_processing-0_0_1"}},
            UpdateExpression="SET #v = :value",
            ExpressionAttributeNames={"#v": "value"},
            ExpressionAttributeValues={":value": {"S": image_name}},
        )

    @patch.object(AWSRemoteClient, "_client")
    @patch("subprocess.run")
    @patch("subprocess.check_output")
    def test_upload_image_to_ecr(self, mock_check_output, mock_subprocess_run, mock_client):
        # Mocking the scenario where the Docker image is uploaded to ECR successfully
        mock_ecr_client = MagicMock()
        mock_client.return_value = mock_ecr_client
        mock_subprocess_run.return_value = MagicMock(check_returncode=lambda: None)
        mock_check_output.return_value = b"login_password"

        client = AWSRemoteClient("region1")

        # Mock the return value of get_caller_identity and meta
        mock_ecr_client.get_caller_identity.return_value = {"Account": "account_id"}
        mock_ecr_client.meta.region_name = "region"

        client._upload_image_to_ecr("image_name")

        # Check that the subprocess.run and subprocess.check_output methods were called
        mock_subprocess_run.assert_called()
        mock_check_output.assert_called()

    @patch.object(AWSRemoteClient, "_client")
    @patch("subprocess.run")
    @patch("time.sleep")
    def test_update_function(self, mock_sleep, mock_subprocess_run, mock_client):
        # Mocking the scenario where the function is updated successfully
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client
        mock_subprocess_run.return_value = MagicMock(check_returncode=lambda: None)

        client = AWSRemoteClient("region1")

        # Mock the return value of _wait_for_function_to_become_active
        client._wait_for_function_to_become_active = MagicMock()
        client._get_deployed_image_uri = MagicMock(return_value="")
        client._store_deployed_image_uri = MagicMock()

        # Mock the input to update_function
        function_name = "function_name"
        role_identifier = "role_identifier"
        runtime = "python"
        handler = "handler.handler"
        environment_variables = {"key": "value"}
        timeout = 10
        memory_size = 128

        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(tmpdirname + "/app.py", "w") as f:
                f.write("print('hello world')")

            with zipfile.ZipFile(tmpdirname + "/test.zip", "w") as z:
                z.write(tmpdirname + "/app.py")

            with open(tmpdirname + "/test.zip", "rb") as f:
                zip_contents = f.read()
            with patch("tempfile.TemporaryDirectory") as mock_tempdir:
                mock_tempdir.return_value.__enter__.return_value = tmpdirname

                # Mock the return value of update_function_code and update_function_configuration
                mock_lambda_client.update_function_code.return_value = {"State": "Active"}
                mock_lambda_client.update_function_configuration.return_value = {
                    "FunctionArn": "arn",
                    "State": "Active",
                }

                result = client.update_function(
                    function_name,
                    role_identifier,
                    zip_contents,
                    runtime,
                    handler,
                    environment_variables,
                    timeout,
                    memory_size,
                )

        # Check that the return value is correct
        self.assertEqual(result, "arn")

    @patch.object(AWSRemoteClient, "_client")
    def test_get_logs_since_last_sync(self, mock_client):
        # Mocking the scenario where the logs are retrieved successfully
        mock_logs_client = MagicMock()
        mock_client.return_value = mock_logs_client

        client = AWSRemoteClient("region1")

        # Mock the return value of filter_log_events
        mock_logs_client.filter_log_events.return_value = {"events": [{"message": "log_message"}]}

        result = client.get_logs_since("function_instance", datetime.now(GLOBAL_TIME_ZONE))

        # Check that the return value is correct
        self.assertEqual(result, ["log_message"])

    @patch.object(AWSRemoteClient, "_client")
    def test_get_logs_between(self, mock_client):
        # Mocking the scenario where the logs are retrieved successfully
        mock_logs_client = MagicMock()
        mock_client.return_value = mock_logs_client

        client = AWSRemoteClient("region1")

        # Mock the return value of filter_log_events
        mock_logs_client.filter_log_events.return_value = {"events": [{"message": "log_message"}]}

        start_time = datetime.now()
        end_time = start_time + timedelta(hours=1)

        result = client.get_logs_between("function_instance", start_time, end_time)

        # Check that the return value is correct
        self.assertEqual(result, ["log_message"])

        # Check that filter_log_events was called with the correct arguments
        mock_logs_client.filter_log_events.assert_called_with(
            logGroupName="/aws/lambda/function_instance",
            startTime=int(start_time.timestamp() * 1000),
            endTime=int(end_time.timestamp() * 1000),
        )

    @patch.object(AWSRemoteClient, "_client")
    def test_get_insights_logs_between(self, mock_client):
        # Mocking the scenario where the logs are retrieved successfully
        mock_logs_client = MagicMock()
        mock_client.return_value = mock_logs_client

        client = AWSRemoteClient("region1")

        # Mock the return value of filter_log_events
        mock_logs_client.filter_log_events.return_value = {"events": [{"message": "log_message"}]}

        start_time = datetime.now()
        end_time = start_time + timedelta(hours=1)

        result = client.get_insights_logs_between("function_instance", start_time, end_time)

        # Check that the return value is correct
        self.assertEqual(result, ["log_message"])

        # Check that filter_log_events was called with the correct arguments
        mock_logs_client.filter_log_events.assert_called_with(
            logGroupName="/aws/lambda-insights",
            logStreamNamePrefix="function_instance",
            startTime=int(start_time.timestamp() * 1000),
            endTime=int(end_time.timestamp() * 1000),
        )

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_key(self, mock_client):
        # Mocking the scenario where the key is removed successfully
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Call the method with test values
        client.remove_key("table_name", "key")

        # Check that the delete_item method was called
        mock_dynamodb_client.delete_item.assert_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_function(self, mock_client):
        # Mocking the scenario where the function is removed successfully
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client

        client = AWSRemoteClient("region1")

        # Call the method with test values
        client.remove_function("function_name")

        # Check that the delete_function method was called
        mock_lambda_client.delete_function.assert_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_role(self, mock_client):
        # Mocking the scenario where the role is removed successfully
        mock_iam_client = MagicMock()
        mock_client.return_value = mock_iam_client

        client = AWSRemoteClient("region1")

        # Mock the return value of list_attached_role_policies and list_role_policies
        mock_iam_client.list_attached_role_policies.return_value = {"AttachedPolicies": [{"PolicyArn": "arn"}]}
        mock_iam_client.list_role_policies.return_value = {"PolicyNames": ["policy_name"]}

        # Call the method with test values
        client.remove_role("role_name")

        # Check that the detach_role_policy, delete_role_policy, and delete_role methods were called
        mock_iam_client.detach_role_policy.assert_called()
        mock_iam_client.delete_role_policy.assert_called()
        mock_iam_client.delete_role.assert_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_messaging_topic(self, mock_client):
        # Mocking the scenario where the messaging topic is removed successfully
        mock_sns_client = MagicMock()
        mock_client.return_value = mock_sns_client

        client = AWSRemoteClient("region1")

        # Call the method with test values
        client.remove_messaging_topic("topic_identifier")

        # Check that the delete_topic method was called
        mock_sns_client.delete_topic.assert_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_get_topic_identifier(self, mock_client):
        # Mocking the scenario where the topic identifier is retrieved successfully
        mock_sns_client = MagicMock()
        mock_client.return_value = mock_sns_client

        client = AWSRemoteClient("region1")

        # Mock the return value of list_topics
        mock_sns_client.list_topics.return_value = {"Topics": [{"TopicArn": "arn:topic_name"}]}

        result = client.get_topic_identifier("topic_name")

        # Check that the return value is correct
        self.assertEqual(result, "arn:topic_name")

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_resource(self, mock_client):
        # Mocking the scenario where the resource is removed successfully
        mock_s3_client = MagicMock()
        mock_client.return_value = mock_s3_client

        client = AWSRemoteClient("region1")

        # Call the method with test values
        client.remove_resource("key")

        # Check that the delete_object method was called
        mock_s3_client.delete_object.assert_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_ecr_repository(self, mock_client):
        # Mocking the scenario where the ECR repository is removed successfully
        mock_ecr_client = MagicMock()
        mock_client.return_value = mock_ecr_client

        client = AWSRemoteClient("region1")

        # Call the method with test values
        client.remove_ecr_repository("repository_name")

        # Check that the delete_repository method was called
        mock_ecr_client.delete_repository.assert_called()

    @patch("botocore.session.Session")
    def test_get_deployed_image_uri(self, mock_session):
        # Mocking the scenario where the image URI is retrieved successfully
        mock_dynamodb_client = MagicMock()
        mock_session.return_value.create_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Define the input
        function_name = "image_processing-0_0_1-getinput_aws-us-east-1"

        # Mock the return value of get_item
        mock_dynamodb_client.get_item.return_value = {
            "Item": {"key": {"S": "image_processing-0_0_1"}, "value": {"S": "image_uri"}}
        }

        result = client._get_deployed_image_uri(function_name)

        # Check that the return value is correct
        self.assertEqual(result, "image_uri")

    @patch.object(AWSRemoteClient, "_client")
    @patch("subprocess.check_output")
    @patch("subprocess.run")
    def test_copy_image_to_region(self, mock_run, mock_check_output, mock_client):
        # Mocking the scenario where the image is copied successfully
        mock_ecr_client = MagicMock()
        mock_sts_client = MagicMock()
        mock_client.side_effect = [mock_ecr_client, mock_sts_client]

        mock_ecr_client.meta.region_name = "region1"

        client = AWSRemoteClient("region1")

        # Define the input
        deployed_image_uri = "123456789012.dkr.ecr.us-west-2.amazonaws.com/my-web-app:latest"

        # Mock the return value of get_caller_identity
        mock_sts_client.get_caller_identity.return_value = {"Account": "123456789012"}

        # Mock the return value of check_output
        mock_check_output.return_value = b"my_password"

        result = client._copy_image_if_not_exists(deployed_image_uri)

        # Check that the return value is correct
        expected_result = "123456789012.dkr.ecr.region1.amazonaws.com/my-web-app:latest"
        self.assertEqual(result, expected_result)

        # Check that the subprocess.run method was called
        mock_run.assert_called()

    @patch.object(AWSRemoteClient, "_create_framework_lambda_function")
    @patch.object(AWSRemoteClient, "_upload_image_to_ecr")
    @patch.object(AWSRemoteClient, "_build_docker_image")
    @patch.object(AWSRemoteClient, "_generate_framework_dockerfile")
    @patch("builtins.open", new_callable=unittest.mock.mock_open)
    @patch("zipfile.ZipFile")
    @patch("os.path.join", side_effect=lambda *args: "/".join(args))
    def test_deploy_remote_cli(
        self,
        mock_path_join,
        mock_zipfile,
        mock_open,
        mock_generate_dockerfile,
        mock_build_docker_image,
        mock_upload_image_to_ecr,
        mock_create_lambda_function,
    ):
        client = AWSRemoteClient("region1")

        function_name = "test_function"
        handler = "app.handler"
        role_arn = "arn:aws:iam::123456789012:role/test_role"
        timeout = 60
        memory_size = 128
        ephemeral_storage = 512
        zip_contents = b"dummy_zip_content"
        tmpdirname = "/tmp/testdir"
        env_vars = {"ENV_VAR_1": "value1", "ENV_VAR_2": "value2"}

        mock_generate_dockerfile.return_value = "Dockerfile content"
        mock_upload_image_to_ecr.return_value = "image_uri"

        mock_zipfile.return_value.__enter__.return_value.extractall = MagicMock()

        client.deploy_remote_cli(
            function_name,
            handler,
            role_arn,
            timeout,
            memory_size,
            ephemeral_storage,
            zip_contents,
            tmpdirname,
            env_vars,
        )

        # Validate the steps
        mock_open.assert_any_call(os.path.join(tmpdirname, "code.zip"), "wb")
        mock_zipfile.assert_called_once_with(os.path.join(tmpdirname, "code.zip"), "r")
        mock_zipfile.return_value.__enter__.return_value.extractall.assert_called_once_with(tmpdirname)
        mock_generate_dockerfile.assert_called_once_with(handler, env_vars)
        mock_build_docker_image.assert_called_once_with(tmpdirname, f"{function_name.lower()}:latest")
        mock_upload_image_to_ecr.assert_called_once_with(f"{function_name.lower()}:latest")
        mock_create_lambda_function.assert_called_once_with(
            function_name, "image_uri", role_arn, timeout, memory_size, ephemeral_storage
        )

    def test_generate_framework_dockerfile(self):
        client = AWSRemoteClient("region1")

        handler = "app.handler"
        env_vars = {"ENV_VAR_1": "value1", "ENV_VAR_2": "value2"}

        expected_env_statements = 'ENV ENV_VAR_1="value1"\nENV ENV_VAR_2="value2"'

        dockerfile_content = client._generate_framework_dockerfile(handler, env_vars)

        self.assertIn(expected_env_statements, dockerfile_content)
        self.assertIn("FROM python:3.12-slim AS builder", dockerfile_content)
        self.assertIn(f'CMD ["{handler}"]', dockerfile_content)

    @patch.object(AWSRemoteClient, "_create_lambda_function")
    @patch.object(AWSRemoteClient, "_wait_for_function_to_become_active")
    def test_create_framework_lambda_function(
        self, mock_wait_for_function_to_become_active, mock_create_lambda_function
    ):
        client = AWSRemoteClient("region1")

        function_name = "test_function"
        image_uri = "image_uri"
        role_arn = "arn:aws:iam::123456789012:role/test_role"
        timeout = 60
        memory_size = 128
        ephemeral_storage_size = 512

        mock_create_lambda_function.return_value = (
            "arn:aws:lambda:region:123456789012:function:test_function",
            "Active",
        )

        result = client._create_framework_lambda_function(
            function_name, image_uri, role_arn, timeout, memory_size, ephemeral_storage_size
        )

        expected_kwargs = {
            "FunctionName": function_name,
            "Role": role_arn,
            "Code": {"ImageUri": image_uri},
            "PackageType": "Image",
            "Timeout": timeout,
            "MemorySize": memory_size,
            "EphemeralStorage": {"Size": ephemeral_storage_size},
        }

        mock_create_lambda_function.assert_called_once_with(expected_kwargs)
        mock_wait_for_function_to_become_active.assert_not_called()  # Since the state is "Active"
        self.assertEqual(result, "arn:aws:lambda:region:123456789012:function:test_function")

        # Test when the state is not active
        mock_create_lambda_function.return_value = (
            "arn:aws:lambda:region:123456789012:function:test_function",
            "Pending",
        )

        result = client._create_framework_lambda_function(
            function_name, image_uri, role_arn, timeout, memory_size, ephemeral_storage_size
        )

        mock_wait_for_function_to_become_active.assert_called_once_with(function_name)

    @patch.object(AWSRemoteClient, "_client")
    def test_ecr_repository_exists(self, mock_client):
        # Create a mock response for describe_repositories
        mock_client.return_value.describe_repositories.return_value = {
            "repositories": [
                {
                    "repositoryName": "test_repository",
                    "repositoryArn": "arn:aws:ecr:us-west-2:123456789012:repository/test_repository",
                    "registryId": "123456789012",
                    "createdAt": "2022-01-01T00:00:00Z",
                }
            ]
        }

        # Create an instance of AWSRemoteClient
        aws_client = AWSRemoteClient("us-west-2")

        # Call the ecr_repository_exists method
        mock_resource = MagicMock()
        mock_resource.name = "test_repository"
        result = aws_client.ecr_repository_exists(mock_resource)

        # Assert that the method returns True
        self.assertTrue(result)

        # Assert that the describe_repositories method was called with the correct parameters
        mock_client.return_value.describe_repositories.assert_called_once_with(repositoryNames=["test_repository"])

    @patch.object(AWSRemoteClient, "_client")
    def test_get_timer_rule_schedule_expression_exists(self, mock_client):
        # Mocking the scenario where the timer rule exists
        mock_events_client = MagicMock()
        mock_client.return_value = mock_events_client

        client = AWSRemoteClient("region1")

        # Mock the return value of describe_rule
        mock_events_client.describe_rule.return_value = {"ScheduleExpression": "rate(5 minutes)"}

        result = client.get_timer_rule_schedule_expression("test_rule")

        # Check that the return value is correct
        self.assertEqual(result, "rate(5 minutes)")

        # Check that describe_rule was called with the correct arguments
        mock_events_client.describe_rule.assert_called_once_with(Name="test_rule")

    @patch.object(AWSRemoteClient, "_client")
    def test_get_timer_rule_schedule_expression_not_exists(self, mock_client):
        # Mocking the scenario where the timer rule does not exist
        mock_events_client = MagicMock()
        mock_client.return_value = mock_events_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of describe_rule to raise a ResourceNotFoundException
        mock_events_client.describe_rule.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "describe_rule"
        )

        result = client.get_timer_rule_schedule_expression("test_rule")

        # Check that the return value is None
        self.assertIsNone(result)

        # Check that describe_rule was called with the correct arguments
        mock_events_client.describe_rule.assert_called_once_with(Name="test_rule")

    @patch.object(AWSRemoteClient, "_client")
    def test_get_timer_rule_schedule_expression_other_error(self, mock_client):
        # Mocking the scenario where another error occurs
        mock_events_client = MagicMock()
        mock_client.return_value = mock_events_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of describe_rule to raise a different ClientError
        mock_events_client.describe_rule.side_effect = ClientError(
            {"Error": {"Code": "InternalError"}}, "describe_rule"
        )

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        result = client.get_timer_rule_schedule_expression("test_rule")

        # Reset redirect.
        sys.stdout = sys.__stdout__

        # Check that the return value is None
        self.assertIsNone(result)

        # Check that describe_rule was called with the correct arguments
        mock_events_client.describe_rule.assert_called_once_with(Name="test_rule")

        # Check that the error message was logged
        self.assertIn(
            "Error removing the EventBridge rule test_rule: An error occurred (InternalError)",
            captured_output.getvalue(),
        )

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_timer_rule_success(self, mock_client):
        # Mocking the scenario where the timer rule is removed successfully
        mock_events_client = MagicMock()
        mock_client.return_value = mock_events_client

        client = AWSRemoteClient("region1")

        # Call the method with test values
        client.remove_timer_rule("lambda_function_name", "rule_name")

        # Check that the remove_targets and delete_rule methods were called
        mock_events_client.remove_targets.assert_called_once_with(Rule="rule_name", Ids=["lambda_function_name-target"])
        mock_events_client.delete_rule.assert_called_once_with(Name="rule_name", Force=True)

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_timer_rule_not_found(self, mock_client):
        # Mocking the scenario where the timer rule does not exist
        mock_events_client = MagicMock()
        mock_client.return_value = mock_events_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of remove_targets to raise a ResourceNotFoundException
        mock_events_client.remove_targets.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "remove_targets"
        )

        # Call the method with test values
        client.remove_timer_rule("lambda_function_name", "rule_name")

        # Check that the remove_targets method was called
        mock_events_client.remove_targets.assert_called_once_with(Rule="rule_name", Ids=["lambda_function_name-target"])

        # Check that the delete_rule method was not called
        mock_events_client.delete_rule.assert_not_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_remove_timer_rule_other_error(self, mock_client):
        # Mocking the scenario where another error occurs
        mock_events_client = MagicMock()
        mock_client.return_value = mock_events_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of remove_targets to raise a different ClientError
        mock_events_client.remove_targets.side_effect = ClientError(
            {"Error": {"Code": "InternalError"}}, "remove_targets"
        )

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        # Call the method with test values
        client.remove_timer_rule("lambda_function_name", "rule_name")

        # Reset redirect.
        sys.stdout = sys.__stdout__

        # Check that the remove_targets method was called
        mock_events_client.remove_targets.assert_called_once_with(Rule="rule_name", Ids=["lambda_function_name-target"])

        # Check that the delete_rule method was not called
        mock_events_client.delete_rule.assert_not_called()

        # Check that the error message was logged
        self.assertIn(
            "Error removing the EventBridge rule rule_name: An error occurred (InternalError)",
            captured_output.getvalue(),
        )

    @patch.object(AWSRemoteClient, "_client")
    def test_event_bridge_permission_exists_true(self, mock_client):
        # Mocking the scenario where the permission exists
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client

        client = AWSRemoteClient("region1")

        # Mock the return value of get_policy
        mock_lambda_client.get_policy.return_value = {
            "Policy": json.dumps({"Statement": [{"Sid": "existing_statement_id"}]})
        }

        result = client.event_bridge_permission_exists("lambda_function_name", "existing_statement_id")

        # Check that the return value is True
        self.assertTrue(result)

        # Check that get_policy was called with the correct arguments
        mock_lambda_client.get_policy.assert_called_once_with(FunctionName="lambda_function_name")

    @patch.object(AWSRemoteClient, "_client")
    def test_event_bridge_permission_exists_false(self, mock_client):
        # Mocking the scenario where the permission does not exist
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client

        client = AWSRemoteClient("region1")

        # Mock the return value of get_policy
        mock_lambda_client.get_policy.return_value = {
            "Policy": json.dumps({"Statement": [{"Sid": "different_statement_id"}]})
        }

        result = client.event_bridge_permission_exists("lambda_function_name", "non_existing_statement_id")

        # Check that the return value is False
        self.assertFalse(result)

        # Check that get_policy was called with the correct arguments
        mock_lambda_client.get_policy.assert_called_once_with(FunctionName="lambda_function_name")

    @patch.object(AWSRemoteClient, "_client")
    def test_event_bridge_permission_exists_client_error(self, mock_client):
        # Mocking the scenario where a ClientError occurs
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of get_policy to raise a ClientError
        mock_lambda_client.get_policy.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "get_policy")

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        result = client.event_bridge_permission_exists("lambda_function_name", "statement_id")

        # Reset redirect.
        sys.stdout = sys.__stdout__

        # Check that the return value is False
        self.assertFalse(result)

        # Check that get_policy was called with the correct arguments
        mock_lambda_client.get_policy.assert_called_once_with(FunctionName="lambda_function_name")

        # Check that the error message was logged
        self.assertIn(
            "Error in asserting if permission exists lambda_function_name - statement_id", captured_output.getvalue()
        )

    @patch.object(AWSRemoteClient, "_client")
    @patch.object(AWSRemoteClient, "event_bridge_permission_exists")
    @patch.object(AWSRemoteClient, "get_lambda_function")
    def test_create_timer_rule(self, mock_get_lambda_function, mock_event_bridge_permission_exists, mock_client):
        # Mocking the scenario where the timer rule is created successfully
        mock_events_client = MagicMock()
        mock_lambda_client = MagicMock()
        mock_client.side_effect = [mock_events_client, mock_lambda_client]

        client = AWSRemoteClient("region1")

        # Define the input
        lambda_function_name = "test_lambda_function"
        schedule_expression = "rate(5 minutes)"
        rule_name = "test_rule"
        event_payload = '{"key": "value"}'

        # Mock the return value of put_rule
        mock_events_client.put_rule.return_value = {"RuleArn": "arn:aws:events:region:123456789012:rule/test_rule"}

        # Mock the return value of event_bridge_permission_exists
        mock_event_bridge_permission_exists.return_value = False

        # Mock the return value of get_lambda_function
        mock_get_lambda_function.return_value = {
            "FunctionArn": "arn:aws:lambda:region:123456789012:function:test_lambda_function"
        }

        # Call the method with test values
        client.create_timer_rule(lambda_function_name, schedule_expression, rule_name, event_payload)

        # Check that put_rule was called with the correct arguments
        mock_events_client.put_rule.assert_called_once_with(
            Name=rule_name, ScheduleExpression=schedule_expression, State="ENABLED"
        )

        # Check that add_permission was called with the correct arguments
        mock_lambda_client.add_permission.assert_called_once_with(
            FunctionName=lambda_function_name,
            StatementId=f"{rule_name}-invoke-lambda",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn="arn:aws:events:region:123456789012:rule/test_rule",
        )

        # Check that put_targets was called with the correct arguments
        mock_events_client.put_targets.assert_called_once_with(
            Rule=rule_name,
            Targets=[
                {
                    "Id": f"{lambda_function_name}-target",
                    "Arn": "arn:aws:lambda:region:123456789012:function:test_lambda_function",
                    "Input": event_payload,
                }
            ],
        )

    @patch.object(AWSRemoteClient, "_client")
    @patch.object(AWSRemoteClient, "event_bridge_permission_exists")
    @patch.object(AWSRemoteClient, "get_lambda_function")
    def test_create_timer_rule_permission_exists(
        self, mock_get_lambda_function, mock_event_bridge_permission_exists, mock_client
    ):
        # Mocking the scenario where the permission already exists
        mock_events_client = MagicMock()
        mock_lambda_client = MagicMock()
        mock_client.side_effect = [mock_events_client, mock_lambda_client]

        client = AWSRemoteClient("region1")

        # Define the input
        lambda_function_name = "test_lambda_function"
        schedule_expression = "rate(5 minutes)"
        rule_name = "test_rule"
        event_payload = '{"key": "value"}'

        # Mock the return value of put_rule
        mock_events_client.put_rule.return_value = {"RuleArn": "arn:aws:events:region:123456789012:rule/test_rule"}

        # Mock the return value of event_bridge_permission_exists
        mock_event_bridge_permission_exists.return_value = True

        # Mock the return value of get_lambda_function
        mock_get_lambda_function.return_value = {
            "FunctionArn": "arn:aws:lambda:region:123456789012:function:test_lambda_function"
        }

        # Call the method with test values
        client.create_timer_rule(lambda_function_name, schedule_expression, rule_name, event_payload)

        # Check that put_rule was called with the correct arguments
        mock_events_client.put_rule.assert_called_once_with(
            Name=rule_name, ScheduleExpression=schedule_expression, State="ENABLED"
        )

        # Check that add_permission was not called since the permission already exists
        mock_lambda_client.add_permission.assert_not_called()

        # Check that put_targets was called with the correct arguments
        mock_events_client.put_targets.assert_called_once_with(
            Rule=rule_name,
            Targets=[
                {
                    "Id": f"{lambda_function_name}-target",
                    "Arn": "arn:aws:lambda:region:123456789012:function:test_lambda_function",
                    "Input": event_payload,
                }
            ],
        )

    @patch.object(AWSRemoteClient, "_client")
    @patch("json.dumps")
    def test_invoke_remote_framework_with_payload(self, mock_json_dumps, mock_client):
        # Mocking the scenario where the Lambda function is invoked successfully
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client

        client = AWSRemoteClient("region1")

        # Define the input
        payload = {"key": "value"}
        invocation_type = "Event"

        # Mock the return value of json.dumps
        mock_json_dumps.return_value = '{"key": "value"}'

        # Call the method with test values
        client.invoke_remote_framework_with_payload(payload, invocation_type)

        # Check that the _client method was called with the correct arguments
        mock_client.assert_called_once_with("lambda")

        # Check that the invoke method was called with the correct arguments
        mock_lambda_client.invoke.assert_called_once_with(
            FunctionName=REMOTE_CARIBOU_CLI_FUNCTION_NAME,
            InvocationType=invocation_type,
            Payload='{"key": "value"}',
        )

    @patch.object(AWSRemoteClient, "_client")
    @patch("json.dumps")
    def test_invoke_remote_framework_with_payload_default_invocation_type(self, mock_json_dumps, mock_client):
        # Mocking the scenario where the Lambda function is invoked successfully with default invocation type
        mock_lambda_client = MagicMock()
        mock_client.return_value = mock_lambda_client

        client = AWSRemoteClient("region1")

        # Define the input
        payload = {"key": "value"}

        # Mock the return value of json.dumps
        mock_json_dumps.return_value = '{"key": "value"}'

        # Call the method with test values
        client.invoke_remote_framework_with_payload(payload)

        # Check that the _client method was called with the correct arguments
        mock_client.assert_called_once_with("lambda")

        # Check that the invoke method was called with the correct arguments
        mock_lambda_client.invoke.assert_called_once_with(
            FunctionName=REMOTE_CARIBOU_CLI_FUNCTION_NAME,
            InvocationType="Event",
            Payload='{"key": "value"}',
        )

    @patch.object(AWSRemoteClient, "invoke_remote_framework_with_payload")
    def test_invoke_remote_framework_internal_action(self, mock_invoke_remote_framework_with_payload):
        action_type = "test_action_type"
        action_events = {"key": "value"}

        self.aws_client.invoke_remote_framework_internal_action(action_type, action_events)

        expected_payload = {
            "action": "internal_action",
            "type": action_type,
            "event": action_events,
        }

        mock_invoke_remote_framework_with_payload.assert_called_once_with(expected_payload, invocation_type="Event")

    @patch.object(AWSRemoteClient, "_client")
    def test_update_value_in_table(self, mock_client):
        table_name = "test_table"
        key = "test_key"
        value = "test_value"

        # Test without convert_to_bytes
        self.aws_client.update_value_in_table(table_name, key, value)
        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.update_item.assert_called_once_with(
            TableName=table_name,
            Key={"key": {"S": key}},
            UpdateExpression="SET #v = :value",
            ExpressionAttributeNames={"#v": "value"},
            ExpressionAttributeValues={":value": {"S": value}},
        )

        # Test with convert_to_bytes
        mock_client.reset_mock()
        with patch(
            "caribou.common.models.remote_client.aws_remote_client.compress_json_str", return_value=b"compressed_value"
        ):
            self.aws_client.update_value_in_table(table_name, key, value, convert_to_bytes=True)
            mock_client.assert_called_with("dynamodb")
            mock_client.return_value.update_item.assert_called_once_with(
                TableName=table_name,
                Key={"key": {"S": key}},
                UpdateExpression="SET #v = :value",
                ExpressionAttributeNames={"#v": "value"},
                ExpressionAttributeValues={":value": {"B": b"compressed_value"}},
            )

    @patch.object(AWSRemoteClient, "_client")
    def test_create_sync_tables_table_exists(self, mock_client):
        # Mocking the scenario where the tables already exist
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the return value of describe_table to simulate existing tables
        mock_dynamodb_client.describe_table.return_value = {"Table": {"TableStatus": "ACTIVE"}}

        client.create_sync_tables()

        # Check that describe_table was called twice
        self.assertEqual(mock_dynamodb_client.describe_table.call_count, 2)

        # Check that create_table was not called since the tables already exist
        mock_dynamodb_client.create_table.assert_not_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_create_sync_tables_table_not_exists(self, mock_client):
        # Mocking the scenario where the tables do not exist and need to be created
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of describe_table to raise a ResourceNotFoundException
        mock_dynamodb_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "describe_table"
        )

        client.create_sync_tables()

        # Check that describe_table was called twice
        self.assertEqual(mock_dynamodb_client.describe_table.call_count, 2)

        # Check that create_table was called twice since the tables do not exist
        self.assertEqual(mock_dynamodb_client.create_table.call_count, 2)

    @patch.object(AWSRemoteClient, "_client")
    def test_create_sync_tables_other_client_error(self, mock_client):
        # Mocking the scenario where another ClientError occurs
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of describe_table to raise a different ClientError
        mock_dynamodb_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "InternalError"}}, "describe_table"
        )

        with self.assertRaises(ClientError):
            client.create_sync_tables()

        # Check that describe_table was called once
        mock_dynamodb_client.describe_table.assert_called_once()

        # Check that create_table was not called since a different ClientError occurred
        mock_dynamodb_client.create_table.assert_not_called()

    @patch.object(AWSRemoteClient, "_client")
    @patch.object(AWSRemoteClient, "_setup_ttl_for_sync_tables")
    def test_create_sync_tables_setup_ttl_called(self, mock_setup_ttl, mock_client):
        # Mocking the scenario where the tables are created successfully and TTL setup is called
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of describe_table to raise a ResourceNotFoundException
        mock_dynamodb_client.describe_table.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}}, "describe_table"
        )

        client.create_sync_tables()

        # Check that describe_table was called twice
        self.assertEqual(mock_dynamodb_client.describe_table.call_count, 2)

        # Check that create_table was called twice since the tables do not exist
        self.assertEqual(mock_dynamodb_client.create_table.call_count, 2)

        # Check that _setup_ttl_for_sync_tables was called once
        mock_setup_ttl.assert_called_once()

    @patch.object(AWSRemoteClient, "_client")
    def test_setup_ttl_for_sync_tables(self, mock_client):
        # Mocking the scenario where TTL is enabled successfully
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the return value of describe_time_to_live to simulate TTL not enabled
        mock_dynamodb_client.describe_time_to_live.return_value = {
            "TimeToLiveDescription": {"TimeToLiveStatus": "DISABLED"}
        }

        client._setup_ttl_for_sync_tables()

        # Check that get_waiter was called twice
        self.assertEqual(mock_dynamodb_client.get_waiter.call_count, 2)

        # Check that describe_time_to_live was called twice
        self.assertEqual(mock_dynamodb_client.describe_time_to_live.call_count, 2)

        # Check that update_time_to_live was called twice
        self.assertEqual(mock_dynamodb_client.update_time_to_live.call_count, 2)

    @patch.object(AWSRemoteClient, "_client")
    def test_setup_ttl_for_sync_tables_already_enabled(self, mock_client):
        # Mocking the scenario where TTL is already enabled
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the return value of describe_time_to_live to simulate TTL already enabled
        mock_dynamodb_client.describe_time_to_live.return_value = {
            "TimeToLiveDescription": {"TimeToLiveStatus": "ENABLED"}
        }

        client._setup_ttl_for_sync_tables()

        # Check that get_waiter was called twice
        self.assertEqual(mock_dynamodb_client.get_waiter.call_count, 2)

        # Check that describe_time_to_live was called twice
        self.assertEqual(mock_dynamodb_client.describe_time_to_live.call_count, 2)

        # Check that update_time_to_live was not called since TTL is already enabled
        mock_dynamodb_client.update_time_to_live.assert_not_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_setup_ttl_for_sync_tables_waiter_error(self, mock_client):
        # Mocking the scenario where the waiter raises an error
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of get_waiter to raise an exception
        mock_dynamodb_client.get_waiter.side_effect = Exception("Waiter error")

        with self.assertRaises(Exception):
            client._setup_ttl_for_sync_tables()

        # Check that get_waiter was called once before raising the exception
        mock_dynamodb_client.get_waiter.assert_called_once()

        # Check that describe_time_to_live was not called due to the exception
        mock_dynamodb_client.describe_time_to_live.assert_not_called()

        # Check that update_time_to_live was not called due to the exception
        mock_dynamodb_client.update_time_to_live.assert_not_called()

    @patch.object(AWSRemoteClient, "_client")
    def test_setup_ttl_for_sync_tables_describe_error(self, mock_client):
        # Mocking the scenario where describe_time_to_live raises an error
        mock_dynamodb_client = MagicMock()
        mock_client.return_value = mock_dynamodb_client

        client = AWSRemoteClient("region1")

        # Mock the side effect of describe_time_to_live to raise an exception
        mock_dynamodb_client.describe_time_to_live.side_effect = Exception("Describe error")

        with self.assertRaises(Exception):
            client._setup_ttl_for_sync_tables()

        # Check that get_waiter was called twice
        ## Only 1 is called because the first call raises an exception
        self.assertEqual(mock_dynamodb_client.get_waiter.call_count, 1)

        # Check that describe_time_to_live was called once before raising the exception
        mock_dynamodb_client.describe_time_to_live.assert_called_once()

        # Check that update_time_to_live was not called due to the exception
        mock_dynamodb_client.update_time_to_live.assert_not_called()

    @patch.object(AWSRemoteClient, "_client")
    @patch("time.time", return_value=1609459200)  # Mocking time to return a fixed timestamp
    def test_upload_predecessor_data_at_sync_node(self, mock_time, mock_client):
        function_name = "test_function"
        workflow_instance_id = "test_workflow_instance_id"
        message = "test_message"
        expiration_time = 1609459200 + SYNC_TABLE_TTL

        mock_client.return_value.update_item.return_value = {"ConsumedCapacity": {"CapacityUnits": 1.0}}

        result = self.aws_client.upload_predecessor_data_at_sync_node(function_name, workflow_instance_id, message)

        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.update_item.assert_called_once_with(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": f"{function_name}:{workflow_instance_id}"}},
            UpdateExpression="ADD #M :m SET #ttl = :ttl",
            ExpressionAttributeNames={"#M": "message", "#ttl": SYNC_TABLE_TTL_ATTRIBUTE_NAME},
            ExpressionAttributeValues={":m": {"SS": [message]}, ":ttl": {"N": str(expiration_time)}},
            ReturnConsumedCapacity="TOTAL",
        )
        self.assertEqual(result, 1.0)

    @patch.object(AWSRemoteClient, "_client")
    @patch("time.time", return_value=1609459200)  # Mocking time to return a fixed timestamp
    def test_upload_predecessor_data_at_sync_node_no_consumed_capacity(self, mock_time, mock_client):
        function_name = "test_function"
        workflow_instance_id = "test_workflow_instance_id"
        message = "test_message"
        expiration_time = 1609459200 + SYNC_TABLE_TTL

        mock_client.return_value.update_item.return_value = {}

        result = self.aws_client.upload_predecessor_data_at_sync_node(function_name, workflow_instance_id, message)

        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.update_item.assert_called_once_with(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": f"{function_name}:{workflow_instance_id}"}},
            UpdateExpression="ADD #M :m SET #ttl = :ttl",
            ExpressionAttributeNames={"#M": "message", "#ttl": SYNC_TABLE_TTL_ATTRIBUTE_NAME},
            ExpressionAttributeValues={":m": {"SS": [message]}, ":ttl": {"N": str(expiration_time)}},
            ReturnConsumedCapacity="TOTAL",
        )
        self.assertEqual(result, 0.0)


if __name__ == "__main__":
    unittest.main()
