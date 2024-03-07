import unittest
from unittest.mock import patch
from unittest.mock import MagicMock

from multi_x_serverless.common.models.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.deployment.common.deploy.models.resource import Resource

import json
import zipfile
import tempfile
import datetime

from botocore.exceptions import ClientError
from unittest.mock import call

from multi_x_serverless.common.constants import (
    SYNC_MESSAGES_TABLE,
    MULTI_X_SERVERLESS_WORKFLOW_IMAGES_TABLE,
    GLOBAL_TIME_ZONE,
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
    def test_upload_message_for_sync(self, mock_client):
        function_name = "test_function"
        workflow_instance_id = "test_workflow_instance_id"
        message = "test_message"

        self.aws_client.upload_predecessor_data_at_sync_node(function_name, workflow_instance_id, message)

        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.update_item.assert_called_once_with(
            TableName=SYNC_MESSAGES_TABLE,
            Key={"id": {"S": f"{function_name}:{workflow_instance_id}"}},
            ExpressionAttributeNames={"#M": "message"},
            ExpressionAttributeValues={":m": {"SS": [message]}},
            UpdateExpression="ADD #M :m",
        )

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
        )
        self.assertEqual(result, ["test_message"])

        mock_client.return_value.get_item.return_value = {}
        result = self.aws_client.get_predecessor_data(current_instance_name, workflow_instance_id)
        self.assertEqual(result, [])

        mock_client.return_value.get_item.return_value = {"Item": {}}
        result = self.aws_client.get_predecessor_data(current_instance_name, workflow_instance_id)
        self.assertEqual(result, [])

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
        self.aws_client.set_value_in_table(table_name, key, value)
        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.put_item.assert_called_once_with(
            TableName=table_name, Item={"key": {"S": key}, "value": {"S": value}}
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
        mock_client.return_value.get_item.return_value = {"Item": {"key": {"S": key}, "value": {"S": "test_value"}}}
        result = self.aws_client.get_value_from_table(table_name, key)
        self.assertEqual(result, "test_value")

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
        mock_client.return_value.scan.return_value = {
            "Items": [{"key": {"S": "key1"}, "value": {"S": json.dumps("value1")}}]
        }
        result = self.aws_client.get_all_values_from_table(table_name)
        self.assertEqual(result, {"key1": '"value1"'})

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
        mock_client.return_value.put_object.assert_called_once_with(
            Body=resource, Bucket="multi-x-serverless-resources", Key=key
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
        mock_dynamodb_client.update_item.return_value = {
            "Attributes": {"sync_node_name": {"M": {"workflow_instance_id": {"BOOL": True}}}}
        }

        result = client.set_predecessor_reached("predecessor_name", "sync_node_name", "workflow_instance_id", True)

        # Check that the return value is correct
        self.assertEqual(result, [True])

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
        
        RUN pip3 install --no-cache-dir -r requirements.txt
        COPY app.py ./
        COPY src ./src
        COPY multi_x_serverless ./multi_x_serverless
        CMD ["handler.handler"]
        """
        self.assertEqual(result.strip(), expected_result.strip())

        result = client._generate_dockerfile("python", "handler.handler", ["command1", "command2"])
        expected_result = """
        FROM public.ecr.aws/lambda/python:
        COPY requirements.txt ./
        RUN command1 && command2
        RUN pip3 install --no-cache-dir -r requirements.txt
        COPY app.py ./
        COPY src ./src
        COPY multi_x_serverless ./multi_x_serverless
        CMD ["handler.handler"]
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
            TableName=MULTI_X_SERVERLESS_WORKFLOW_IMAGES_TABLE,
            Key={"key": {"S": "image_processing-0_0_1"}},
            UpdateExpression="SET #v = if_not_exists(#v, :empty_map)",
            ExpressionAttributeNames={"#v": "value"},
            ExpressionAttributeValues={":empty_map": {"M": {}}},
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

        result = client.get_logs_since("function_instance", datetime.datetime.now(GLOBAL_TIME_ZONE))

        # Check that the return value is correct
        self.assertEqual(result, ["log_message"])

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
            "Item": {
                "key": {"S": "image_processing-0_0_1"},
                "value": {"M": {"getinput": {"S": "image_uri"}}},
            }
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

        result = client._copy_image_to_region(deployed_image_uri)

        # Check that the return value is correct
        expected_result = "123456789012.dkr.ecr.region1.amazonaws.com/my-web-app:latest"
        self.assertEqual(result, expected_result)

        # Check that the subprocess.run method was called
        mock_run.assert_called()


if __name__ == "__main__":
    unittest.main()
