import unittest
from unittest.mock import patch

from multi_x_serverless.deployment.common.remote_client.aws_remote_client import AWSRemoteClient
from multi_x_serverless.deployment.common.deploy.models.resource import Resource

import json

from botocore.exceptions import ClientError
from unittest.mock import call


class TestAWSRemoteClient(unittest.TestCase):
    @patch("boto3.session.Session")
    def setUp(self, mock_session):
        self.region = "us-west-2"
        self.aws_client = AWSRemoteClient(self.region)
        self.mock_session = mock_session

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
    @patch.object(AWSRemoteClient, "_create_lambda_function")
    @patch.object(AWSRemoteClient, "_wait_for_function_to_become_active")
    def test_create_function(self, mock_wait_for_function, mock_create_lambda, mock_client):
        function_name = "test_function"
        role_arn = "arn:aws:iam::123456789012:role/test_role"
        zip_contents = b"test_zip_contents"
        runtime = "python3.8"
        handler = "test_handler"
        environment_variables = {"test_key": "test_value"}
        timeout = 10
        memory_size = 128
        mock_create_lambda.return_value = ("arn:aws:lambda:us-west-2:123456789012:function:test_function", "Inactive")
        result = self.aws_client.create_function(
            function_name, role_arn, zip_contents, runtime, handler, environment_variables, timeout, memory_size
        )
        mock_create_lambda.assert_called_once()
        mock_wait_for_function.assert_called_once_with(function_name)
        self.assertEqual(result, "arn:aws:lambda:us-west-2:123456789012:function:test_function")

    @patch.object(AWSRemoteClient, "_client")
    def test_update_function(self, mock_client):
        function_name = "test_function"
        role_arn = "arn:aws:iam::123456789012:role/test_role"
        zip_contents = b"test_zip_contents"
        runtime = "python3.8"
        handler = "test_handler"
        environment_variables = {"test_key": "test_value"}
        timeout = 10
        memory_size = 128

        mock_client.return_value.update_function_code.return_value = {"State": "Active"}
        mock_client.return_value.update_function_configuration.return_value = {
            "State": "Active",
            "FunctionArn": "test_function_arn",
        }

        result = self.aws_client.update_function(
            function_name, role_arn, zip_contents, runtime, handler, environment_variables, timeout, memory_size
        )

        mock_client.assert_called_with("lambda")
        mock_client.return_value.update_function_code.assert_called_once_with(
            FunctionName=function_name, ZipFile=zip_contents
        )
        mock_client.return_value.update_function_configuration.assert_called_once()
        self.assertEqual(result, "test_function_arn")

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
    def test_update_role(self, mock_client):
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
    def test_increment_counter(self, mock_client):
        function_name = "test_function"
        workflow_instance_id = "test_workflow_instance_id"
        mock_client.return_value.update_item.return_value = {"Attributes": {"counter_value": {"N": "1"}}}

        result = self.aws_client.increment_counter(function_name, workflow_instance_id)

        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.update_item.assert_called_once_with(
            TableName="counters",
            Key={"id": {"S": f"{function_name}:{workflow_instance_id}"}},
            UpdateExpression="SET counter_value = counter_value + :val",
            ExpressionAttributeValues={":val": 1},
            ReturnValues="UPDATED_NEW",
        )
        self.assertEqual(result, 1)

    @patch.object(AWSRemoteClient, "_client")
    def test_upload_message_for_merge(self, mock_client):
        function_name = "test_function"
        workflow_instance_id = "test_workflow_instance_id"
        message = "test_message"

        self.aws_client.upload_message_for_merge(function_name, workflow_instance_id, message)

        mock_client.assert_called_with("dynamodb")
        mock_client.return_value.update_item.assert_called_once_with(
            TableName="merge_messages",
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
            TableName="merge_messages",
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


if __name__ == "__main__":
    unittest.main()