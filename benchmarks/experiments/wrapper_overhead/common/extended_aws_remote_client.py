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

from multi_x_serverless.common.models.remote_client.aws_remote_client import AWSRemoteClient

class ExtendedAWSRemoteClient(AWSRemoteClient): 
    def __init__(self, region: str) -> None:
        super().__init__(region)

    def invoke_lambda_function(self, function_name: str, payload: str) -> int:
        client = self._client("lambda")
        response = client.invoke(
            FunctionName=function_name,
            InvocationType="Event",
            Payload=payload
        )
        return response['StatusCode']

    def create_state_machine(self, state_machine_name: str, state_machine_definition: str, policy_arn: str) -> str:
        client = self._client("stepfunctions")
        response = client.create_state_machine(
            name=state_machine_name,
            definition=state_machine_definition,
            roleArn=policy_arn,
            type='STANDARD'  # or 'EXPRESS' https://docs.aws.amazon.com/step-functions/latest/dg/concepts-standard-vs-express.html
        )
        return response["stateMachineArn"]

    def remove_state_machine(self, state_machine_arn: str) -> None:
        client = self._client("stepfunctions")
        client.delete_state_machine(stateMachineArn=state_machine_arn)

    def get_state_machine_arn(self, state_machine_name: str) -> Optional[str]:
        client = self._client("stepfunctions")
        next_token = ""

        while True:
            if next_token:
                response = client.list_state_machines(nextToken=next_token)
            else:
                response = client.list_state_machines()

            for state_machine in response["stateMachines"]:
                # State machine names are unique, so we check for an exact match
                if state_machine_name == state_machine["name"]:
                    return state_machine["stateMachineArn"]

            next_token = response.get("nextToken")
            if not next_token:
                break

        return None
    
    def create_local_function(
        self,
        function_name: str,
        role_identifier: str,
        zip_contents: bytes,
        runtime: str,
        handler: str,
        environment_variables: dict[str, str],
        timeout: int,
        memory_size: int,
        additional_docker_commands: Optional[list[str]] = None,
    ) -> str:
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

    def get_sns_topic_arn(self, topic_name: str) -> Optional[str]:
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

        return None
    