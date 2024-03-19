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

    def create_function_image(
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
