from typing import Any
from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient

class WrapperOverheadRunUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)

    def run_statemachine(self, directory_path: str, payload: dict[str, Any], times: int = 1) -> None:
        # Step 1: Read the config.yaml file
        # To get the state machine name

        # Step 2: Get the arn of the state machine

        # Step 3: For n times, run the state machine with the payload

        pass

    def run_lambda_functions(self, directory_path: str, payload: dict[str, Any], times: int = 1) -> None:
        # Step 1: Read the config.yaml file
        # To get the starting lambda function name

        # Step 2: For n times, run the starting lambda functions with the payload

        pass

    def run_sns_topic(self, directory_path: str, payload: dict[str, Any], times: int = 1) -> None:
        # Step 1: Read the config.yaml file
        # To get the starting sns topic name

        # Step 2: Go through all the folders in the directory_path
        # To get all the arns of the sns topics

        # Step 3: For n times, publish the payload to the starting sns topic
        # Apart of the payload will be the arn of each sns topic

        # Step 4: For n times, publish the payload to the starting sns topic

        pass
