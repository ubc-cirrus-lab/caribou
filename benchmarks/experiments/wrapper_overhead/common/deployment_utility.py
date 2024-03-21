import os
from typing import Any
from unittest.mock import MagicMock
from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient
from benchmarks.experiments.wrapper_overhead.common.common_utility import CommonUtility
from multi_x_serverless.deployment.common.deploy.deployment_packager import DeploymentPackager
from multi_x_serverless.deployment.common.deploy.models.resource import Resource
from multi_x_serverless.deployment.common.config.config import Config

class WrapperOverheadDeploymentUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
        self._common_utility: CommonUtility = CommonUtility(aws_region)
        self._deployment_packager_config: MagicMock = MagicMock(spec=Config)
        self._deployment_packager_config.workflow_version = "1.0.0"
        self._deployment_packager: DeploymentPackager = DeploymentPackager(self._deployment_packager_config)
        self._runtime: str = "python3.8"
        self._lambda_trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "",
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

    def deploy_experiment(self, directory_path: str) -> bool:
        config = self._common_utility.get_config(directory_path)
        if config != {}:
            experiment_type = config['type']
            if experiment_type == 'boto3_direct' or experiment_type == 'boto3_sns':
                return self._deploy_lambda_functions(config, experiment_type == 'boto3_sns')
            elif experiment_type == 'aws_step_function':
                return self._deploy_statemachine(config)
            elif experiment_type == 'multi_x':
                pass # We do not handle launching multi_x, follow the instructions in the README
            else:
                raise ValueError('Invalid experiment type: config.yml misconfigured')
        else:
            raise ValueError('Invalid experiment type: config.yml not found')

    def _deploy_lambda_functions(self, config: dict[str, Any], has_sns: bool) -> None:
        print(f"Deploying {config['type']} workload")
        print(f"{config['workload_name']}")
        for function in config['functions'].values():
            # Load Name information
            function_name = function['function_name']
            iam_policy_name = function['iam_policy_name']
            sns_topic_name = function['sns_topic_name']

            print(f"\nDeploying {function_name}")

            # First, delete the old functions and all associated roles and sns if found
            print(f"Removing old resources")
            if self._client.resource_exists(Resource(iam_policy_name, "iam_role")): # For iam role
                self._client.remove_role(iam_policy_name)
            if self._client.resource_exists(Resource(function_name, "function")): # For lambda function
                self._client.remove_function(function_name)
            if has_sns: # For sns topic
                sns_topic_arn = self._client.get_sns_topic_arn(sns_topic_name)
                if sns_topic_arn:
                    self._client.remove_messaging_topic(sns_topic_arn)
            print(f"Old resources for Removed")

            # Load function information
            timeout = function['timeout']
            memory = function['memory']
            handler = function['handler']
            data_directory_path = function['data_directory_path']
            iam_policies_content = function['iam_policies_content']
            additional_docker_commands = function['additional_docker_commands']

            # Create iam role
            print(f"Creating iam role")
            policy_arn = self._client.create_role(iam_policy_name, iam_policies_content, self._lambda_trust_policy)
            self._deployment_packager_config.workflow_name = function_name
            print(f"Resulting Policy ARN: {policy_arn}")

            # Create lambda function
            ## First zip the code content
            zip_path = self._deployment_packager._create_deployment_package(data_directory_path, self._runtime)
            with open(zip_path, 'rb') as f:
                zip_contents = f.read()

            ## Then create the lambda function
            print(f"Creating lambda function")
            function_arn = self._client.create_local_function(
                function_name = function_name,
                role_identifier = policy_arn,
                zip_contents = zip_contents,
                runtime = self._runtime,
                handler =  handler,
                environment_variables = {},
                timeout = timeout,
                memory_size = memory,
                additional_docker_commands = additional_docker_commands
            )
            print(f"Resulting Function ARN: {function_arn}")

            # Create sns topic and subscribe the lambda function to the sns topic
            # If it requires it
            if has_sns:
                print(f"Creating sns topic and subscribing lambda function")
                protocol = "lambda"
                sns_topic_arn = self._client.create_sns_topic(sns_topic_name)
                print(f"Resulting Topic ARN: {sns_topic_arn}")
                sns_subscription_arn = self._client.subscribe_sns_topic(sns_topic_arn, protocol, function_arn)
                print(f"Resulting Subscription ARN: {sns_subscription_arn}")
        
        print(f"Completed deployment of {config['workload_name']}\n\n")

        return True

    def _deploy_statemachine(self, config: dict[str, Any]) -> None:
        # Use the _deploy_lambda_functions to deploy all aws functions and rols
        self._deploy_lambda_functions(config, False)

        # Now we need to aquire the arn of the lambda functions
        arns = self._common_utility.aquire_arns(config)

        # Now lets load the state_machine.json and config.yaml
        print(arns)



        # Step 1: Go through all the folders in the directory_path
        # Those are the lambda functions that need to be deployed

        # Step 2: For each folder, access the config to get the configuration
        # and iam policies files

        # Step 3: Check if the lambda function exists
        # If it does, delete the old lambda function

        # Step 4: Create a zip file of the contents of the code and the requirements
        
        # Step 5: Create the lambda function, with the zip file and the configuration with docker

        # Step 6: verify that the lambda function created

        # Step 7: Get the arn of the lambda functions

        # Step 8: Read the config.yaml and state_machine.json files
        # To get the needed information to create the state machine

        # Step 9: Delete the old state machine if it exists

        # Step 10: Load the config.yaml and replace the resources with the arn of the lambda functions

        # Step 11: Create the state machine with the arn of the lambda functions

        # Step 12: Verify that the state machine is created and correctly configured

        return True

if __name__ == "__main__":
    desired_region = 'us-east-2'
    deployment_utility = WrapperOverheadDeploymentUtility(desired_region)
    common_utility = CommonUtility(desired_region)
    current_path = os.getcwd()

    # Direct calls
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/boto3_only_direct_calls'
    full_path = os.path.join(current_path, additional_path)
    # deployment_utility.deploy_experiment(full_path)


    # SNS calls
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/boto3_only_sns'
    full_path = os.path.join(current_path, additional_path)
    # deployment_utility.deploy_experiment(full_path)


    # Step Function
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/aws_step_function'
    full_path = os.path.join(current_path, additional_path)
    deployment_utility.deploy_experiment(full_path)
