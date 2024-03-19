from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient
from benchmarks.experiments.wrapper_overhead.common.file_utility import FileUtility

class WrapperOverheadDeploymentUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
        self._file_utility: FileUtility = FileUtility()
        self._runtime: str = "python3.8"

    def deploy_experiment(self, directory_path: str) -> bool:
        files_dict, directories_dict = self._file_utility.get_files_and_directories(directory_path)

        if 'config.yml' in files_dict:
            config_content = self._file_utility.load_yaml_file(files_dict['config.yml'])
            experiment_type = config_content['type']
            if experiment_type == 'boto3_direct' or experiment_type == 'boto3_sns':
                return self._deploy_lambda_functions(files_dict, directories_dict, experiment_type == 'boto3_sns')
            elif experiment_type == 'aws_step_function':
                return self._deploy_statemachine(files_dict, directories_dict)
            elif experiment_type == 'multi_x':
                pass # TODO: Alter deployment utility to handle multi_x
            else:
                raise ValueError('Invalid experiment type: config.yml misconfigured')

        else:
            raise ValueError('Invalid experiment type: config.yml not found')

    def _deploy_lambda_functions(self, files_dict: dict[str, str], directories_dict: dict[str, str], require_sns: bool) -> bool:
        # Step 1: Go through all the folders in the directory_path
        # Those are the lambda functions that need to be deployed

        # Step 2: For each folder, access the config to get the configuration
        # (this may also determine if sns is used) and iam policies files

        # Step 3: Check if the lambda function and sns topic exists
        # If it does, delete the old lambda function and sns topic

        # Step 4: Create a zip file of the contents of the code and the requirements
        
        # Step 5: Create the lambda function, with the zip file and the configuration with docker
        
        # Step 6: And sns topic and subscribe the lambda function to the sns topic (if it requires it)

        # Step 7: verify that the lambda function and sns topic are created and correctly subscribed if needed

        pass


    def _deploy_statemachine(self, files_dict: dict[str, str], directories_dict: dict[str, str]) -> bool:
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

        pass

