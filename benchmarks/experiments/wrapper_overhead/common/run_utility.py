import json
import os
from typing import Any
from benchmarks.experiments.wrapper_overhead.common.common_utility import CommonUtility
from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient

class WrapperOverheadRunUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
        self._common_utility: CommonUtility = CommonUtility(aws_region)

    def run_experiment(self, directory_path: str, payload: str, times: int) -> bool:
        config = self._common_utility.get_config(directory_path, False)
        if config != {}:
            experiment_type = config['type']
            if experiment_type == 'boto3_direct':
                self._run_lambda_functions(config, payload, times)
            elif experiment_type == 'boto3_sns':
                self._run_sns_topic(config, payload, times)
            elif experiment_type == 'aws_step_function':
                self._run_statemachine(config, payload, times)
            elif experiment_type == 'multi_x':
                self._run_multi_x(config, payload, times)
            else:
                raise ValueError('Invalid experiment type: config.yml misconfigured')
        else:
            raise ValueError('Invalid experiment type: config.yml not found')

    def _run_lambda_functions(self, config: dict[str, Any], payload: str, times: int = 1) -> None:
        print(f"Running {config['type']} workload: {config['workload_name']}")
        print(f"Payload: {payload}")
        print(f"Times: {times}")

        # Get the starting function name
        starting_function_name = config['starting_function_name']

        # Invoke the starting function n times
        for _ in range(times):
            status_code = self._client.invoke_lambda_function(starting_function_name, payload)
            if status_code != 202:
                print(f"Recieved wrong status code {status_code}")
        
        print("Done\n")

    def _run_sns_topic(self, config: dict[str, Any], payload: str, times: int = 1) -> None:
        print(f"Running {config['type']} workload: {config['workload_name']}")
        print(f"Payload: {payload}")
        print(f"Times: {times}")

        # Get the starting function name
        starting_function_name = config['starting_function_name']
        
        # Get the sns topic of this function
        sns_topic_name = f'{starting_function_name}-sns_topic'

        # Get the arn of the sns topic
        sns_topic_arn = self._client.get_sns_topic_arn(sns_topic_name)

        if not sns_topic_arn:
            raise ValueError(f"No sns topic found for {sns_topic_name}")
        
        # Publish the payload to the sns topic n times
        for _ in range(times):
            self._client.send_message_to_messaging_service(sns_topic_arn, payload)

        print("Done\n")

    def _run_statemachine(self, directory_path: str, config: dict[str, Any], payload: str, times: int = 1) -> None:
        # Step 1: Read the config.yaml file
        # To get the state machine name

        # Step 2: Get the arn of the state machine

        # Step 3: For n times, run the state machine with the payload

        pass



    def _run_multi_x(self, config: dict[str, Any], payload: str, times: int = 1) -> None:
        # Step 1: Read the config.yaml file
        # To get the starting lambda function name

        # Step 2: For n times, run the starting lambda functions with the payload

        pass

if __name__ == "__main__":
    desired_region = 'us-east-2'
    run_utility = WrapperOverheadRunUtility(desired_region)
    current_path = os.getcwd()
    payload = {
        "gen_file_name": "small_sequence.gb"
    }
    payload = json.dumps(payload)
    times = 1

    # Direct calls
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/boto3_only_direct_calls'
    full_path = os.path.join(current_path, additional_path)
    # run_utility.run_experiment(full_path, payload, times)

    # SNS calls
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/boto3_only_sns'
    full_path = os.path.join(current_path, additional_path)
    run_utility.run_experiment(full_path, payload, times)


#     # Step Function
#     additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/aws_step_function'
#     full_path = os.path.join(current_path, additional_path)
#     # deployment_utility.deploy_experiment(full_path)

#     # config = common_utility.get_config(full_path) # This is the config file that is read
#     # arns = common_utility.aquire_arns(config)
#     # print(arns)
#     # config["functions"] = {}
#     # print(config)
