import json
import os
import uuid
import datetime
import pytz
from typing import Any
from benchmarks.experiments.wrapper_overhead.common.common_utility import CommonUtility
from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient

class WrapperOverheadRunUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
        self._common_utility: CommonUtility = CommonUtility(aws_region)

    def run_experiment(self, directory_path: str, payload: dict[str, Any], times: int) -> bool:
        config = self._common_utility.get_config(directory_path, False)

        print(f"Running {config['type']} workload: {config['workload_name']}")
        print(f"Payload: {payload}")
        print(f"Times: {times}")

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

    def _run_lambda_functions(self, config: dict[str, Any], payload: dict[str, Any], times: int = 1) -> None:
        # Get the starting function name
        starting_function_name = config['starting_function_name']

        # Invoke the starting function n times
        for _ in range(times):
            # Get the additional metadata
            metadata = self._get_metadata(config)
            payload['metadata'] = metadata

            status_code = self._client.invoke_lambda_function(starting_function_name, json.dumps(payload))
            if status_code != 202:
                print(f"Recieved wrong status code {status_code}")
        
        print("Done\n")

    def _run_sns_topic(self, config: dict[str, Any], payload: dict[str, Any], times: int = 1) -> None:
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
            # Get the additional metadata
            metadata = self._get_metadata(config)
            payload['metadata'] = metadata

            self._client.send_message_to_messaging_service(sns_topic_arn, json.dumps(payload))

        print("Done\n")

    def _run_statemachine(self, config: dict[str, Any], payload: dict[str, Any], times: int = 1) -> None:
        # Get the state machine name
        state_machine_name = config['state_machine_name']

        # Get the arn of the state machine
        state_machine_arn = self._client.get_state_machine_arn(state_machine_name)

        # Step 3: For n times, run the state machine with the payload
        for _ in range(times):
            # Get the additional metadata
            metadata = self._get_metadata(config)
            payload['metadata'] = metadata

            self._client.run_state_machine(state_machine_arn, json.dumps(payload))

    def _run_multi_x(self, config: dict[str, Any], payload: dict[str, Any], times: int = 1) -> None:
        # Get the workflow name
        workload_name = config['workload_name']

        # Step 2: For n times, invoke the workflow with the payload


    def _get_metadata(self, config: dict[str, Any]) -> dict[str, Any]:
        # Parse and append metadata to the payload
        request_id = str(uuid.uuid4())
        current_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
        return {
            "workload_name": config['workload_name'],
            "experiment_type": config['type'], # "boto3_direct", "boto3_sns", "aws_step_function", "multi_x
            "request_id": request_id,
            "start_time": current_time
        }

if __name__ == "__main__":
    desired_region = 'us-east-2'
    run_utility = WrapperOverheadRunUtility(desired_region)
    current_path = os.getcwd()
    payload = {
        "gen_file_name": "small_sequence.gb"
    }
    times = 1

    # Direct calls
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/boto3_only_direct_calls'
    full_path = os.path.join(current_path, additional_path)
    # run_utility.run_experiment(full_path, payload, times)

    # SNS calls
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/boto3_only_sns'
    full_path = os.path.join(current_path, additional_path)
    # run_utility.run_experiment(full_path, payload, times)

    # Step Function
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/aws_step_function'
    full_path = os.path.join(current_path, additional_path)
    # run_utility.run_experiment(full_path, payload, times)

    # Multi X
    additional_path = 'benchmarks/experiments/wrapper_overhead/dna_visualization/external_database/multi_x'
    full_path = os.path.join(current_path, additional_path)
    # run_utility.run_experiment(full_path, payload, times)
    # current_time = datetime.datetime.now(pytz.utc).isoformat()
    # print(current_time)