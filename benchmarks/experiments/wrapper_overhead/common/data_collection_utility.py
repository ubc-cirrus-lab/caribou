import os
import re
from typing import Optional
from benchmarks.experiments.wrapper_overhead.common.common_utility import CommonUtility
from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient

class WrapperOverheadCollectionUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
        self._common_utility: CommonUtility = CommonUtility(aws_region)

        # Regular expression pattern for parsing the log message
        self._log_pattern = re.compile(
            r"Workload Name: (.*?), "
            r"Request ID: (.*?), "
            r"Client Start Time: (.*?), "
            r"First Function Start Time: (.*?), "
            r"Time Taken from workload invocation from client: (.*?) ms, "
            r"Time Taken from first function: (.*?) ms, "
            r"Function End Time: (.*)"
        )

    def aquire_experimental_results(self,
                                       experiment_root_directory_paths: list[str],
                                       allowed_experiment_types: Optional[dict[str, str]] = None,
                                       enforce_all_types: bool = True,
                                       from_invocation: bool = False
                                       ) -> dict[str, dict[str, list[float]]]:
        
        # Get all log groups, this will be used for accessing the logs
        all_log_groups = self._client.list_all_log_groups()

        latency_output = {}
        for experiment_root_directory_path in experiment_root_directory_paths:
            files_dict, directories_dict = self._common_utility.get_files_and_directories(experiment_root_directory_path)
            experiment_name = self._common_utility.load_yaml_file(files_dict["info.yml"])["experiment_name"]
            latency_output[experiment_name] = {}

            # Go over the directories and get the latency for each experiment type
            for experiment_type_name, experiment_path in directories_dict.items():
                if allowed_experiment_types is not None and experiment_type_name not in allowed_experiment_types:
                    continue

                files_dict, _ = self._common_utility.get_files_and_directories(experiment_path)
                workflow_name = self._common_utility.load_yaml_file(files_dict["config.yml"])["workload_name"]
                workflow_name_altered = workflow_name.replace(".", "_")

                # Now get all the log groups that are related to the workflow
                log_groups = [log_group for log_group in all_log_groups if workflow_name_altered in log_group]
                latency_output[experiment_name][allowed_experiment_types[experiment_type_name]] = self._aquire_latency_from_experiment(log_groups, from_invocation)

            if enforce_all_types:
                # If we want to enforce all types, then we need to make sure that all types are present
                # Any that are not present will return an list of 1 with 0.0
                for experiment_type_name in allowed_experiment_types.values():
                    if experiment_type_name not in latency_output[experiment_name]:
                        latency_output[experiment_name][experiment_type_name] = [0.0]

        return latency_output
    
        
    def _aquire_latency_from_experiment(self, log_groups: list[str], from_invocation: bool = False) -> list[float]:
        joined_logs = []
        for log_group in log_groups:
            logs = self._client.get_special_log_events(log_group, self._log_pattern)
            joined_logs.extend(logs)

        # Group all with the same request id
        grouped_requests = {}
        for log in joined_logs:
            request_id = log['request_id']
            if request_id not in grouped_requests:
                grouped_requests[request_id] = []
            grouped_requests[request_id].append(log)

        time_results = []

        # Now from the grouped request, get the max time from invocation and first function
        for request_id, logs in grouped_requests.items():
            max_time_from_invocation = max([float(log['time_from_invocation']) for log in logs])
            max_time_from_first_function = max([float(log['time_from_first_function']) for log in logs])
            # print(max_time_from_invocation, max_time_from_first_function)

            if from_invocation:
                time_results.append(max_time_from_invocation)
            else:
                time_results.append(max_time_from_first_function)
    
        return time_results


# if __name__ == "__main__":
#     desired_region = 'us-east-2'
#     collection_utility = WrapperOverheadCollectionUtility(desired_region)
#     current_path = os.getcwd()
#     allowed_experiment_types = {
#         'boto3_direct': 'Direct Calls',
#         'boto3_sns': 'SNS Calls',
#         'multi_x': 'EntsGuard',
#         'aws_step_function': 'AWS Step Function',
#     }

#     # Direct calls
#     experiment_paths = ['benchmarks/experiments/wrapper_overhead/dna_visualization/external_database']
#     full_experiment_paths = []
#     for experiment_path in experiment_paths:
#         full_path = os.path.join(current_path, experiment_path)
#         full_experiment_paths.append(full_path)
        
#     experimental_results = collection_utility.aquire_experimental_results(full_experiment_paths, allowed_experiment_types)
#     print(experimental_results)