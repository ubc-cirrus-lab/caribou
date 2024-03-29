import os
import re
from typing import Optional
from benchmarks.experiments.wrapper_overhead.common.common_utility import CommonUtility
from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient

class WrapperOverheadCollectionUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
        self._common_utility: CommonUtility = CommonUtility(aws_region)

        self._end_time_log_pattern = re.compile(
            r"\[INFO\]\s+\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\s+"
            r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\s+"
            r"Workload Name: (.*?), "
            r"Request ID: (.*?), "
            r"Client Start Time: (.*?), "
            r"First Function Start Time: (.*?), "
            r"Time Taken from workload invocation from client: (.*?) ms, "
            r"Time Taken from first function: (.*?) ms, "
            r"Function End Time: (.*)"
        )

        self._aws_report_log_pattern = re.compile(
            r"REPORT RequestId: (\S+)\s+"
            r"Duration: ([\d.]+) ms\s+"
            r"Billed Duration: ([\d]+) ms\s+"
            r"Memory Size: (\d+) MB\s+"
            r"Max Memory Used: (\d+) MB"
            r"(?:\s+Init Duration: ([\d.]+) ms)?"
        )

        self._additional_info_log_pattern = re.compile(
            r"\[INFO\]\s+\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\s+"
            r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})\s+"
            r"Workload Name: (.*?), "
            r"Request ID: (.*?), "
            r"CPU Model: (.*)"
        )

    def aquire_experimental_results(self,
                                       experiment_root_directory_paths: list[str],
                                       allowed_experiment_types: Optional[dict[str, str]] = None,
                                       enforce_all_types: bool = True,
                                       from_user_invocation: bool = False
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
                latency_output[experiment_name][allowed_experiment_types[experiment_type_name]] = self._aquire_latency_from_experiment(log_groups, from_user_invocation)

            if enforce_all_types:
                # If we want to enforce all types, then we need to make sure that all types are present
                # Any that are not present will return an list of 1 with 0.0
                for experiment_type_name in allowed_experiment_types.values():
                    if experiment_type_name not in latency_output[experiment_name]:
                        latency_output[experiment_name][experiment_type_name] = [0.0]

        return latency_output
    
        #     def get_special_log_events(self, log_group_name: str, log_pattern: Pattern[str]) -> list[dict[str, Any]]:
        # client = self._client("logs")
        # response = client.filter_log_events(logGroupName=log_group_name)

        # formatted_matches = []
        # for event in response['events']:
        #     message = event['message']
        #     match = log_pattern.search(message)
        #     if match:
        #         parsed_data = match.groups()
        #         formatted_matches.append({
        #             # "Workload Name": parsed_data[0],
        #             "request_id": parsed_data[1],
        #             # "client_start_time": parsed_data[2],
        #             # "first_function_start_time": parsed_data[3],
        #             "time_from_invocation": parsed_data[4],
        #             "time_from_first_function": parsed_data[5],
        #             # "Function End Time": parsed_data[6]
        #         })
        
        # return formatted_matches
    def _aquire_latency_from_experiment(self, log_groups: list[str], from_user_invocation: bool = False) -> list[float]:
        cold_start_runs = set()
        unique_cpu_models = set()

        joined_logs_end_time = []
        joined_logs_aws_report = []
        joined_logs_additional_info = []
        for log_group in log_groups:
            log_response = self._client.get_raw_log_events(log_group)
            end_time_matches = []
            aws_report_matches = []
            additional_info_matches = []

            for event in log_response['events']:
                message = event['message']

                match_end_time_pattern = self._end_time_log_pattern.search(message)
                if match_end_time_pattern:
                    parsed_data = match_end_time_pattern.groups()
                    end_time_matches.append({
                        "run_id": parsed_data[0],  # This is the new addition for the Lambda run ID
                        "request_id": parsed_data[2],  # Adjusted index due to the addition of run_id
                        "time_from_invocation": parsed_data[5],  # Adjusted index
                        "time_from_first_function": parsed_data[6],  # Adjusted index
                    })
                    continue
                    
                match_aws_report_pattern = self._aws_report_log_pattern.search(message)
                if match_aws_report_pattern:
                    request_id, duration, billed_duration, memory_size, max_memory_used, init_duration = match_aws_report_pattern.groups()

                    # Construct the parsed data object
                    parsed_data = {
                        "request_id": request_id,
                        "duration_ms": float(duration),  # Convert to float for consistency
                        "billed_duration_ms": int(billed_duration),  # Convert to integer
                        "memory_size_mb": int(memory_size),  # Convert to integer
                        "max_memory_used_mb": int(max_memory_used),  # Convert to integer
                        "cold_start": init_duration is not None  # Boolean indicating if init_duration was captured
                    }

                    # Optionally add init_duration if present
                    if init_duration is not None:
                        parsed_data["init_duration_ms"] = float(init_duration)
                        cold_start_runs.add(request_id)
                    else:
                        parsed_data["init_duration_ms"] = 0.0

                    aws_report_matches.append(parsed_data)
                    continue

                match_additional_info_pattern = self._additional_info_log_pattern.search(message)
                if match_additional_info_pattern:
                    run_id, workload_name, request_id, cpu_model = match_additional_info_pattern.groups()
                    parsed_data = {
                        "run_id": run_id,  # Now capturing and including the run ID
                        "workload_name": workload_name,
                        "request_id": request_id,
                        "cpu_model": cpu_model
                    }
                    additional_info_matches.append(parsed_data)
                    unique_cpu_models.add(cpu_model)

            joined_logs_end_time.extend(end_time_matches)
            joined_logs_aws_report.extend(aws_report_matches)
            joined_logs_additional_info.extend(additional_info_matches)

        # joined_logs_end_time = [{'run_id': '8ada32d0-14f9-40ae-905f-9b2f00e6208b', 'request_id': 'c836e9dd-1de6-443c-96a3-ba91a5684ee1', 'time_from_invocation': '84182.909', 'time_from_first_function': '75866.376'}]
        # joined_logs_aws_report = [{'request_id': '5a4498aa-6393-4de1-88cd-f07efa0ac9f9', 'duration_ms': 563.55, 'billed_duration_ms': 9690, 'memory_size_mb': 512, 'max_memory_used_mb': 167, 'cold_start': True, 'init_duration_ms': 9125.81}]
        # joined_logs_additional_info = [{'run_id': '5a4498aa-6393-4de1-88cd-f07efa0ac9f9', 'workload_name': 'wo-dna_vis-ed-multi_x-0.0.3', 'request_id': 'c836e9dd-1de6-443c-96a3-ba91a5684ee1', 'cpu_model': 'Intel(R) Xeon(R) Processor @ 2.90GHz'}]
        # cold_start_runs = {'5a4498aa-6393-4de1-88cd-f07efa0ac9f9'}

        # Go through the joined_logs_additional_info, group request_id which have a list of run_id
        grouped_additional_info = {}
        for log in joined_logs_additional_info:
            request_id = log['request_id']
            if request_id not in grouped_additional_info:
                grouped_additional_info[request_id] = set()
            grouped_additional_info[request_id].add(log['run_id'])

        # Now for each of the groupped additional info, check if ANY of its associated run_id is in the cold_start_runs
        # We only want to consider the request_id that are not in the cold_start_runs
        non_cold_start_requests = set() # Contains all request_id that are not cold start
        for request_id, run_ids in grouped_additional_info.items():
            if not run_ids.intersection(cold_start_runs):
                non_cold_start_requests.add(request_id)
        
        # # Lets now short out by matching cpu_model with the request_id
        # # We first want to go through unique_cpu_models and see if we can find a request_id
        # # That solely uses that cpu_model
        # for cpu_model in unique_cpu_models:
        #     request_ids = [log['request_id'] for log in joined_logs_additional_info if log['cpu_model'] == cpu_model]
        #     if len(request_ids) == 1:
        #         non_cold_start_requests.add(request_ids[0])


        # Now we finally have a list of request_id that has not experienced any cold start
        # We can now filter the joined_logs_end_time and joined_logs_aws_report
        joined_logs_end_time = [log for log in joined_logs_end_time if log['request_id'] in non_cold_start_requests]

        # Group all with the same request id
        grouped_requests = {}
        for log in joined_logs_end_time:
            request_id = log['request_id']
            if request_id not in grouped_requests:
                grouped_requests[request_id] = []
            grouped_requests[request_id].append(log)

        time_results = []
        # Now from the grouped request, get the max time from invocation and first function
        for request_id, logs in grouped_requests.items():
            max_time_from_invocation = max([float(log['time_from_invocation']) for log in logs])
            max_time_from_first_function = max([float(log['time_from_first_function']) for log in logs])

            if from_user_invocation:
                time_results.append(max_time_from_invocation)
            else:
                time_results.append(max_time_from_first_function)
    
        return time_results


if __name__ == "__main__":
    desired_region = 'ca-west-1'
    collection_utility = WrapperOverheadCollectionUtility(desired_region)
    current_path = os.getcwd()
    allowed_experiment_types = {
        'boto3_direct': 'Direct Calls',
        'boto3_sns': 'SNS Calls',
        'multi_x': 'EntsGuard',
        'aws_step_function': 'AWS Step Function',
    }

    # Direct calls
    experiment_paths = ['benchmarks/experiments/wrapper_overhead/dna_visualization/external_database']
    full_experiment_paths = []
    for experiment_path in experiment_paths:
        full_path = os.path.join(current_path, experiment_path)
        full_experiment_paths.append(full_path)
        
    experimental_results = collection_utility.aquire_experimental_results(full_experiment_paths, allowed_experiment_types)
    print(experimental_results)