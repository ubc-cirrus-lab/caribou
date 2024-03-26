import os
from typing import Optional
from benchmarks.experiments.wrapper_overhead.common.common_utility import CommonUtility
from benchmarks.experiments.wrapper_overhead.common.extended_aws_remote_client import ExtendedAWSRemoteClient

class WrapperOverheadCollectionUtility():
    def __init__(self, aws_region: str):
        self._client: ExtendedAWSRemoteClient = ExtendedAWSRemoteClient(aws_region)
        self._common_utility: CommonUtility = CommonUtility(aws_region)

    def aquire_latency_from_experiment(self,
                                       experiment_root_directory_paths: list[str],
                                       allowed_experiment_types: Optional[dict[str, str]] = None
                                       ) -> dict[str, dict[str, list[float]]]:
        
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

                latency_output[experiment_name][allowed_experiment_types[experiment_type_name]] = []
                print(allowed_experiment_types[experiment_type_name])
                
        return latency_output
    
if __name__ == "__main__":
    desired_region = 'us-east-2'
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
        
    collection_utility.aquire_latency_from_experiment(full_experiment_paths, allowed_experiment_types)