
from typing import Any, Union

import yaml
import os
import json

class FileUtility():
    def get_config(self, directory_path: str) -> dict[str, Any]:
        loadded_config = {}

        files_dict, directories_dict = self.get_files_and_directories(directory_path)
        # For the files list, find the files that end with config.yaml
        # If the file is found, then read the file and print the content

        if 'config.yml' in files_dict:
            config_content = self.load_yaml_file(files_dict['config.yml'])
            loadded_config.update(config_content)
            
            functions = {}
            # Go get information on every function
            for folder_path in directories_dict.values():
                _, inner_directories_dict = self.get_files_and_directories(folder_path)

                # Step 2: For each folder, access the config to get the configuration
                # (this may also determine if sns is used) and iam policies files
                config_content = {}
                iam_policies_content = {}
                if 'configs' in inner_directories_dict:
                    config_files_dict, _ = self.get_files_and_directories(inner_directories_dict['configs'])

                    # Load the config file
                    if 'config.yml' in config_files_dict:
                        config_content = self.load_yaml_file(config_files_dict['config.yml'])
                        # print(config_content)
                    else:
                        raise ValueError('config.yml not found in the configs directory')
                    
                    # Load the iam policies file
                    if 'iam_policy.json' in config_files_dict:
                        iam_policies_content = self.load_json_file(config_files_dict['iam_policy.json'], load_as_dict=False)
                        # print(iam_policies_content)
                    else:
                        raise ValueError('iam_policy.json not found in the configs directory')
                else:
                    raise ValueError('configs directory not found')
                
                # Now aquire all the possible paths for the function
                function_name = config_content['function_name']
                iam_policy_name = f'{function_name}-policy'
                sns_topic_name = f'{function_name}-sns_topic'
                timeout = config_content['configs']['timeout']
                memory = config_content['configs']['memory']
                handler = config_content['handler']
                sns_topic_name = f'{function_name}-sns_topic'
                directory_path = folder_path
                additional_docker_commands = config_content.get('additional_docker_commands', None)

                # Now save the information per function
                functions[function_name] = {
                    'function_name': function_name,
                    'iam_policy_name': iam_policy_name,
                    'sns_topic_name': sns_topic_name,
                    'timeout': timeout,
                    'memory': memory,
                    'handler': handler,
                    'data_directory_path': directory_path,
                    'iam_policies_content': iam_policies_content,
                    'additional_docker_commands': additional_docker_commands
                }

            loadded_config['functions'] = functions

        return loadded_config

    def load_json_file(self, file_path: str, load_as_dict: bool = True) -> Union[dict[str, Any], str]:
        # Check if the file exists
        if os.path.exists(file_path) and os.path.isfile(file_path):
            # Read the file
            with open(file_path, "r") as file:
                if load_as_dict:
                    return json.load(file)
                else:
                    return file.read()
        else:
            if load_as_dict:
                return {}
            else:
                return ""

    def load_yaml_file(self, file_path: str) -> dict[str, Any]:
        # Check if the file exists
        if os.path.exists(file_path) and os.path.isfile(file_path):
            # Read the file
            with open(file_path, "r") as file:
                return yaml.safe_load(file)
        else:
            return {}
        
    def get_files_and_directories(self, directory_path: str) -> tuple[dict[str, str], dict[str, str]]:
        # Check if the path exists and is a directory
        if os.path.exists(directory_path) and os.path.isdir(directory_path):
            # Initialize lists to store files and directories
            files_dict = {}
            directories_dict = {}
            
            # List all files and directories in the given path
            files_and_directories = os.listdir(directory_path)

            # Separate files and directories
            for item in files_and_directories:
                item_path = os.path.join(directory_path, item)
                if os.path.isfile(item_path):
                    files_dict[item] = item_path
                elif os.path.isdir(item_path):
                    directories_dict[item] = item_path

            return files_dict, directories_dict
        else:
            return {}, {}
    
