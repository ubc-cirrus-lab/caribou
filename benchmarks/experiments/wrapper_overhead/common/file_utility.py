
from typing import Any

import yaml
import os
import json

class FileUtility():

    def load_json_file(self, file_path: str) -> dict[str, Any]:
        # Check if the file exists
        if os.path.exists(file_path) and os.path.isfile(file_path):
            # Read the file
            with open(file_path, "r") as file:
                return json.load(file)
        else:
            return {}

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
    
