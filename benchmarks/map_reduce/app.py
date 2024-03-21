from typing import Any

import json
import boto3
import tempfile
import os
import datetime

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="map_reduce", version="0.0.1")

@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "message" in event:
        message = event["message"]
    else:
        raise ValueError("No message provided")

    payload = {
        "input_name": message,
    }

    workflow.invoke_serverless_function(input_processor, payload)

    return {"status": 200}

@workflow.serverless_function(
    name="Input-Processor",
    entry_point=False,
)
def input_processor(event: dict[str, Any]) -> dict[str, Any]:
    # get data from s3
    input_name = event["input_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()
    local_file_path = f"{tmp_dir}/{input_name}"

    s3.download_file("multi-x-serverless-map-reduce/input", input_name, local_file_path)

    file_size = os.path.getsize(local_file_path)
    chunk_size = file_size // 4

    payloads = []  

    with open(local_file_path, 'r') as file:
        for i in range(4):
            chunk_data = ""
            if i != 3:  
                read_bytes = 0
                while read_bytes < chunk_size:
                    line = file.readline()
                    if not line:
                        break  
                    read_bytes += len(line.encode('utf-8')) 
                    chunk_data += line
            else:
                chunk_data = file.read()  # Read the rest of the file for the last chunk

            payloads.append({"data": chunk_data})

    workflow.invoke_serverless_function(mapper, payloads[0])
    workflow.invoke_serverless_function(mapper, payloads[1])
    workflow.invoke_serverless_function(mapper, payloads[2])
    workflow.invoke_serverless_function(mapper, payloads[3])


    return {"status": 200}


@workflow.serverless_function(name="Mapper-Function")
def mapper(event: dict[str, Any]) -> dict[str, Any]:

    data = event["data"]
    
    word_counts = {}

    words = data.split()

    for word in words:
        cleaned_word = ''.join(char for char in word if char.isalnum()).lower()

        if cleaned_word:
            if cleaned_word in word_counts:
                word_counts[cleaned_word] += 1
            else:
                word_counts[cleaned_word] = 1


    payload = {
        "word_counts": word_counts,
    }

    workflow.invoke_serverless_function(shuffler, payload)

    return {"status": 200}


@workflow.serverless_function(name="Shuffler-Function")
def shuffler(event: dict[str, Any]) -> dict[str, Any]:
    
    results = workflow.get_predecessor_data()

    reducer_payloads = [{
        "mapper_result1": results[0]["word_counts"],
        "mapper_result2": results[1]["word_counts"]
    }, {
        "mapper_result1": results[2]["word_counts"],
        "mapper_result2": results[3]["word_counts"]
    }]

    workflow.invoke_serverless_function(reducer, reducer_payloads[0])
    workflow.invoke_serverless_function(reducer, reducer_payloads[1])

    return {"status": 200}


@workflow.serverless_function(name="Reducer-Function")
def reducer(event: dict[str, Any]) -> dict[str, Any]:
    
    mapper_result1 = event["mapper_result1"]
    mapper_result2 = event["mapper_result2"]

    merged_word_counts = {}

    for word, count in mapper_result1.items():
        if word in merged_word_counts:
            merged_word_counts[word] += count
        else:
            merged_word_counts[word] = count
    
    for word, count in mapper_result2.items():
        if word in merged_word_counts:
            merged_word_counts[word] += count
        else:
            merged_word_counts[word] = count

    sorted_word_counts = sorted(merged_word_counts.keys())

    payload = {
        "sorted_word_counts": sorted_word_counts,
    }

    workflow.invoke_serverless_function(output_processor, payload)

    return {"status": 200}


@workflow.serverless_function(name="Output-Processor")
def output_processor(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data()

    final_word_counts = {}

    for result in results:
        for word in result["sorted_word_counts"]:
            if word in final_word_counts:
                final_word_counts[word] += 1
            else:
                final_word_counts[word] = 1

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()
    local_file_path = f"{tmp_dir}/output.txt"
    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "output.txt"

    with open(local_file_path, 'w') as file:
        for word, count in final_word_counts.items():
            file.write(f"{word}: {count}\n")

    s3.upload_file(local_file_path, "multi-x-serverless-map-reduce/results", f"results{file_name}")

    return {"status": 200}
