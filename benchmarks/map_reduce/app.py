from typing import Any

import json
import boto3
import tempfile
import os
from datetime import datetime 
import logging
import math

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="map_reduce", version="0.0.1")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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

    s3.download_file("multi-x-serverless-map-reduce", f"input/{input_name}", local_file_path)

    file_size = os.path.getsize(local_file_path)

    chunk_size = 150 * 1024 # 150 KB

    num_chunks = min(4, math.ceil(file_size / chunk_size))

    payloads = []

    with open(local_file_path, 'r') as file:
        for i in range(num_chunks):
            chunk_data = ""
            if i != num_chunks - 1:  
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


    logger.info(f"Payload to mappers: {payloads} \n Length: {len(payloads)}")

    workflow.invoke_serverless_function(mapper, payloads[0])

    payload_one = payloads[1] if len(payloads) >= 2 else None
    workflow.invoke_serverless_function(mapper, payload_one, len(payloads) >= 2)

    payload_two = payloads[2] if len(payloads) >= 3 else None
    workflow.invoke_serverless_function(mapper, payload_two, len(payloads) >= 3)

    payload_three = payloads[3] if len(payloads) == 4 else None
    workflow.invoke_serverless_function(mapper, payload_three, len(payloads) == 4)

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

    logger.info(f"Payloads to shuffler: {payload}")

    workflow.invoke_serverless_function(shuffler, payload)

    return {"status": 200}


@workflow.serverless_function(name="Shuffler-Function")
def shuffler(event: dict[str, Any]) -> dict[str, Any]:
    
    results = workflow.get_predecessor_data()

    num_results = len(results)

    logger.info(f"Results: {results} \n Number of results: {num_results}")

    workflow.invoke_serverless_function(reducer, {
        "mapper_result1": results[0]["word_counts"],
        "mapper_result2": results[1].get("word_counts", {}) if num_results >= 2 else {}
    })

    workflow.invoke_serverless_function(reducer, {
        "mapper_result1": results[2].get("word_counts", {}) if num_results >= 3 else {},
        "mapper_result2": results[3].get("word_counts", {}) if num_results == 4 else {}
    }, num_results >= 3)

    return {"status": 200}


@workflow.serverless_function(name="Reducer-Function")
def reducer(event: dict[str, Any]) -> dict[str, Any]:
    
    mapper_result1 = event.get("mapper_result1")
    mapper_result2 = event.get("mapper_result2")

    merged_word_counts = {}

    if mapper_result1 is not None:
        for word, count in mapper_result1.items():
            if word in merged_word_counts:
                merged_word_counts[word] += count
            else:
                merged_word_counts[word] = count
    
    if mapper_result2 is not None:
        for word, count in mapper_result2.items():
            if word in merged_word_counts:
                merged_word_counts[word] += count
            else:
                merged_word_counts[word] = count

    sorted_word_counts = {word: merged_word_counts[word] for word in sorted(merged_word_counts)}

    payload = {
        "sorted_word_counts": sorted_word_counts,
    }

    logger.info(f"Payloads to output processor: {payload}")

    workflow.invoke_serverless_function(output_processor, payload)

    return {"status": 200}


@workflow.serverless_function(name="Output-Processor")
def output_processor(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data()

    final_word_counts = {}

    for result in results:
        for word, count in result["sorted_word_counts"].items():
            if word in final_word_counts:
                final_word_counts[word] += count
            else:
                final_word_counts[word] = count

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()
    local_file_path = f"{tmp_dir}/output.txt"
    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "output.txt"

    with open(local_file_path, 'w') as file:
        for word, count in final_word_counts.items():
            file.write(f"{word}: {count}\n")

    s3.upload_file(local_file_path, "multi-x-serverless-map-reduce", f"output/{file_name}")

    return {"status": 200}
