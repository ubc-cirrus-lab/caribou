from typing import Any

import json
import boto3
from tempfile import TemporaryDirectory
import os
from datetime import datetime
import logging
import math

from caribou.deployment.client import CaribouWorkflow

# Change the following bucket name and region to match your setup
s3_bucket_name = "caribou-map-reduce"
s3_bucket_region_name = "us-east-1"

workflow = CaribouWorkflow(name="map_reduce", version="0.0.1")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@workflow.serverless_function(
    name="input_processor",
    entry_point=True,
)
def input_processor(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "input_base_dir" in event and "number_shards" in event and "input_file_size" in event:
        input_name = event["input_base_dir"]
        number_shards = event["number_shards"]
        input_file_size = event["input_file_size"]
    else:
        raise ValueError("No message provided")

    min_workload_per_worker = 12.8 * 1024 * 1024  # 12.8 MB

    # If the input file size is less than 6 times the
    # minimum workload per worker, we can process it with less than 6 workers
    if input_file_size < min_workload_per_worker * 6:
        number_of_workers_needed = math.ceil(input_file_size / min_workload_per_worker)
    else:
        number_of_workers_needed = 6

    shards_per_worker = math.ceil(number_shards / number_of_workers_needed)

    payload = {
        "input_base_dir": input_name,
        "number_shards": number_shards,
        "shards_per_worker": shards_per_worker,
    }

    # Mapper 1
    payload["worker_index"] = 0
    workflow.invoke_serverless_function(mapper, payload)

    # Mapper 2
    payload["worker_index"] = 1
    workflow.invoke_serverless_function(mapper, payload, number_of_workers_needed > 1)

    # Mapper 3
    payload["worker_index"] = 2
    workflow.invoke_serverless_function(mapper, payload, number_of_workers_needed > 2)

    # Mapper 4
    payload["worker_index"] = 3
    workflow.invoke_serverless_function(mapper, payload, number_of_workers_needed > 3)

    # Mapper 5
    payload["worker_index"] = 4
    workflow.invoke_serverless_function(mapper, payload, number_of_workers_needed > 4)

    # Mapper 6
    payload["worker_index"] = 5
    workflow.invoke_serverless_function(mapper, payload, number_of_workers_needed > 5)

    return {"status": 200}


@workflow.serverless_function(name="mapper")
def mapper(event: dict[str, Any]) -> dict[str, Any]:

    input_base_dir = event["input_base_dir"]
    number_shards = event["number_shards"]
    shards_per_worker = event["shards_per_worker"]
    worker_index = event["worker_index"]

    run_id = workflow.get_run_id()

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    with TemporaryDirectory() as tmp_dir:

        start_index = worker_index * shards_per_worker
        end_index = min((worker_index + 1) * shards_per_worker, number_shards)

        word_counts = {}
        local_file_path = f"{tmp_dir}/chunk.txt"

        for chunk_index in range(start_index, end_index):
            chunk_file_path = f"input/{input_base_dir}/chunk_{chunk_index}.txt"

            s3.download_file(s3_bucket_name, chunk_file_path, local_file_path)

            with open(local_file_path, "r") as file:
                data = file.read()

            words = data.split()

            for word in words:
                cleaned_word = "".join(char for char in word if char.isalnum()).lower()

                if cleaned_word:
                    if cleaned_word in word_counts:
                        word_counts[cleaned_word] += 1
                    else:
                        word_counts[cleaned_word] = 1

            os.remove(local_file_path)

        word_count_file_path = f"{tmp_dir}/word_count.json"
        with open(word_count_file_path, "w") as file:
            json.dump(word_counts, file)

        remote_word_count_file_path = f"word_counts/word_count_{chunk_index}_{run_id}.json"

        s3.upload_file(word_count_file_path, s3_bucket_name, remote_word_count_file_path)

        payload = {
            "word_count_file_path": remote_word_count_file_path,
        }

        logger.info(f"Payloads to shuffler: {payload}")

        workflow.invoke_serverless_function(shuffler, payload)

    return {"status": 200}


@workflow.serverless_function(name="shuffler")
def shuffler(event: dict[str, Any]) -> dict[str, Any]:

    results = workflow.get_predecessor_data()

    num_results = len(results)

    logger.info(f"Results: {results}, Number of results: {num_results}")

    # Reducer 1
    workflow.invoke_serverless_function(
        reducer,
        {
            "mapper_result1": results[0]["word_count_file_path"],
            "mapper_result2": results[1].get("word_count_file_path", None) if num_results >= 2 else None,
            "reducer_index": 1,
        },
    )

    # Reducer 2
    if num_results >= 3:
        payload2 = {
            "mapper_result1": results[2]["word_count_file_path"],
            "mapper_result2": results[3].get("word_count_file_path", None) if num_results == 4 else None,
            "reducer_index": 2,
        }
    else:
        payload2 = {"mapper_result1": None, "mapper_result2": None, "reducer_index": 2}
    workflow.invoke_serverless_function(reducer, payload2, num_results >= 3)

    return {"status": 200}


@workflow.serverless_function(name="reducer")
def reducer(event: dict[str, Any]) -> dict[str, Any]:

    word_count_file_path_1 = event["mapper_result1"]
    word_count_file_path_2 = event["mapper_result2"]

    reducer_index = event["reducer_index"]

    merged_word_counts = {}

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:
        if word_count_file_path_1 is not None:
            local_file_path1 = f"{tmp_dir}/word_count1.json"
            s3.download_file(s3_bucket_name, word_count_file_path_1, local_file_path1)
            with open(local_file_path1, "r") as file:
                mapper_result1 = json.load(file)
            for word, count in mapper_result1.items():
                if word in merged_word_counts:
                    merged_word_counts[word] += count
                else:
                    merged_word_counts[word] = count

        if word_count_file_path_2 is not None:
            local_file_path2 = f"{tmp_dir}/word_count2.json"
            s3.download_file(s3_bucket_name, word_count_file_path_2, local_file_path2)
            with open(local_file_path2, "r") as file:
                mapper_result2 = json.load(file)
            for word, count in mapper_result2.items():
                if word in merged_word_counts:
                    merged_word_counts[word] += count
                else:
                    merged_word_counts[word] = count

        sorted_word_counts = {word: merged_word_counts[word] for word in sorted(merged_word_counts)}

        sorted_word_count_file_path = f"{tmp_dir}/sorted_word_count.json"

        with open(sorted_word_count_file_path, "w") as file:
            json.dump(sorted_word_counts, file)

        remote_sorted_word_count_file_path = (
            f"sorted_word_counts/sorted_word_count_{reducer_index}_{workflow.get_run_id()}.json"
        )

        s3.upload_file(sorted_word_count_file_path, s3_bucket_name, remote_sorted_word_count_file_path)

        payload = {
            "sorted_word_count_file_path": remote_sorted_word_count_file_path,
        }

        logger.info(f"Payloads to output processor: {payload}")

        workflow.invoke_serverless_function(output_processor, payload)

    return {"status": 200}


@workflow.serverless_function(name="output_processor")
def output_processor(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data()

    final_word_counts = {}

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)

    with TemporaryDirectory() as tmp_dir:

        for result in results:
            local_file_path = f"{tmp_dir}/sorted_word_count.json"
            s3.download_file(s3_bucket_name, result["sorted_word_count_file_path"], local_file_path)
            with open(local_file_path, "r") as file:
                result = json.load(file)

            for word, count in result.items():
                if word in final_word_counts:
                    final_word_counts[word] += count
                else:
                    final_word_counts[word] = count

        local_file_path = f"{tmp_dir}/output.txt"
        file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "output.txt"

        with open(local_file_path, "w") as file:
            for word, count in final_word_counts.items():
                file.write(f"{word}: {count}\n")

        s3.upload_file(local_file_path, s3_bucket_name, f"output/{file_name}")

    return {"status": 200}
