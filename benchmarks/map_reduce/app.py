from typing import Any

import json
import boto3
from tempfile import TemporaryDirectory
import os
from datetime import datetime
import logging
import math
import concurrent.futures

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="map_reduce", version="0.0.4")

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

    run_id = workflow.get_run_id()

    s3 = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:
        local_file_path = f"{tmp_dir}/{input_name}"

        s3.download_file("multi-x-serverless-map-reduce", f"input/{input_name}", local_file_path)

        file_size = os.path.getsize(local_file_path)

        chunk_size = 150 * 1024  # 150 KB

        num_chunks = min(4, math.ceil(file_size / chunk_size))

        payloads = []

        with open(local_file_path, "r") as file:
            for i in range(num_chunks):
                chunk_data = ""
                if i != num_chunks - 1:
                    read_bytes = 0
                    while read_bytes < chunk_size:
                        line = file.readline()
                        if not line:
                            break
                        read_bytes += len(line.encode("utf-8"))
                        chunk_data += line
                else:
                    chunk_data = file.read()  # Read the rest of the file for the last chunk

                # Write the chunk to a temporary file
                chunk_file_path = f"{tmp_dir}/chunk_{i}.txt"
                with open(chunk_file_path, "w") as chunk_file:
                    chunk_file.write(chunk_data)

                remote_chunk_file_path = f"chunks/{input_name}_chunk_{i}_{run_id}.txt"
                payloads.append(
                    {
                        "chunk_file_path": remote_chunk_file_path,
                        "chunk_index": i,
                    }
                )
                # Upload the chunk to S3
                s3.upload_file(chunk_file_path, "multi-x-serverless-map-reduce", remote_chunk_file_path)

        logger.info(f"Payload to mappers: {payloads}, Length: {len(payloads)}")

        pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)

        def worker1():
            workflow.invoke_serverless_function(mapper, payloads[0])

        def worker2():
            payload_one = payloads[1] if len(payloads) >= 2 else None
            workflow.invoke_serverless_function(mapper, payload_one, len(payloads) >= 2)

        def worker3():
            payload_two = payloads[2] if len(payloads) >= 3 else None
            workflow.invoke_serverless_function(mapper, payload_two, len(payloads) >= 3)

        def worker4():
            payload_three = payloads[3] if len(payloads) == 4 else None
            workflow.invoke_serverless_function(mapper, payload_three, len(payloads) == 4)

        pool.submit(worker1)
        pool.submit(worker2)
        pool.submit(worker3)
        pool.submit(worker4)

        pool.shutdown(wait=True)

    return {"status": 200}


@workflow.serverless_function(name="Mapper-Function")
def mapper(event: dict[str, Any]) -> dict[str, Any]:

    chunk_file_path = event["chunk_file_path"]
    chunk_index = event["chunk_index"]
    run_id = workflow.get_run_id()

    s3 = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:

        local_file_path = f"{tmp_dir}/chunk.txt"

        s3.download_file("multi-x-serverless-map-reduce", chunk_file_path, local_file_path)

        with open(local_file_path, "r") as file:
            data = file.read()

        word_counts = {}

        words = data.split()

        for word in words:
            cleaned_word = "".join(char for char in word if char.isalnum()).lower()

            if cleaned_word:
                if cleaned_word in word_counts:
                    word_counts[cleaned_word] += 1
                else:
                    word_counts[cleaned_word] = 1

        word_count_file_path = f"{tmp_dir}/word_count.json"
        with open(word_count_file_path, "w") as file:
            json.dump(word_counts, file)

        remote_word_count_file_path = f"word_counts/word_count_{chunk_index}_{run_id}.json"

        s3.upload_file(word_count_file_path, "multi-x-serverless-map-reduce", remote_word_count_file_path)

        payload = {
            "word_count_file_path": remote_word_count_file_path,
        }

        logger.info(f"Payloads to shuffler: {payload}")

        workflow.invoke_serverless_function(shuffler, payload)

    return {"status": 200}


@workflow.serverless_function(name="Shuffler-Function")
def shuffler(event: dict[str, Any]) -> dict[str, Any]:

    results = workflow.get_predecessor_data()

    num_results = len(results)

    logger.info(f"Results: {results}, Number of results: {num_results}")

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)

    def worker1():
        workflow.invoke_serverless_function(
            reducer,
            {
                "mapper_result1": results[0]["word_count_file_path"],
                "mapper_result2": results[1].get("word_count_file_path", None) if num_results >= 2 else None,
                "reducer_index": 1,
            },
        )

    def worker2():
        if num_results >= 3:
            payload2 = {
                "mapper_result1": results[2]["word_count_file_path"],
                "mapper_result2": results[3].get("word_count_file_path", None) if num_results == 4 else None,
                "reducer_index": 2,
            }
        else:
            payload2 = {"mapper_result1": None, "mapper_result2": None, "reducer_index": 2}

        workflow.invoke_serverless_function(reducer, payload2, num_results >= 3)
    
    pool.submit(worker1)
    pool.submit(worker2)

    pool.shutdown(wait=True)

    return {"status": 200}


@workflow.serverless_function(name="Reducer-Function")
def reducer(event: dict[str, Any]) -> dict[str, Any]:

    word_count_file_path_1 = event["mapper_result1"]
    word_count_file_path_2 = event["mapper_result2"]

    reducer_index = event["reducer_index"]

    merged_word_counts = {}

    s3 = boto3.client("s3")

    with TemporaryDirectory() as tmp_dir:
        if word_count_file_path_1 is not None:
            local_file_path1 = f"{tmp_dir}/word_count1.json"
            s3.download_file("multi-x-serverless-map-reduce", word_count_file_path_1, local_file_path1)
            with open(local_file_path1, "r") as file:
                mapper_result1 = json.load(file)
            for word, count in mapper_result1.items():
                if word in merged_word_counts:
                    merged_word_counts[word] += count
                else:
                    merged_word_counts[word] = count

        if word_count_file_path_2 is not None:
            local_file_path2 = f"{tmp_dir}/word_count2.json"
            s3.download_file("multi-x-serverless-map-reduce", word_count_file_path_2, local_file_path2)
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

        s3.upload_file(sorted_word_count_file_path, "multi-x-serverless-map-reduce", remote_sorted_word_count_file_path)

        payload = {
            "sorted_word_count_file_path": remote_sorted_word_count_file_path,
        }

        logger.info(f"Payloads to output processor: {payload}")

        workflow.invoke_serverless_function(output_processor, payload)

    return {"status": 200}


@workflow.serverless_function(name="Output-Processor")
def output_processor(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data()

    final_word_counts = {}

    s3 = boto3.client("s3")

    with TemporaryDirectory() as tmp_dir:

        for result in results:
            local_file_path = f"{tmp_dir}/sorted_word_count.json"
            s3.download_file("multi-x-serverless-map-reduce", result["sorted_word_count_file_path"], local_file_path)
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

        s3.upload_file(local_file_path, "multi-x-serverless-map-reduce", f"output/{file_name}")

    return {"status": 200}
