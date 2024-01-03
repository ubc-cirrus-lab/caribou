from typing import Any
import os

from multi_x_serverless.deployment.client import MultiXServerlessWorkflow
import json
from gtts import gTTS
from io import BytesIO
import boto3
from datetime import datetime
from profanity import profanity
from pydub import AudioSegment
import numpy as np

import tempfile

workflow = MultiXServerlessWorkflow("text_2_speech_censoring")


@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
    providers=[
        {
            "name": "aws",
            "configuration": {
                "timeout": 90,
                "memory": 256,
            }
        }
    ]
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    request = event["request"]
    request_json = json.loads(request)

    if "message" in request_json:
        message = request_json["message"]
    else:
        raise ValueError("No image name provided")

    payload = {
        "data": message,
    }

    workflow.invoke_serverless_function(text_2_speech, payload)

    workflow.invoke_serverless_function(profanity, payload)

    return {"status": 200}


@workflow.serverless_function(name="Text2Speech")
def text_2_speech(event: dict[str, Any]) -> dict[str, Any]:
    message = event["data"]

    tts = gTTS(message, lang="en")
    mp3_fp = BytesIO()
    tts.write_to_fp(mp3_fp)
    result = mp3_fp.getvalue()
    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "text_2_speech.mp3"
    tmp_dir = tempfile.mkdtemp()
    with open(os.path.join(tmp_dir, file_name), "wb") as f:
        f.write(result)

    s3 = boto3.client("s3")

    s3.upload_file(os.path.join(tmp_dir, file_name), "multi-x-serverless", f"text_2_speech_censoring/{file_name}")

    payload = {
        "file_name": file_name,
    }

    workflow.invoke_serverless_function(conversion, payload)
    return {"status": 200}


@workflow.serverless_function(name="Profanity")
def profanity(event: dict[str, Any]) -> dict[str, Any]:
    message = event["data"]

    profanity.set_censor_characters("*")
    filtered_message = profanity.censor(message)

    extracted_indexes = extract_indexes(filtered_message)

    payload = {
        "indexes": extracted_indexes,
    }

    workflow.invoke_serverless_function(merge_function, payload)
    return {"status": 200}


def extract_indexes(text, char="*") -> list:
    indexes = []
    in_word = False
    start = 0
    for index, value in enumerate(text):
        if value == char:
            if not in_word:
                # This is the first character, else this is one of many
                in_word = True
                start = index
        else:
            if in_word:
                # This is the first non-character
                in_word = False
                indexes.append(((start - 1) / len(text), (index) / len(text)))
    return indexes


@workflow.serverless_function(name="Conversion")
def conversion(event: dict[str, Any]) -> dict[str, Any]:
    file_name = event["file_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", f"text_2_speech_censoring/{file_name}", f"{tmp_dir}/{file_name}")

    dlFile = open(f"{tmp_dir}/{file_name}", "rb").read()
    input = BytesIO(dlFile)
    inputSize = len(input.getvalue())
    speech = AudioSegment.from_mp3(input)
    output = BytesIO()
    speech.export(output, format="wav")
    result = output.getvalue()

    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "text_2_speech.wav"

    with open(os.path.join(tmp_dir, file_name), "wb") as f:
        f.write(result)

    s3.upload_file(os.path.join(tmp_dir, file_name), "multi-x-serverless", f"text_2_speech_censoring/{file_name}")

    payload = {
        "file_name": file_name,
    }

    workflow.invoke_serverless_function(compression, payload)
    return {"status": 200}


@workflow.serverless_function(name="Compression")
def compression(event: dict[str, Any]) -> dict[str, Any]:
    file_name = event["file_name"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", f"text_2_speech_censoring/{file_name}", f"{tmp_dir}/{file_name}")

    dlFile = open(f"{tmp_dir}/{file_name}", "rb").read()
    dlFile = BytesIO(dlFile)
    dlFile.seek(0, os.SEEK_END)
    _ = dlFile.tell()

    dlFile.seek(0)
    outputfile = BytesIO()
    speech = AudioSegment.from_wav(dlFile)
    speech = speech.set_frame_rate(5000)
    speech = speech.set_sample_width(1)
    speech.export(outputfile, format="wav")
    result = outputfile.getvalue()

    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "text_2_speech_compressed.wav"

    with open(os.path.join(tmp_dir, file_name), "wb") as f:
        f.write(result)

    s3.upload_file(os.path.join(tmp_dir, file_name), "multi-x-serverless", f"text_2_speech_censoring/{file_name}")

    payload = {
        "file_name": file_name,
    }

    workflow.invoke_serverless_function(merge_function, payload)
    return {"status": 200}


@workflow.serverless_function(name="MergeFunction")
def merge_function(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data(event)

    for result in results:
        if "file_name" in result:
            file_name = result["file_name"]

        if "indexes" in result:
            indexes = result["indexes"]

    if not "file_name" or not "data":
        raise ValueError("No file name or data provided")

    payload = {
        "file_name": file_name,
        "indexes": indexes,
    }

    workflow.invoke_serverless_function(censor, payload)

    return {"status": 200}


@workflow.serverless_function(name="Censor")
def censor(event: dict[str, Any]) -> dict[str, Any]:
    file_name = event["file_name"]
    indexes = event["data"]

    s3 = boto3.client("s3")
    tmp_dir = tempfile.mkdtemp()

    s3.download_file("multi-x-serverless", f"text_2_speech_censoring/{file_name}", f"{tmp_dir}/{file_name}")

    dlFile = open(f"{tmp_dir}/{file_name}", "rb").read()
    dlFile = BytesIO(dlFile)
    dlFile.seek(0, os.SEEK_END)
    _ = dlFile.tell()

    dlFile.seek(0)
    outputfile = BytesIO()
    speech = AudioSegment.from_wav(dlFile)

    samples = np.array(speech.get_array_of_samples())

    for index, s in enumerate(samples):
        for start, end in indexes:
            start_sample = int(start * len(samples))
            end_sample = int(end * len(samples))
            if index > start_sample and index < end_sample:
                samples[index] = 0

    new_sound = speech._spawn(samples)
    new_sound.export(outputfile, format="wav")
    result = outputfile.getvalue()

    file_name = datetime.now().strftime("%Y%m%d-%H%M%S") + "text_2_speech_censored.wav"

    with open(os.path.join(tmp_dir, file_name), "wb") as f:
        f.write(result)

    s3.upload_file(os.path.join(tmp_dir, file_name), "multi-x-serverless", f"text_2_speech_censoring/{file_name}")

    return {"status": 200}