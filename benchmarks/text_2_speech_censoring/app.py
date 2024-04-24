from typing import Any
import os

from caribou.deployment.client import CaribouWorkflow
import json
from gtts import gTTS
from io import BytesIO
import boto3
from datetime import datetime
from profanity import profanity as prfnty
from pydub import AudioSegment

from tempfile import TemporaryDirectory

workflow = CaribouWorkflow(name="text_2_speech_censoring", version="0.0.1")


@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "input_file" in event:
        message = event["input_file"]
    else:
        raise ValueError("No message provided")
    
    input_file = f"input/{message}"

    payload = {
        "input_file": input_file,
    }

    workflow.invoke_serverless_function(text_2_speech, payload)

    workflow.invoke_serverless_function(profanity, payload)

    return {"status": 200}


@workflow.serverless_function(name="Text2Speech")
def text_2_speech(event: dict[str, Any]) -> dict[str, Any]:
    input_file = event["input_file"]

    s3 = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:

        local_name = f"{tmp_dir}/input.txt"

        s3.download_file("caribou-text-2-speech-censoring", input_file, local_name)

        with open(local_name, "r") as f:
            message = f.read()

        tts = gTTS(message, lang="en")
        mp3_fp = BytesIO()
        tts.write_to_fp(mp3_fp)
        result = mp3_fp.getvalue()

        file_name = f"{workflow.get_run_id()}_text_2_speech.mp3"
        local_name = os.path.join(tmp_dir, file_name)

        with open(local_name, "wb") as f:
            f.write(result)

        remote_name = f"text_2_speech/{workflow.get_run_id()}_{file_name}"

        s3 = boto3.client("s3")

        s3.upload_file(
            local_name,
            "caribou-text-2-speech-censoring",
            remote_name,
        )

        payload = {
            "file_name": remote_name,
        }

        workflow.invoke_serverless_function(conversion, payload)

    return {"status": 200}


@workflow.serverless_function(name="Profanity")
def profanity(event: dict[str, Any]) -> dict[str, Any]:
    input_file = event["input_file"]

    s3 = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:

        local_name = f"{tmp_dir}/input.txt"

        s3.download_file("caribou-text-2-speech-censoring", input_file, local_name)

        with open(local_name, "r") as f:
            message = f.read()

        prfnty.set_censor_characters("*")
        filtered_message = prfnty.censor(message)

        extracted_indexes = extract_indexes(filtered_message)

        local_file_name = f"{tmp_dir}/indexes.json"

        with open(local_file_name, "w") as f:
            json.dump(extracted_indexes, f)

        remote_file_name = f"cencoring_indexes/{workflow.get_run_id()}_indexes.json"

        s3.upload_file(
            local_file_name,
            "caribou-text-2-speech-censoring",
            remote_file_name,
        )

        payload = {
            "index_file": remote_file_name,
        }

        workflow.invoke_serverless_function(censor, payload)
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
    with TemporaryDirectory() as tmp_dir:

        local_name = f"{tmp_dir}/input.txt"

        s3.download_file("caribou-text-2-speech-censoring", file_name, local_name)

        dlFile = open(local_name, "rb").read()
        input = BytesIO(dlFile)
        speech = AudioSegment.from_mp3(input)
        output = BytesIO()
        speech.export(output, format="wav")
        result = output.getvalue()

        file_name = "text_2_speech.wav"

        local_file_name = os.path.join(tmp_dir, file_name)

        with open(local_file_name, "wb") as f:
            f.write(result)

        remote_file_name = f"text_2_speech_converted/{workflow.get_run_id()}_{file_name}"

        s3.upload_file(
            local_file_name,
            "caribou-text-2-speech-censoring",
            remote_file_name,
        )

        payload = {
            "file_name": remote_file_name,
        }

        workflow.invoke_serverless_function(compression, payload)

    return {"status": 200}


@workflow.serverless_function(name="Compression")
def compression(event: dict[str, Any]) -> dict[str, Any]:
    file_name = event["file_name"]

    s3 = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:
        local_name = f"{tmp_dir}/input.wav"
        s3.download_file("caribou-text-2-speech-censoring", file_name, local_name)

        dlFile = open(local_name, "rb").read()
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

        local_file_name = os.path.join(tmp_dir, "text_2_speech_compressed.wav")

        with open(local_file_name, "wb") as f:
            f.write(result)

        remote_file_name = f"text_2_speech_compressed/{workflow.get_run_id()}_text_2_speech_compressed.wav"

        s3.upload_file(
            local_file_name,
            "caribou-text-2-speech-censoring",
            remote_file_name,
        )

        payload = {
            "file_name": remote_file_name,
        }

        workflow.invoke_serverless_function(censor, payload)

    return {"status": 200}


@workflow.serverless_function(name="Censor")
def censor(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data()

    for result in results:
        if "file_name" in result:
            file_name = result["file_name"]

        if "index_file" in result:
            index_file = result["index_file"]

    if not "file_name" or not "data":
        raise ValueError("No file name or data provided")

    s3 = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:
        local_index_name = f"{tmp_dir}/indexes.json"
        s3.download_file("caribou-text-2-speech-censoring", index_file, local_index_name)

        local_wav_name = f"{tmp_dir}/speech.wav"
        s3.download_file("caribou-text-2-speech-censoring", file_name, local_wav_name)

        dlFile = open(local_wav_name, "rb").read()
        dlFile = BytesIO(dlFile)
        dlFile.seek(0, os.SEEK_END)
        _ = dlFile.tell()

        dlFile.seek(0)
        outputfile = BytesIO()
        speech = AudioSegment.from_wav(dlFile)

        samples = speech.get_array_of_samples()

        with open(local_index_name, "r") as f:
            indexes = json.load(f)

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

        s3.upload_file(
            os.path.join(tmp_dir, file_name),
            "caribou-text-2-speech-censoring",
            f"text_2_speech_censoring/{file_name}",
        )

    return {"status": 200}
