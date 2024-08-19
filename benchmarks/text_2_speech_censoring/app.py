from typing import Any
import os

from caribou.deployment.client import CaribouWorkflow
import json
from gtts import gTTS
from io import BytesIO
import boto3
from profanity import profanity as prfnty
from pydub import AudioSegment

from tempfile import TemporaryDirectory

# Change the following bucket name and region to match your setup
s3_bucket_name = "caribou-text-2-speech-censoring"
s3_bucket_region_name = "us-east-1"
polly_region_name = s3_bucket_region_name # Note AWS Polly is not available in all regions

workflow = CaribouWorkflow(name="text_2_speech_censoring", version="0.0.1")


@workflow.serverless_function(
    name="get_input",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "input_file" in event:
        message = event["input_file"]
    else:
        raise ValueError("No message provided")
    output_folder_name = event.get("output_folder_name", message.split(".")[0])
    t2s_service: str = event.get("t2s_service", "polly")
    if t2s_service.lower() not in ["polly", "gtts"]:
        raise ValueError(f"Invalid T2S service, Valid t2s_service are: ['polly', 'gTTS'] (Any case)")

    print(f"input file: {message}, Desired output folder name: {output_folder_name}, T2S service: {t2s_service}")
    input_file = f"input/{message}"
    payload = {
        "input_file": input_file,
        "output_folder_name": output_folder_name,
        "t2s_service": t2s_service,
    }

    workflow.invoke_serverless_function(text_2_speech, payload)

    workflow.invoke_serverless_function(profanity, payload)

    return {"status": 200}

@workflow.serverless_function(name="profanity")
def profanity(event: dict[str, Any]) -> dict[str, Any]:
    input_file = event["input_file"]
    output_folder_name = event["output_folder_name"]

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    with TemporaryDirectory() as tmp_dir:
        local_name = f"{tmp_dir}/input.txt"

        s3.download_file(s3_bucket_name, input_file, local_name)

        with open(local_name, "r") as f:
            message = f.read()

        prfnty.set_censor_characters("*")
        filtered_message = prfnty.censor(message)

        extracted_indexes = extract_indexes(filtered_message)

        local_file_name = f"{tmp_dir}/indexes.json"

        with open(local_file_name, "w") as f:
            json.dump(extracted_indexes, f)

        remote_file_name = f"output/{output_folder_name}/intermediate_files/cencoring_indexes.json"

        s3.upload_file(
            local_file_name,
            s3_bucket_name,
            remote_file_name,
        )

        payload = {
            "index_file": remote_file_name,
            "output_folder_name": output_folder_name,
        }

        workflow.invoke_serverless_function(censor, payload)

    return {"status": 200}


@workflow.serverless_function(name="text_2_speech")
def text_2_speech(event: dict[str, Any]) -> dict[str, Any]:
    input_file = event["input_file"]
    output_folder_name = event["output_folder_name"]
    t2s_service: str = event["t2s_service"]
    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    with TemporaryDirectory() as tmp_dir:
        local_name = f"{tmp_dir}/input.txt"

        s3.download_file(s3_bucket_name, input_file, local_name)

        with open(local_name, "r") as f:
            message = f.read()

        # Convert text to speech (Either using Polly or gTTS)
        if t2s_service.lower() == "polly":
            polly = boto3.client("polly", region_name=polly_region_name)
            # Check if the message is too long for Polly
            # If it is, split it into smaller chunks, else
            # we directly convert it to speech (< 1500 characters)
            if len(message) > 1500:
                print("Splitting the message into smaller chunks, current length:", len(message))
                chunks = split_text(message)
                print("Number of chunks:", len(chunks))
                audio_files = []

                for i, chunk in enumerate(chunks):
                    response = polly.synthesize_speech(
                        Text=chunk,
                        OutputFormat="mp3",
                        VoiceId="Joanna"
                    )

                    chunk_file_name = f"chunk_{i}.mp3"
                    chunk_local_name = os.path.join(tmp_dir, chunk_file_name)
                    with open(chunk_local_name, "wb") as f:
                        f.write(response['AudioStream'].read())
                    audio_files.append(chunk_local_name)

                # Concatenate audio files
                combined = AudioSegment.empty()
                for file in audio_files:
                    segment = AudioSegment.from_mp3(file)
                    combined += segment

                # Get the audio stream from the combined audio
                mp3_fp = BytesIO()
                combined.export(mp3_fp, format="mp3")
                result = mp3_fp.getvalue()
            else:
                response = polly.synthesize_speech(
                    Text=message,
                    OutputFormat="mp3",
                    VoiceId="Amy",  # You can choose a different voice if you prefer
                    Engine="standard"
                )
                result = response['AudioStream'].read()
        elif t2s_service.lower() == "gtts":
            tts = gTTS(message, lang="en")
            mp3_fp = BytesIO()
            tts.write_to_fp(mp3_fp)
            result = mp3_fp.getvalue()
        else:
            raise ValueError(f"Invalid T2S service, Valid t2s_service are: ['polly', 'gTTS'] (Any case)")

        file_name = "raw_text_2_speech.mp3"
        local_name = os.path.join(tmp_dir, file_name)

        with open(local_name, "wb") as f:
            f.write(result)

        remote_name = f"output/{output_folder_name}/intermediate_files/{file_name}"

        s3.upload_file(
            local_name,
            s3_bucket_name,
            remote_name,
        )

        payload = {
            "file_name": remote_name,
            "output_folder_name": output_folder_name,
        }

        workflow.invoke_serverless_function(encoding, payload)

    return {"status": 200}

@workflow.serverless_function(name="encoding")
def encoding(event: dict[str, Any]) -> dict[str, Any]:
    file_name = event["file_name"]
    output_folder_name = event["output_folder_name"]

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    with TemporaryDirectory() as tmp_dir:
        local_name = f"{tmp_dir}/input.txt"

        s3.download_file(s3_bucket_name, file_name, local_name)

        dlFile = open(local_name, "rb").read()
        input = BytesIO(dlFile)
        speech = AudioSegment.from_mp3(input)
        speech = speech.set_frame_rate(5000)
        speech = speech.set_sample_width(1)
        output = BytesIO()
        speech.export(output, format="wav")
        result = output.getvalue()

        file_name = "text_2_speech_compressed.wav"
        local_file_name = os.path.join(tmp_dir, file_name)
        with open(local_file_name, "wb") as f:
            f.write(result)

        remote_file_name = f"output/{output_folder_name}/intermediate_files/{file_name}"
        s3.upload_file(
            local_file_name,
            s3_bucket_name,
            remote_file_name,
        )
        payload = {
            "file_name": remote_file_name,
            "output_folder_name": output_folder_name,
        }

        workflow.invoke_serverless_function(censor, payload)

    return {"status": 200}

@workflow.serverless_function(name="censor")
def censor(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data()

    output_folder_name = results[0]["output_folder_name"]
    for result in results:
        if "file_name" in result:
            file_name = result["file_name"]

        if "index_file" in result:
            index_file = result["index_file"]

    if not "file_name" or not "data":
        raise ValueError("No file name or data provided")

    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    with TemporaryDirectory() as tmp_dir:
        local_index_name = f"{tmp_dir}/indexes.json"
        s3.download_file(s3_bucket_name, index_file, local_index_name)

        local_wav_name = f"{tmp_dir}/speech.wav"
        s3.download_file(s3_bucket_name, file_name, local_wav_name)

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

        file_name = "text_2_speech_censored.wav"

        with open(os.path.join(tmp_dir, file_name), "wb") as f:
            f.write(result)

        s3.upload_file(
            os.path.join(tmp_dir, file_name),
            s3_bucket_name,
            f"output/{output_folder_name}/{file_name}",
        )

    return {"status": 200}


# Profanity helper function
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


# Text2Speech helper function
def split_text(text, max_length=1500):
    chunks = []
    while len(text) > max_length:
        split_index = text[:max_length].rfind(' ')
        if split_index == -1:
            split_index = max_length
        chunks.append(text[:split_index + 1].strip())
        text = text[split_index + 1:].strip()
    chunks.append(text)
    return chunks