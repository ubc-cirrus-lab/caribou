from typing import Any

import json
import boto3
import cv2
import tempfile
import os
from torchvision import transforms
from PIL import Image
import torch
import torchvision.models as models
from multi_x_serverless.deployment.client import MultiXServerlessWorkflow

workflow = MultiXServerlessWorkflow(name="video_analytics", version="0.0.1")


@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    if "message" in event:
        video_name = event["message"]
    else:
        raise ValueError("No image name provided")

    payload = {
        "video_name": video_name,
        "request_id": 1,
    }

    workflow.invoke_serverless_function(streaming, payload)

    payload["request_id"] = 2

    workflow.invoke_serverless_function(streaming, payload)

    payload["request_id"] = 3

    workflow.invoke_serverless_function(streaming, payload)

    payload["request_id"] = 4

    workflow.invoke_serverless_function(streaming, payload)

    return {"status": 200}


@workflow.serverless_function(name="Streaming")
def streaming(event: dict[str, Any]) -> dict[str, Any]:
    video_name = event["video_name"]
    request_id = event["request_id"]
    print(f"Processing video: {video_name}")

    video_name = video_analytics_streaming(video_name, request_id)

    payload = {
        "video_name": video_name,
        "request_id": request_id,
    }

    workflow.invoke_serverless_function(decode, payload)

    return {"status": 200}


@workflow.serverless_function(name="Decode")
def decode(event: dict[str, Any]) -> dict[str, Any]:
    video_name = event["video_name"]
    request_id = event["request_id"]
    print(f"Decoding video: {video_name}")

    video_name = video_analytics_decode(video_name, request_id)

    payload = {
        "video_name": video_name,
        "request_id": request_id,
    }

    workflow.invoke_serverless_function(recognition, payload)

    return {"status": 200}


@workflow.serverless_function(name="Recognition")
def recognition(event: dict[str, Any]) -> dict[str, Any]:
    video_name = event["video_name"]
    request_id = event["request_id"]
    print(f"Recognizing video: {video_name}")

    return {"status": 200}


def video_analytics_streaming(filename: str, request_id: int) -> str:
    local_filename = f'/tmp/{filename}'

    s3 = boto3.client("s3")

    s3.download_file("multi-x-serverless-video_analytics", filename, local_filename)

    resized_local_filename = resize_and_store(local_filename)

    streaming_filename = f"streaming-{request_id}-{filename}"

    s3.upload_file(resized_local_filename, "multi-x-serverless-video_analytics", streaming_filename)

    os.remove(local_filename)
    os.remove(resized_local_filename)
    return streaming_filename

def resize_and_store(local_filename: str) -> str:
    cap = cv2.VideoCapture(local_filename)
    fps = cap.get(cv2.CAP_PROP_FPS)
    fourcc = cv2.VideoWriter_fourcc('m', 'p', '4', 'v')
    resized_local_filename = '/tmp/resized_video.mp4'
    width, height = 340, 256
    writer = cv2.VideoWriter(resized_local_filename, fourcc, fps, (width, height))

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        frame = cv2.resize(frame, (width, height))
        writer.write(frame)
    return resized_local_filename


def video_analytics_decode(filename: str, request_id: int) -> str:
    local_filename = f'/tmp/{filename}'

    s3 = boto3.client("s3")

    s3.download_file("multi-x-serverless-video_analytics", filename, local_filename)

    video_bytes = decode_video(local_filename)

    tmp = tempfile.NamedTemporaryFile(suffix='.mp4')
    tmp.write(video_bytes)
    tmp.seek(0)
    vidcap = cv2.VideoCapture(tmp.name)

    success, image = vidcap.read()

    if not success:
        raise ValueError("Failed to read video")
    
    decoded_filename = f"decoded-{request_id}-{filename}"
    cv2.imwrite(f'/tmp/{decoded_filename}', image)
    s3.upload_file(f'/tmp/{decoded_filename}', "multi-x-serverless-video_analytics", decoded_filename)

    tmp.close()

    return decoded_filename


def decode_video(local_filename: str) -> bytes:
    cap = cv2.VideoCapture(local_filename)
    frames = []
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        frames.append(cv2.imencode('.jpg', frame)[1].tobytes())
    return frames