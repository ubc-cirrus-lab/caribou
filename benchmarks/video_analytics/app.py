from typing import Any

import json
import boto3
import cv2
from tempfile import TemporaryDirectory
import os
from torchvision import transforms
from PIL import Image
import torch
import torchvision.models as models
from multi_x_serverless.deployment.client import MultiXServerlessWorkflow
import io
import torch
from torchvision import models

FANOUT_NUM = 4

workflow = MultiXServerlessWorkflow(name="video_analytics", version="0.0.1")


@workflow.serverless_function(
    name="GetInput",
    entry_point=True,
)
def get_input(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "video_name" in event:
        video_name = event["video_name"]
    else:
        raise ValueError("No message provided")

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

    decoded_filename = video_analytics_decode(video_name, request_id)

    payload = {
        "decoded_filename": decoded_filename,
        "request_id": request_id,
    }

    workflow.invoke_serverless_function(recognition, payload)

    return {"status": 200}


@workflow.serverless_function(name="Recognition")
def recognition(event: dict[str, Any]) -> dict[str, Any]:
    decoded_filename = event["decoded_filename"]
    request_id = event["request_id"]
    print(f"Recognizing video: {decoded_filename}")

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket="multi-x-serverless-video-analytics", Key=decoded_filename)
    image_bytes = response["Body"].read()

    # Perform inference
    result = infer(image_bytes)

    # Upload the result to S3
    result_key = f"output/{request_id}-{decoded_filename}-result.txt"
    s3.put_object(Bucket="multi-x-serverless-video-analytics", Key=result_key, Body=result.encode("utf-8"))

    return {"status": 200, "result_key": result_key}


def video_analytics_streaming(filename: str, request_id: int) -> str:
    with TemporaryDirectory() as tmp_dir:
        local_filename = os.path.join(tmp_dir, filename)

        filename = f"input/{filename}"

        # Make sure the directory exists
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)

        s3 = boto3.client("s3")

        s3.download_file("multi-x-serverless-video-analytics", filename, local_filename)

        resized_local_filename = resize_and_store(local_filename, tmp_dir)

        streaming_filename = f"output/streaming-{request_id}-{filename}"

        s3.upload_file(resized_local_filename, "multi-x-serverless-video-analytics", streaming_filename)

        return streaming_filename

def resize_and_store(local_filename: str, tmp_dir: str) -> str:
    cap = cv2.VideoCapture(local_filename)
    fps = cap.get(cv2.CAP_PROP_FPS)
    fourcc = cv2.VideoWriter_fourcc("m", "p", "4", "v")
    resized_local_filename = os.path.join(tmp_dir, "resized.mp4")
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
    with TemporaryDirectory() as tmp_dir:
        local_filename = os.path.join(tmp_dir, filename)

        # Make sure the directory exists
        os.makedirs(os.path.dirname(local_filename), exist_ok=True)

        s3 = boto3.client("s3")

        # Download the video file from S3
        s3.download_file("multi-x-serverless-video-analytics", filename, local_filename)

        # Open the video file
        cap = cv2.VideoCapture(local_filename)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Calculate frame range for this request_id
        frames_per_section = total_frames // FANOUT_NUM
        start_frame = frames_per_section * (request_id - 1)
        end_frame = start_frame + frames_per_section

        # Adjust for the last section to cover all remaining frames
        if request_id == FANOUT_NUM:
            end_frame = total_frames

        # Seek to the start frame
        cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

        # Process and upload frames from start_frame to end_frame
        for frame_idx in range(start_frame, end_frame):
            success, image = cap.read()
            if not success:
                break  # Exit loop if no frame is found

            frame_name = filename.split(".")[0]

            decoded_filename = f"output/decoded-{request_id}-{frame_idx}-{frame_name}.jpg"
            decoded_local_path = os.path.join(tmp_dir, decoded_filename)

            # Make sure the directory exists
            os.makedirs(os.path.dirname(decoded_local_path), exist_ok=True)

            # Write the frame to a temporary file
            cv2.imwrite(decoded_local_path, image)

            # Upload the frame to S3
            s3.upload_file(decoded_local_path, "multi-x-serverless-video-analytics", decoded_filename)

        # Return the name of the last uploaded frame as an example
        return decoded_filename


def decode_video(local_filename: str) -> bytes:
    cap = cv2.VideoCapture(local_filename)
    frames = []
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        frames.append(cv2.imencode(".jpg", frame)[1].tobytes())
    return frames


def preprocess_image(image_bytes):
    img = Image.open(io.BytesIO(image_bytes))
    transform = transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    img = transform(img)
    return torch.unsqueeze(img, 0)


def infer(image_bytes):
    # Load model labels
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket="multi-x-serverless-video-analytics", Key="imagenet_labels.txt")
    labels = response["Body"].read().decode("utf-8").splitlines()

    with TemporaryDirectory() as tmp_dir:
        os.environ['TORCH_HOME'] = tmp_dir

        # Load the model
        model = models.squeezenet1_1(pretrained=True)

        frame = preprocess_image(image_bytes)
        model.eval()
        with torch.no_grad():
            out = model(frame)
        _, indices = torch.sort(out, descending=True)
        percentages = torch.nn.functional.softmax(out, dim=1)[0] * 100

        return ",".join([f"{labels[idx]}: {percentages[idx].item()}%" for idx in indices[0][:5]]).strip()
