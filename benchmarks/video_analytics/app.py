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
from caribou.deployment.client import CaribouWorkflow
import io
import torch
from torchvision import models
import zipfile
from copy import deepcopy

MAX_FANOUT_NUM = 6

# Change the following bucket name and region to match your setup
s3_bucket_name = "caribou-video-analytics"
s3_bucket_region_name = "us-east-1"

workflow = CaribouWorkflow(name="video_analytics", version="0.0.1")

# Setup the torch home directory path
model_storage_path = '/tmp/model_storage'
if not os.path.exists(model_storage_path):
    os.makedirs(model_storage_path)
os.environ['TORCH_HOME'] = model_storage_path

@workflow.serverless_function(
    name="streaming",
    entry_point=True,
    allow_placement_decision_override=True,
)
def streaming(event: dict[str, Any]) -> dict[str, Any]:
    if isinstance(event, str):
        event = json.loads(event)

    if "video_name" in event:
        video_name = event["video_name"]
    else:
        raise ValueError("No message provided")
    
    # Get the output folder name (or default to the video name)
    output_folder_name = event.get("output_folder_name", video_name.split(".")[0])

    # See if the user wish to specify number of recognition tasks
    # to fan out to. If not specified, default to -1 indicating auto selection.
    fanout_num = int(event.get("fanout_num", -1))
    fanout_num = 1 if fanout_num == 0 else fanout_num # If fanout of 0, then it means only 1 task (no fanout)
    # Verify that fanout is either -1 or a positive 
    # integer and less than or equal to 4
    if fanout_num != -1 and (fanout_num < 1 or fanout_num > 6):
        raise ValueError("Fanout number must be -1 (indicating automatic) or a positive integer less than or equal than 6")

    print(f"Processing (Streaming) video: {video_name}, Output folder name: {output_folder_name}")
    streaming_filepath = video_analytics_streaming(video_name, output_folder_name)
    payload = {
        "video_name": video_name,
        "output_folder_name": output_folder_name,
        "streaming_filepath": streaming_filepath,
        "fanout_num": fanout_num,
    }

    workflow.invoke_serverless_function(decode, payload)
    return {"status": 200}

@workflow.serverless_function(name="decode")
def decode(event: dict[str, Any]) -> dict[str, Any]:
    video_name = event["video_name"]
    output_folder_name = event["output_folder_name"]
    streaming_filepath = event["streaming_filepath"]
    fanout_num = event["fanout_num"]

    print(f"Processing (Decoding) video: {video_name}, Linked Streaming Filepath: {streaming_filepath}, Output folder name: {output_folder_name}")
    with TemporaryDirectory() as tmp_dir:
        local_streaming_filepath = os.path.join(tmp_dir, video_name)

        # Make sure the directory exists
        os.makedirs(os.path.dirname(local_streaming_filepath), exist_ok=True)

        s3 = boto3.client("s3", region_name=s3_bucket_region_name)

        # Download the video file from S3
        s3.download_file(s3_bucket_name, streaming_filepath, local_streaming_filepath)

        # Calculate the number of partitions needed if not specified
        if fanout_num == -1:
            fanout_num = calculate_fanout_num(local_streaming_filepath)
        else:
            fanout_num = validate_fanout_num(local_streaming_filepath, fanout_num)
        
        # Configure the base payload and the payload list
        payloads = []
        payload = {
            "video_name": video_name,
            "output_folder_name": output_folder_name,
        }

        # Process each partition
        for partition_id in range(1, fanout_num + 1):
            print(f"Processing partition {partition_id} of {fanout_num}")
            decoded_filepath = video_analytics_decode(tmp_dir, local_streaming_filepath, output_folder_name, fanout_num, partition_id)
            payload['partition_id'] = partition_id
            payload['decoded_filepath'] = decoded_filepath
            payloads.append(deepcopy(payload))
        
        # Partition 1 (Always exists)
        workflow.invoke_serverless_function(recognition, payloads[0])

        # Partition 2 (If exists)
        payload = payloads[1] if len(payloads) > 1 else None
        workflow.invoke_serverless_function(recognition, payload, fanout_num > 1)

        # Partition 3 (If exists)
        payload = payloads[2] if len(payloads) > 2 else None
        workflow.invoke_serverless_function(recognition, payload, fanout_num > 2)

        # Partition 4 (If exists)
        payload = payloads[3] if len(payloads) > 3 else None
        workflow.invoke_serverless_function(recognition, payload, fanout_num > 3)

        # Partition 5 (If exists)
        payload = payloads[4] if len(payloads) > 4 else None
        workflow.invoke_serverless_function(recognition, payload, fanout_num > 4)

        # Partition 6 (If exists)
        payload = payloads[5] if len(payloads) > 5 else None
        workflow.invoke_serverless_function(recognition, payload, fanout_num > 5)

        return {"status": 200}

@workflow.serverless_function(name="recognition")
def recognition(event: dict[str, Any]) -> dict[str, Any]:
    video_name = event["video_name"]
    output_folder_name = event["output_folder_name"]
    partition_id = event['partition_id'] 
    decoded_filepath = event['decoded_filepath']
    
    print(f"Processing (Recognition) video: {video_name}, Linked Decode Filepath: {decoded_filepath}, Output folder name: {output_folder_name}")

    with TemporaryDirectory() as tmp_dir:
        local_decode_filepath = os.path.join(tmp_dir, video_name)

        # Make sure the directory exists
        os.makedirs(os.path.dirname(local_decode_filepath), exist_ok=True)

        # Download the zip file of image shards from S3
        s3 = boto3.client("s3", region_name=s3_bucket_region_name)
        s3.download_file(s3_bucket_name, decoded_filepath, local_decode_filepath)
        
        # Unzip the file into a folder
        decode_folder_local_path = os.path.join(tmp_dir, f"decoded-{partition_id}")
        os.makedirs(decode_folder_local_path, exist_ok=True)
        with zipfile.ZipFile(local_decode_filepath, 'r') as zip_ref:
            zip_ref.extractall(decode_folder_local_path)

        recognition_results: dict[str, list[tuple[int, float]]] = {}

        # Go through every image in the folder and perform inference (Also note down the number out of total)
        image_paths = os.listdir(decode_folder_local_path)
        total_images = len(image_paths)
        step_size = total_images // 5

        for idx, image_filename in enumerate(image_paths):
            # We will print such each time it reaches 1/10th of the total images
            # Or the beginning and end of the images
            if idx == 0 or idx == total_images - 1 or (idx % step_size == 0 and idx != 0):
                print(f"Processing image {idx + 1} out of {total_images}")

            image_filepath = os.path.join(decode_folder_local_path, image_filename)
            with open(image_filepath, 'rb') as f:
                frame_idx = int(image_filename.split("-")[-1].split(".")[0])
                image_bytes = f.read()
                result = infer(image_bytes)

                # Add the result to the list
                recognition_results[frame_idx] = result
        
        # Upload the results to S3
        recognition_filepath = f"output/{output_folder_name}/intermediate_files/recognition_results-{partition_id}.json"
        with open(os.path.join(tmp_dir, "recognition_results.json"), 'w') as f:
            json.dump(recognition_results, f)
        s3.upload_file(os.path.join(tmp_dir, "recognition_results.json"), s3_bucket_name, recognition_filepath)

        payload = {
            "video_name": video_name,
            "output_folder_name": output_folder_name,
            "partition_id": partition_id,
            "recognition_filepath": recognition_filepath,
        }
        workflow.invoke_serverless_function(consolidate, payload)

        return {"status": 200}

@workflow.serverless_function(name="consolidate")
def consolidate(event: dict[str, Any]) -> dict[str, Any]:
    results = workflow.get_predecessor_data()

    video_name = results[0]["video_name"]
    output_folder_name = results[0]["output_folder_name"]
    print(f"Processing (Label Consolidation) video: {video_name}, Partition Count: {len(results)}, Output folder name: {output_folder_name}")

    with TemporaryDirectory() as tmp_dir:
        local_decode_filepath = os.path.join(tmp_dir, video_name)

        # Make sure the directory exists
        os.makedirs(os.path.dirname(local_decode_filepath), exist_ok=True)

        # Load model labels
        s3 = boto3.client("s3", region_name=s3_bucket_region_name)
        response = s3.get_object(Bucket=s3_bucket_name, Key="imagenet_labels.txt")
        image_net_labels = response["Body"].read().decode("utf-8").splitlines()

        ordered_results: list[str] = []

        consolidated_labels_dict = {}
        for result in results:
            recognition_filepath = result["recognition_filepath"]

            # Download the recognition results from S3
            s3.download_file(s3_bucket_name, recognition_filepath, os.path.join(tmp_dir, "recognition_results.json"))

            # Load the recognition results
            with open(os.path.join(tmp_dir, "recognition_results.json"), 'r') as f:
                recognition_results = json.load(f)

            # Merge the recognition results dictionary into the consolidated_labels_dict
            consolidated_labels_dict = {**consolidated_labels_dict, **recognition_results}

        for frame in range(len(consolidated_labels_dict)):
            frame = str(frame) # Convert to string for dictionary lookup
            if frame not in consolidated_labels_dict:
                print(consolidated_labels_dict[str(frame)])
                print(f"Skipping frame {frame} as it was not found in the recognition results")
                continue
            frame_top_five_recognition_results = consolidated_labels_dict[frame]
            top_results_str = " ), (".join([f"{image_net_labels[idx]}: {percentage}%" for (idx, percentage) in frame_top_five_recognition_results]).strip()
            ordered_results.append(f"{frame}: [( {top_results_str} )]")

        # Upload the result to S3
        remote_consolidation_results_path = f"output/{output_folder_name}/resulting_imagenet_labels.txt"
        local_consolidation_results_path = os.path.join(tmp_dir, f"video_frame_imagenet_labels.txt")
        with open(local_consolidation_results_path, 'w') as f:
            f.write("\n".join(ordered_results))
        
        s3.upload_file(local_consolidation_results_path, s3_bucket_name, remote_consolidation_results_path)

        return {"status": 200}

# Streaming helper function
def video_analytics_streaming(video_name: str, output_folder_name: str) -> str:
    with TemporaryDirectory() as tmp_dir:
        local_filepath = os.path.join(tmp_dir, video_name)

        remote_filepath = f"input/{video_name}"

        # Make sure the directory exists
        os.makedirs(os.path.dirname(local_filepath), exist_ok=True)

        s3 = boto3.client("s3", region_name=s3_bucket_region_name)

        # Download the file from S3
        s3.download_file(s3_bucket_name, remote_filepath, local_filepath)

        resized_local_filename = resize_and_store(local_filepath, tmp_dir)

        output_folder_path = f"output/{output_folder_name}/intermediate_files"
        remote_streaming_filepath = f"{output_folder_path}/streaming-{video_name}"
        s3.upload_file(resized_local_filename, s3_bucket_name, remote_streaming_filepath)

        return remote_streaming_filepath

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


# Decode helper function
def video_analytics_decode(tmp_dir: str, local_streaming_filepath: str, output_folder_name: int, fanout_num: int, partition_id: int) -> str:
    # Open the video file
    cap = cv2.VideoCapture(local_streaming_filepath)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Calculate frame range for this request_id
    frames_per_section = total_frames // fanout_num
    start_frame = frames_per_section * (partition_id - 1)
    end_frame = start_frame + frames_per_section

    # Adjust for the last section to cover all remaining frames
    if partition_id == fanout_num:
        end_frame = total_frames

    # Seek to the start frame
    cap.set(cv2.CAP_PROP_POS_FRAMES, start_frame)

    # Process and upload frames from start_frame to end_frame
    decode_folder_local_path = os.path.join(tmp_dir, str(partition_id))
    os.makedirs(decode_folder_local_path, exist_ok=True) # Create a folder for the partition
    frame_count = 0
    for frame_idx in range(start_frame, end_frame):
        success, image = cap.read()
        if not success:
            break  # Exit loop if no frame is found

        decoded_filename = f"decoded-{partition_id}-{frame_idx}.jpg"
        decoded_filepath = os.path.join(decode_folder_local_path, decoded_filename)

        # Write the frame to a temporary file
        cv2.imwrite(decoded_filepath, image)
        frame_count += 1
    print(f"Frames processed for partition {partition_id}: {frame_count}")

    # Now we have all the frames in the folder, we can zip them and upload them to S3
    # Create a zip file
    zip_filename = f"decoded-{partition_id}.zip"
    local_zip_filepath = os.path.join(tmp_dir, zip_filename)
    os.makedirs(os.path.dirname(local_zip_filepath), exist_ok=True)
    zip_folder(decode_folder_local_path, local_zip_filepath)

    # Upload the zip file to S3
    remote_decoded_zip_filepath = f"output/{output_folder_name}/intermediate_files/{zip_filename}"
    s3 = boto3.client("s3", region_name=s3_bucket_region_name)
    s3.upload_file(local_zip_filepath, s3_bucket_name, remote_decoded_zip_filepath)

    # Return the S3 path to the zip file
    return remote_decoded_zip_filepath

def calculate_fanout_num(local_streaming_filepath: str, desired_frames_per_partition: int = 200) -> int:
    # Open the video file and calculate the total number of frames
    cap = cv2.VideoCapture(local_streaming_filepath)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # We want to evenly distribute the frames to each partition
    # ideally each partition should have at least 
    # desired_frames_per_partition frames
    # Without a maximum unless max_fanout is reached

    # Calculate the number of partitions needed
    fanout_num = (max((total_frames - 1), 1) // desired_frames_per_partition) + 1
    fanout_num = min(fanout_num, MAX_FANOUT_NUM)
    return fanout_num

def validate_fanout_num(local_streaming_filepath: str, fanout_num: int) -> int:
    # Open the video file and calculate the total number of frames
    cap = cv2.VideoCapture(local_streaming_filepath)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

    # Verify that there are at least 1 frame to distribute
    # to each partition given a fanout_num, else set fanout_num to
    # at least process 1 frame
    return min(fanout_num, total_frames, MAX_FANOUT_NUM)

def zip_folder(desired_zip_folder: str, output_zip_filepath: str):
    file_paths = [os.path.join(desired_zip_folder, f) for f in os.listdir(desired_zip_folder)]

    with zipfile.ZipFile(output_zip_filepath, 'w') as zipf:
        for file in file_paths:
            zipf.write(file, os.path.basename(file))


# Recognition helper function
def infer(image_bytes):
    # Load the model
    model = models.squeezenet1_1(weights=models.SqueezeNet1_1_Weights.DEFAULT)

    frame = preprocess_image(image_bytes)
    model.eval()
    with torch.no_grad():
        out = model(frame)
    _, indices = torch.sort(out, descending=True)
    percentages = torch.nn.functional.softmax(out, dim=1)[0] * 100

    # Return the top 5 results
    return [(int(idx), float(percentages[idx])) for idx in indices[0][:5]]

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