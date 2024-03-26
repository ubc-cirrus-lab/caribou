import datetime

def get_input(event, context):
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")

    if "message" not in event:
        raise ValueError("No image name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")

    # Get the additional metadata
    metadata = event['metadata']
    metadata['first_function_start_time'] = start_time

    return {
        "image_name": event["message"],
        'metadata': metadata
    }