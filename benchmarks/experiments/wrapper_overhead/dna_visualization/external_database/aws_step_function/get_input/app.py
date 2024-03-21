import datetime


def get_input(event, context):
    start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S,%f%z")
    if "gen_file_name" not in event:
        raise ValueError("No gen_file_name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")
    
    # Get the additional metadata
    metadata = event['metadata']
    metadata['first_function_start_time'] = start_time

    return {
        "gen_file_name": event["gen_file_name"],
        'metadata': metadata
    }