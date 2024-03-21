def get_input(event, context):
    if "gen_file_name" not in event:
        raise ValueError("No gen_file_name provided")
    if "metadata" not in event:
        raise ValueError("No metadata provided")

    return {
        "gen_file_name": event["gen_file_name"],
        'metadata': event['metadata']
    }