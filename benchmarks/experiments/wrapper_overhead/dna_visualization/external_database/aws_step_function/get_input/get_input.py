def get_input(event, context):
    if "gen_file_name" in event:
        gen_file_name = event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    if "gen_file_name" in event:
        gen_file_name = event["gen_file_name"]
    else:
        raise ValueError("No gen_file_name provided")

    payload = {
        "gen_file_name": gen_file_name,
    }

    return {
        "status": 200,
        "Payload": payload
    }