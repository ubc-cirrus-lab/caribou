def get_input(event, context):
    # Check if 'gen_file_name' is in the event and raise an error if not.
    if "gen_file_name" not in event:
        raise ValueError("No gen_file_name provided")
    
    gen_file_name = event["gen_file_name"]

    return {"gen_file_name": gen_file_name}